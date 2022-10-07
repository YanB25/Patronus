#include <algorithm>
#include <random>
#include <set>

#include "Common.h"
#include "boost/thread/barrier.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "patronus/Patronus.h"
#include "patronus/RdmaAdaptor.h"
#include "patronus/memory/direct_allocator.h"
#include "patronus/memory/mr_allocator.h"
#include "thirdparty/racehashing/hashtable.h"
#include "thirdparty/racehashing/hashtable_handle.h"
#include "thirdparty/racehashing/utils.h"
#include "util/BenchRand.h"
#include "util/DataFrameF.h"
#include "util/Rand.h"
#include "util/Tracer.h"

using namespace patronus::hash;
using namespace util::literals;
using namespace patronus;
using namespace hmdf;

[[maybe_unused]] constexpr uint16_t kClientNodeId = 0;
[[maybe_unused]] constexpr uint16_t kServerNodeId = 1;
constexpr static size_t kTotalThreadNr = 16;
constexpr static size_t kPreparedThreadNr = 10;
constexpr static size_t kWorkerThreadNr = 6;
constexpr static size_t kMaxCoroNr = 8;

static_assert(kTotalThreadNr >= kPreparedThreadNr + kWorkerThreadNr);

DEFINE_string(exec_meta, "", "The meta data of this execution");

std::vector<std::string> col_idx;

std::vector<bool> col_is_fast_recovery;
std::vector<size_t> col_fault_issue_ns;
std::vector<size_t> col_fault_arrive_ns;
std::vector<size_t> col_following_op_issue_ns;
std::vector<size_t> col_following_op_arrive_ns;
std::vector<size_t> col_fast_thread_desc_ns;
std::vector<size_t> col_slow_signal_server_ns;
std::vector<size_t> col_slow_client_recover_ns;
std::vector<size_t> col_recovery_total_ns;

struct AdditionalCoroCtx
{
    ssize_t thread_remain_task{0};
    size_t rdma_protection_nr{0};
};

struct BenchConfig
{
    std::string name;
    size_t thread_nr;
    size_t coro_nr;
    size_t test_nr;
    size_t fault_after_nr;
    size_t fault_every;
    bool first_enter;
    bool server_should_leave;
    bool should_report;
    static BenchConfig get_empty_conf(const std::string &name,
                                      size_t thread_nr,
                                      size_t coro_nr,
                                      size_t test_nr,
                                      size_t fault_after_nr,
                                      size_t fault_nr)
    {
        BenchConfig ret;
        ret.name = name;
        ret.thread_nr = thread_nr;
        ret.coro_nr = coro_nr;
        ret.test_nr = test_nr;
        ret.fault_after_nr = fault_after_nr;
        ret.fault_every = (test_nr - fault_after_nr) / fault_nr;
        return ret;
    }
};

class ConfigFactory
{
public:
    static std::vector<BenchConfig> get_basic(const std::string &name,
                                              size_t thread_nr,
                                              size_t coro_nr,
                                              size_t test_nr,
                                              size_t fault_after_nr,
                                              size_t fault_nr)
    {
        auto conf = BenchConfig::get_empty_conf(
            name, thread_nr, coro_nr, test_nr, fault_after_nr, fault_nr);
        conf.should_report = true;
        return pipeline({conf});
    }

private:
    static std::vector<BenchConfig> pipeline(std::vector<BenchConfig> &&confs)
    {
        for (auto &conf : confs)
        {
            conf.first_enter = false;
            conf.server_should_leave = false;
        }
        if (confs.empty())
        {
            return confs;
        }
        confs.front().first_enter = true;
        confs.back().server_should_leave = true;
        return confs;
    }
};

std::ostream &operator<<(std::ostream &os, const BenchConfig &conf)
{
    os << "{conf: name: " << conf.name << ", thread: " << conf.thread_nr
       << ", coro: " << conf.coro_nr << ", test: " << conf.test_nr
       << ", enter: " << conf.first_enter
       << ", leave: " << conf.server_should_leave
       << ", report: " << conf.should_report << "}";
    return os;
}

void test_basic_client_worker(
    Patronus::pointer p,
    size_t coro_id,
    CoroYield &yield,
    const BenchConfig &conf,
    CoroExecutionContextWith<kMaxCoroNr, AdditionalCoroCtx> &ex)
{
    std::ignore = conf;
    auto tid = p->get_thread_id();
    auto dir_id = tid;
    CoroContext ctx(tid, &yield, &ex.master(), coro_id);

    size_t rdma_protection_nr = 0;
    size_t executed_nr = 0;
    size_t succ_nr = 0;

    size_t acquire_nr = 0;
    size_t rel_nr = 0;

    std::string key;
    std::string value;

    ChronoTimer timer;

    bool i_trigger_fault = coro_id == 0;

    while (ex.get_private_data().thread_remain_task > 0)
    {
        executed_nr++;

        tid = p->get_thread_id();
        dir_id = tid;

        auto ac_flag = (flag_t) AcquireRequestFlag::kNoGc |
                       (flag_t) AcquireRequestFlag::kNoBindPR;
        auto lease = p->get_wlease(kServerNodeId,
                                   dir_id,
                                   GlobalAddress(0),
                                   0 /* hint */,
                                   64 /* size */,
                                   0ns,
                                   ac_flag,
                                   &ctx);
        acquire_nr++;
        if (unlikely(!lease.success()))
        {
            CHECK(lease.ec() == AcquireRequestStatus::kMagicMwErr ||
                  lease.ec() == AcquireRequestStatus::kBindErr)
                << "** Unexpected lease.ec(): " << lease.ec();
            continue;
        }

        bool should_fault = (executed_nr >= conf.fault_after_nr) &&
                            (executed_nr % conf.fault_every == 0);
        util::TraceManager crash_tm(1);
        util::TraceManager rw_tm(1);
        if (unlikely(i_trigger_fault && should_fault))
        {
            auto trace = crash_tm.trace("crash");

            ChronoTimer timer;
            // p->trig
            DLOG(INFO) << "[debug] trigger rdma_protection_error";

            auto rdma_buf = p->get_rdma_buffer(8);
            CHECK_GE(rdma_buf.size, 8);
            auto ec = p->write(lease,
                               rdma_buf.buffer,
                               8,
                               4_KB /* offset */,
                               0 /* flag */,
                               &ctx,
                               trace);
            CHECK_EQ(ec, kRdmaProtectionErr);
            auto fault_ns = timer.pin();

            auto rw_flag = (flag_t) RWFlag::kNoLocalExpireCheck |
                           (flag_t) RWFlag::kUseUniversalRkey;
            // must get a new buffer here
            // because the QP may potentially switched
            auto rdma_buf2 = p->get_rdma_buffer(8);
            // not important this copy
            memcpy(rdma_buf2.buffer, rdma_buf.buffer, 8);
            ec = p->write(
                lease, rdma_buf2.buffer, 8, 0 /* offset */, rw_flag, &ctx);

            CHECK_EQ(ec, kOk);

            auto rdma_buf3 = p->get_rdma_buffer(8);
            memcpy(rdma_buf3.buffer, rdma_buf2.buffer, 8);
            auto trace2 = rw_tm.trace("rw");
            ec = p->write(lease,
                          rdma_buf3.buffer,
                          8,
                          0 /* offset */,
                          rw_flag,
                          &ctx,
                          trace2);

            CHECK_EQ(ec, kOk);

            LOG(INFO) << "[bench] crash op takes " << fault_ns
                      << " ns. total: " << fault_ns << " ns. Trace: " << trace;

            uint64_t total_fault_ns = 0;
            CHECK(trace.enabled());
            CHECK(trace2.enabled());
            auto map = trace.retrieve_map();
            auto map2 = trace2.retrieve_map();
            LOG(INFO) << "[debug] map: " << util::pre_map(map)
                      << ", map2: " << util::pre_map(map2);
            col_idx.push_back(std::to_string(col_idx.size()));
            auto fault_issue_ns = map["issue"];
            total_fault_ns += fault_issue_ns;
            col_fault_issue_ns.push_back(fault_issue_ns);
            auto fault_arrive_ns = map["arrived"];
            total_fault_ns += fault_arrive_ns;
            col_fault_arrive_ns.push_back(fault_arrive_ns);
            col_following_op_issue_ns.push_back(map2["issue"]);
            col_following_op_arrive_ns.push_back(map2["arrived"]);
            if (map.count("switch-thread-desc"))
            {
                col_is_fast_recovery.push_back(true);
                CHECK_EQ(map.count("signal-server-recovery"), 0)
                    << "map is " << util::pre_map(map);
                CHECK_EQ(map.count("client-recovery"), 0)
                    << "map is " << util::pre_map(map);
                auto switch_ns = map["switch-thread-desc"];
                total_fault_ns += switch_ns;
                col_fast_thread_desc_ns.push_back(switch_ns);
                col_slow_client_recover_ns.push_back(0);
                col_slow_signal_server_ns.push_back(0);
            }
            else
            {
                col_is_fast_recovery.push_back(false);
                CHECK_EQ(map.count("signal-server-recovery"), 1)
                    << "map is " << util::pre_map(map);
                CHECK_EQ(map.count("client-recovery"), 1)
                    << "map is " << util::pre_map(map);
                col_fast_thread_desc_ns.push_back(0);
                auto client_recv_ns = map["client-recovery"];
                col_slow_client_recover_ns.push_back(client_recv_ns);
                total_fault_ns += client_recv_ns;
                auto server_signal_ns = map["signal-server-recovery"];
                col_slow_signal_server_ns.push_back(server_signal_ns);
                total_fault_ns += server_signal_ns;
            }
            col_recovery_total_ns.push_back(total_fault_ns);
        }
        else
        {
            auto rdma_buf = p->get_rdma_buffer(8);
            CHECK_GE(rdma_buf.size, 8);
            auto ec = p->read(
                lease, rdma_buf.buffer, 8, 0 /* offset */, 0 /* flag */, &ctx);
            CHECK_EQ(ec, kOk) << "** unexpected read failure at " << executed_nr
                              << "-th op. tid: " << tid
                              << ", dir_id: " << dir_id << ". lease: " << lease;
            p->put_rdma_buffer(std::move(rdma_buf));

            succ_nr++;
            auto rel_flag = (flag_t) 0;
            p->relinquish(lease, 0 /* hint */, rel_flag, &ctx);
            rel_nr++;
        }

        ex.get_private_data().thread_remain_task--;
    }

    auto ns = timer.pin();

    auto &comm = ex.get_private_data();
    comm.rdma_protection_nr += rdma_protection_nr;

    LOG(INFO) << "[bench] rdma_protection_nr: " << rdma_protection_nr
              << ". executed: " << executed_nr << ", take: " << ns << " ns. "
              << ", ops: " << 1e9 * executed_nr / ns
              << " . acquire_nr: " << acquire_nr << ", rel_nr: " << rel_nr
              << ", succ_nr: " << succ_nr << ". ctx: " << ctx;

    ex.worker_finished(coro_id);
    ctx.yield_to_master();
}

void test_basic_client_master(
    Patronus::pointer p,
    CoroYield &yield,
    size_t test_nr,
    std::atomic<ssize_t> &atm_task_nr,
    size_t coro_nr,
    CoroExecutionContextWith<kMaxCoroNr, AdditionalCoroCtx> &ex)
{
    auto tid = p->get_thread_id();

    CoroContext mctx(tid, &yield, ex.workers());
    CHECK(mctx.is_master());

    ssize_t task_per_sync = test_nr / 20;
    LOG_IF(WARNING, tid == 0 && task_per_sync <= (ssize_t) coro_nr)
        << "task_per_sync " << task_per_sync
        << " less than coro_nr: " << coro_nr;
    task_per_sync = std::max(task_per_sync, ssize_t(coro_nr));
    ssize_t remain =
        atm_task_nr.fetch_sub(task_per_sync, std::memory_order_relaxed) -
        task_per_sync;
    ex.get_private_data().thread_remain_task = task_per_sync;

    for (size_t i = 0; i < coro_nr; ++i)
    {
        mctx.yield_to_worker(i);
    }

    coro_t coro_buf[2 * kMaxCoroNr];
    while (true)
    {
        tid = p->get_thread_id();
        if ((ssize_t) ex.get_private_data().thread_remain_task <=
            2 * ssize_t(coro_nr))
        {
            // refill the thread_remain_task
            auto cur_task_nr = std::min(remain, task_per_sync);
            if (cur_task_nr >= 0)
            {
                remain = atm_task_nr.fetch_sub(cur_task_nr,
                                               std::memory_order_relaxed) -
                         cur_task_nr;
                if (remain >= 0)
                {
                    ex.get_private_data().thread_remain_task += cur_task_nr;
                }
            }
        }
        auto nr = p->try_get_client_continue_coros(coro_buf, 2 * kMaxCoroNr);
        for (size_t i = 0; i < nr; ++i)
        {
            auto coro_id = coro_buf[i];
            mctx.yield_to_worker(coro_id);
        }

        if (remain <= 0)
        {
            if (ex.is_finished_all())
            {
                CHECK_LE(remain, 0);
                break;
            }
        }
    }
}

std::atomic<ssize_t> g_total_test_nr;
void benchmark_client(Patronus::pointer p,
                      boost::barrier &bar,
                      const BenchConfig &conf,
                      bool is_master,
                      uint64_t key)
{
    auto coro_nr = conf.coro_nr;
    auto thread_nr = conf.thread_nr;
    bool first_enter = conf.first_enter;
    bool server_should_leave = conf.server_should_leave;
    CHECK_LE(coro_nr, kMaxCoroNr);

    auto tname = p->get_thread_name_id();
    if (is_master)
    {
        // init here by master
        g_total_test_nr = conf.test_nr;
        if (first_enter)
        {
            auto dsm = p->get_dsm();
            dsm->keeper_partial_barrier("server_ready-" + std::to_string(key),
                                        2 /* expect_nr */,
                                        false,
                                        100ms);
        }
    }
    bar.wait();

    ChronoTimer timer;
    CoroExecutionContextWith<kMaxCoroNr, AdditionalCoroCtx> ex;
    if (tname < thread_nr)
    {
        ex.get_private_data().thread_remain_task = 0;
        for (size_t i = coro_nr; i < kMaxCoroNr; ++i)
        {
            // no that coro, so directly finished.
            ex.worker_finished(i);
        }
        for (size_t i = 0; i < coro_nr; ++i)
        {
            ex.worker(i) =
                CoroCall([p, coro_id = i, &ex, &conf](CoroYield &yield) {
                    test_basic_client_worker(p, coro_id, yield, conf, ex);
                });
        }
        auto &master = ex.master();
        master = CoroCall(
            [p, &ex, test_nr = conf.test_nr, coro_nr](CoroYield &yield) {
                test_basic_client_master(
                    p, yield, test_nr, g_total_test_nr, coro_nr, ex);
            });

        master();
    }
    bar.wait();
    auto ns = timer.pin();

    double ops = 1e9 * conf.test_nr / ns;
    double avg_ns = 1.0 * ns / conf.test_nr;
    LOG_IF(INFO, is_master)
        << "[bench] total op: " << conf.test_nr << ", ns: " << ns
        << ", ops: " << ops << ", avg " << avg_ns << " ns";

    if (is_master && server_should_leave)
    {
        LOG(INFO) << "p->finished(" << key << ")";
        p->finished(key);
    }
}

void benchmark_server(Patronus::pointer p,
                      boost::barrier &bar,
                      const std::vector<BenchConfig> &confs,
                      bool is_master,
                      uint64_t key)
{
    auto thread_nr = confs[0].thread_nr;
    for (const auto &conf : confs)
    {
        CHECK_EQ(conf.thread_nr, thread_nr);
    }
    if (is_master)
    {
        p->finished(key);
    }

    bar.wait();
    if (is_master)
    {
        auto dsm = p->get_dsm();
        dsm->keeper_partial_barrier("server_ready-" + std::to_string(key),
                                    2 /* expect_nr */,
                                    true,
                                    100ms);
    }

    p->server_serve(key);
    bar.wait();
}

void benchmark(Patronus::pointer p, boost::barrier &bar, bool is_client)
{
    uint64_t key = 0;
    bool is_master = p->get_thread_id() == 0;
    bar.wait();

    // very easy to saturate, no need to test many threads
    for (size_t thread_nr : {1})
    {
        {
            key++;
            constexpr static size_t kFaultNr = 2 * kPreparedThreadNr;
            constexpr static size_t kFaultEvery = 100;
            auto confs = ConfigFactory::get_basic("crash",
                                                  thread_nr,
                                                  1,
                                                  10_K + kFaultNr * kFaultEvery,
                                                  10_K,
                                                  kFaultNr);

            if (is_client)
            {
                for (const auto &conf : confs)
                {
                    LOG_IF(INFO, is_master) << "[conf] " << conf;
                    benchmark_client(p, bar, conf, is_master, key);
                }
            }
            else
            {
                LOG_IF(INFO, is_master) << "[conf] confs[0]: " << confs[0];
                benchmark_server(p, bar, confs, is_master, key);
            }
        }
    }
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    PatronusConfig pconfig;
    pconfig.machine_nr = ::config::kMachineNr;
    pconfig.block_class = {2_MB, 8_KB};
    pconfig.block_ratio = {0.5, 0.5};
    pconfig.reserved_buffer_size = 2_GB;
    pconfig.lease_buffer_size = (kDSMCacheSize - 2_GB) / 2;
    pconfig.alloc_buffer_size = (kDSMCacheSize - 2_GB) / 2;

    auto patronus = Patronus::ins(pconfig);

    auto nid = patronus->get_node_id();

    bool is_client = nid == kClientNodeId;
    bool is_server = nid == kServerNodeId;

    LOG(WARNING)
        << "TODO: This benchmark can not re-enter. Only one sub-conf "
           "to run, otherwise will introduce undefined behaviour. The reason "
           "is that fast switching QP will introduce the change of tid.";

    if (is_client)
    {
        std::vector<std::thread> threads;
        boost::barrier bar(kWorkerThreadNr);
        patronus->registerClientThread();
        CHECK_EQ(patronus->get_thread_id(), patronus->get_thread_name_id());
        for (size_t i = 0; i < kWorkerThreadNr - 1; ++i)
        {
            threads.emplace_back([patronus, &bar]() {
                patronus->registerClientThread();
                CHECK_EQ(patronus->get_thread_id(),
                         patronus->get_thread_name_id());
                bar.wait();  // after registered
                LOG(INFO) << "[bench] " << pre_patronus_explain(*patronus)
                          << ". tid: " << patronus->get_thread_id();

                benchmark(patronus, bar, true);
            });
        }
        bar.wait();  // after registered
        patronus->prepare_fast_backup_recovery(kPreparedThreadNr);
        LOG(INFO) << "[bench] " << pre_patronus_explain(*patronus)
                  << ". tid: " << patronus->get_thread_id();
        benchmark(patronus, bar, true);

        for (auto &t : threads)
        {
            t.join();
        }
    }
    else if (is_server)
    {
        std::vector<std::thread> threads;
        boost::barrier bar(kTotalThreadNr);
        patronus->registerServerThread();
        CHECK_EQ(patronus->get_thread_id(), patronus->get_thread_name_id());
        // server should register total thread
        // to handle backup QPs
        for (size_t i = 0; i < kTotalThreadNr - 1; ++i)
        {
            threads.emplace_back([patronus, &bar]() {
                patronus->registerServerThread();
                CHECK_EQ(patronus->get_thread_id(),
                         patronus->get_thread_name_id());
                bar.wait();  // after registered
                LOG(INFO) << "[bench] " << pre_patronus_explain(*patronus)
                          << ". tid: " << patronus->get_thread_id();

                benchmark(patronus, bar, false);
            });
        }
        bar.wait();  // after registered
        LOG(INFO) << "[bench] " << pre_patronus_explain(*patronus)
                  << ". tid: " << patronus->get_thread_id();
        benchmark(patronus, bar, false);

        for (auto &t : threads)
        {
            t.join();
        }
    }
    else
    {
        patronus->registerClientThread();
        for (size_t i = 0; i < 4; ++i)
        {
            patronus->finished(i);
        }
        LOG(INFO) << "SKIP.";
    }

    StrDataFrame df;
    df.load_index(std::move(col_idx));

    df.load_column<bool>("fast_recovery", std::move(col_is_fast_recovery));
    df.load_column<size_t>("fault_issue(ns)", std::move(col_fault_issue_ns));
    df.load_column<size_t>("fault_arrive(ns)", std::move(col_fault_arrive_ns));
    df.load_column<size_t>("next_issue(ns)",
                           std::move(col_following_op_issue_ns));
    df.load_column<size_t>("next_arrive(ns)",
                           std::move(col_following_op_arrive_ns));
    df.load_column<size_t>("switch_QP(ns)", std::move(col_fast_thread_desc_ns));
    df.load_column<size_t>("signal_server(ns)",
                           std::move(col_slow_signal_server_ns));
    df.load_column<size_t>("recover_QP(ns)",
                           std::move(col_slow_client_recover_ns));
    df.load_column<size_t>("total(ns)", std::move(col_recovery_total_ns));

    // auto div_f = gen_F_div<size_t, size_t, double>();
    // auto div_f2 = gen_F_div<double, size_t, double>();
    // auto ops_f = gen_F_ops<size_t, size_t, double>();
    // auto mul_f = gen_F_mul<double, size_t, double>();
    // auto mul_f2 = gen_F_mul<size_t, size_t, size_t>();

    auto filename = binary_to_csv_filename(argv[0], FLAGS_exec_meta);
    df.write<std::ostream, std::string, bool, size_t, double>(std::cout,
                                                              io_format::csv2);
    df.write<std::string, bool, size_t, double>(filename.c_str(),
                                                io_format::csv2);

    patronus->keeper_barrier("finished", 100ms);

    LOG(INFO) << "finished. ctrl+C to quit.";
}