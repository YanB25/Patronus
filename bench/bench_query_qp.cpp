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

using namespace patronus::hash;
using namespace util::literals;
using namespace patronus;
using namespace hmdf;

[[maybe_unused]] constexpr uint16_t kClientNodeId = 1;
[[maybe_unused]] constexpr uint16_t kServerNodeId = 0;
constexpr uint32_t kMachineNr = 2;
constexpr static size_t kThreadNr = 16;
constexpr static size_t kMaxCoroNr = 1;

DEFINE_string(exec_meta, "", "The meta data of this execution");

std::vector<std::string> col_idx;
std::vector<size_t> col_x_thread_nr;
std::vector<size_t> col_x_coro_nr;
std::vector<size_t> col_test_op_nr;
std::vector<size_t> col_ns;

struct BenchConfig;

struct BenchConfig
{
    size_t thread_nr{1};
    size_t coro_nr{1};
    size_t test_nr{0};
    bool first_enter{true};
    bool server_should_leave{true};
    bool should_report{false};
    std::string name;

    static BenchConfig get_empty_conf(const std::string &name,
                                      size_t thread_nr,
                                      size_t coro_nr,
                                      size_t test_nr)
    {
        BenchConfig ret;
        ret.name = name;
        ret.thread_nr = thread_nr;
        ret.coro_nr = coro_nr;
        ret.test_nr = test_nr;
        return ret;
    }
};

struct AdditionalCoroCtx
{
    ssize_t thread_remain_task{0};
    size_t rdma_protection_nr{0};
    std::array<bool, kMaxCoroNr> want_next_task{};
};

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

    size_t executed_nr = 0;
    auto dsm = p->get_dsm();
    auto *qp = dsm->get_dir_qp(kServerNodeId, tid, dir_id);
    while (ex.get_private_data().thread_remain_task > 0)
    {
        rdmaQueryQueuePair(qp);
        executed_nr++;
        ex.get_private_data().thread_remain_task--;
        ex.get_private_data().want_next_task[coro_id] = true;
        ctx.yield_to_master();
    }

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

        for (size_t i = 0; i < coro_nr; ++i)
        {
            if (ex.get_private_data().want_next_task[i])
            {
                ex.get_private_data().want_next_task[i] = false;
                mctx.yield_to_worker(i);
            }
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

std::ostream &operator<<(std::ostream &os, const BenchConfig &conf)
{
    os << "{conf: name: " << conf.name << ", thread: " << conf.thread_nr
       << ", coro: " << conf.coro_nr << ", test: " << conf.test_nr
       << ", enter: " << conf.first_enter
       << ", leave: " << conf.server_should_leave
       << ", report: " << conf.should_report << "}";
    return os;
}

class ConfigFactory
{
public:
    static std::vector<BenchConfig> get_conf(const std::string &name,
                                             size_t thread_nr,
                                             size_t coro_nr,
                                             size_t test_nr)
    {
        auto conf =
            BenchConfig::get_empty_conf(name, thread_nr, coro_nr, test_nr);
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

    auto tid = p->get_thread_id();
    if (is_master)
    {
        // init here by master
        g_total_test_nr = conf.test_nr;
        if (first_enter)
        {
            p->keeper_barrier("server_ready-" + std::to_string(key), 100ms);
        }
    }
    bar.wait();

    ChronoTimer timer;
    CoroExecutionContextWith<kMaxCoroNr, AdditionalCoroCtx> ex;
    if (tid < thread_nr)
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
                CoroCall([p, coro_id = i, &conf, &ex](CoroYield &yield) {
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

    if (is_master && conf.should_report)
    {
        col_idx.push_back(conf.name);
        col_x_thread_nr.push_back(conf.thread_nr);
        col_x_coro_nr.push_back(conf.coro_nr);

        col_test_op_nr.push_back(conf.test_nr);
        col_ns.push_back(ns);
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
        p->keeper_barrier("server_ready-" + std::to_string(key), 100ms);
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
    for (size_t thread_nr : {1, 4, 8, 16})
    {
        {
            key++;
            auto confs =
                ConfigFactory::get_conf("query QP", thread_nr, kMaxCoroNr, 1_M);
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
                benchmark_server(p, bar, confs, is_master, key);
            }
        }
    }
}

void report_qp(Patronus::pointer p)
{
    auto dsm = p->get_dsm();
    auto *qp = dsm->get_dir_qp(kServerNodeId, 0, 0);
    rdmaReportQueuePair2(qp);
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    PatronusConfig pconfig;
    pconfig.machine_nr = kMachineNr;
    pconfig.block_class = {2_MB, 8_KB};
    pconfig.block_ratio = {0.5, 0.5};
    pconfig.reserved_buffer_size = 2_GB;
    pconfig.lease_buffer_size = (::config::kDefaultDSMSize - 2_GB) / 2;
    pconfig.alloc_buffer_size = (::config::kDefaultDSMSize - 2_GB) / 2;

    auto patronus = Patronus::ins(pconfig);

    std::vector<std::thread> threads;
    boost::barrier bar(kThreadNr);
    auto nid = patronus->get_node_id();

    bool is_client = nid == kClientNodeId;

    if (is_client)
    {
        patronus->registerClientThread();
        report_qp(patronus);
    }
    else
    {
        patronus->registerServerThread();
    }

    for (size_t i = 0; i < kThreadNr - 1; ++i)
    {
        threads.emplace_back([patronus, &bar, is_client]() {
            if (is_client)
            {
                patronus->registerClientThread();
            }
            else
            {
                patronus->registerServerThread();
            }
            LOG(INFO) << "[bench] " << pre_patronus_explain(*patronus)
                      << ". tid: " << patronus->get_thread_id();

            benchmark(patronus, bar, is_client);
        });
    }
    LOG(INFO) << "[bench] " << pre_patronus_explain(*patronus)
              << ". tid: " << patronus->get_thread_id();
    benchmark(patronus, bar, is_client);

    for (auto &t : threads)
    {
        t.join();
    }

    StrDataFrame df;
    df.load_index(std::move(col_idx));
    df.load_column<size_t>("x_thread_nr", std::move(col_x_thread_nr));
    df.load_column<size_t>("x_coro_nr", std::move(col_x_coro_nr));

    df.load_column<size_t>("test_nr(total)", std::move(col_test_op_nr));
    df.load_column<size_t>("test_ns(total)", std::move(col_ns));

    auto div_f = gen_F_div<size_t, size_t, double>();
    auto div_f2 = gen_F_div<double, size_t, double>();
    auto ops_f = gen_F_ops<size_t, size_t, double>();
    auto mul_f = gen_F_mul<double, size_t, double>();
    auto mul_f2 = gen_F_mul<size_t, size_t, size_t>();
    df.consolidate<size_t, size_t, size_t>(
        "x_thread_nr", "x_coro_nr", "client_nr(effective)", mul_f2, false);
    df.consolidate<size_t, size_t, size_t>("test_ns(total)",
                                           "client_nr(effective)",
                                           "test_ns(effective)",
                                           mul_f2,
                                           false);
    df.consolidate<size_t, size_t, double>(
        "test_ns(total)", "test_nr(total)", "lat(div)", div_f, false);
    df.consolidate<size_t, size_t, double>(
        "test_ns(effective)", "test_nr(total)", "lat(avg)", div_f, false);
    df.consolidate<size_t, size_t, double>(
        "test_nr(total)", "test_ns(total)", "ops(total)", ops_f, false);
    df.consolidate<double, size_t, double>(
        "ops(total)", "x_thread_nr", "ops(thread)", div_f2, false);
    df.consolidate<double, size_t, double>(
        "ops(thread)", "x_coro_nr", "ops(coro)", div_f2, false);

    auto filename = binary_to_csv_filename(argv[0], FLAGS_exec_meta);
    df.write<std::ostream, std::string, size_t, double>(std::cout,
                                                        io_format::csv2);
    df.write<std::string, size_t, double>(filename.c_str(), io_format::csv2);

    patronus->keeper_barrier("finished", 100ms);

    LOG(INFO) << "finished. ctrl+C to quit.";
}