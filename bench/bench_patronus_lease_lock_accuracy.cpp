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
#include "util/Pre.h"
#include "util/Rand.h"
#include "util/TimeConv.h"

using namespace patronus::hash;
using namespace util::literals;
using namespace patronus;
using namespace hmdf;

[[maybe_unused]] constexpr uint16_t kClientNodeId = 1;
[[maybe_unused]] constexpr uint16_t kServerNodeId = 0;
constexpr uint32_t kMachineNr = 2;
constexpr static size_t kThreadNr = 16;
constexpr static size_t kMaxCoroNr = 1;
constexpr static size_t kAccurateFactor = 50;

DEFINE_string(exec_meta, "", "The meta data of this execution");

std::vector<std::string> col_idx;
std::vector<size_t> col_x_thread_nr;
std::vector<size_t> col_x_coro_nr;
std::vector<uint64_t> col_x_lease_time;
std::vector<size_t> col_ns;
std::vector<size_t> col_test_op_nr;
std::vector<size_t> col_early_unlock_nr;
std::vector<size_t> col_late_min_ns;
std::vector<size_t> col_late_p5_ns;
std::vector<size_t> col_late_p9_ns;
std::vector<size_t> col_late_p99_ns;
std::vector<size_t> col_succ_nr;
std::vector<size_t> col_fail_nr;
std::vector<size_t> col_total_nr;

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

    std::chrono::nanoseconds lease_time_ns{0ns};

    static BenchConfig get_empty_conf(const std::string &name,
                                      size_t thread_nr,
                                      size_t coro_nr,
                                      size_t test_nr,
                                      std::chrono::nanoseconds lease_time_ns)
    {
        BenchConfig ret;
        ret.name = name;
        ret.thread_nr = thread_nr;
        ret.coro_nr = coro_nr;
        ret.test_nr = test_nr;
        ret.lease_time_ns = lease_time_ns;
        return ret;
    }
};

std::ostream &operator<<(std::ostream &os, const BenchConfig &conf)
{
    os << "{conf: name: " << conf.name << ", thread: " << conf.thread_nr
       << ", coro: " << conf.coro_nr << ", test: " << conf.test_nr
       << ", lease_time: " << util::time::to_ns(conf.lease_time_ns)
       << ", enter: " << conf.first_enter
       << ", leave: " << conf.server_should_leave
       << ", report: " << conf.should_report << "}";
    return os;
}

class ThreadsComm
{
public:
    void clear()
    {
        succ_nr_ = 0;
        fail_nr_ = 0;
        total_nr_ = 0;
        acquire_time_.clear();
    }
    template <size_t kSize>
    void collect(const Sequence<uint64_t, kSize> &seq,
                 size_t succ_nr,
                 size_t fail_nr,
                 size_t total_nr)
    {
        std::lock_guard<std::mutex> lk(mu_);
        auto vec = seq.to_vector();
        for (auto time : vec)
        {
            acquire_time_.emplace(time);
        }
        succ_nr_ += succ_nr;
        fail_nr_ += fail_nr;
        total_nr_ += total_nr;
    }

    size_t succ_nr_{0};
    size_t fail_nr_{0};
    size_t total_nr_{0};
    std::set<uint64_t> acquire_time_;
    std::mutex mu_;
};

struct AdditionalCoroCtx
{
    ssize_t thread_remain_task{0};
    size_t rdma_protection_nr{0};
    size_t succ_nr{0};
    size_t fail_nr{0};
    size_t total_nr{0};
    Sequence<uint64_t, 100> acquire_time;
};

void test_basic_client_worker(
    Patronus::pointer p,
    size_t coro_id,
    CoroYield &yield,
    const BenchConfig &conf,
    CoroExecutionContextWith<kMaxCoroNr, AdditionalCoroCtx> &ex)
{
    auto tid = p->get_thread_id();
    auto dir_id = tid;
    CoroContext ctx(tid, &yield, &ex.master(), coro_id);

    size_t rdma_protection_nr = 0;
    size_t executed_nr = 0;
    size_t succ_nr = 0;
    size_t fail_nr = 0;

    std::string key;
    std::string value;

    ChronoTimer timer;

    auto &comm = ex.get_private_data();
    while (comm.thread_remain_task > 0)
    {
        auto ac_flag = (flag_t) AcquireRequestFlag::kWithConflictDetect;
        auto lease = p->get_rlease(kServerNodeId,
                                   dir_id,
                                   GlobalAddress(0),
                                   0,
                                   8,
                                   conf.lease_time_ns,
                                   ac_flag,
                                   &ctx);
        comm.total_nr++;
        if (unlikely(!lease.success()))
        {
            CHECK(lease.ec() == AcquireRequestStatus::kMagicMwErr ||
                  lease.ec() == AcquireRequestStatus::kLockedErr);
            comm.fail_nr++;
            fail_nr++;
            std::this_thread::sleep_for(conf.lease_time_ns / kAccurateFactor);
        }
        else
        {
            comm.succ_nr++;
            succ_nr++;
            auto now = std::chrono::system_clock::now();
            auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                          now.time_since_epoch())
                          .count();
            comm.acquire_time.push_back(ns);
            std::this_thread::sleep_for(conf.lease_time_ns / kAccurateFactor);
        }

        ex.get_private_data().thread_remain_task--;
    }

    auto ns = timer.pin();

    comm.rdma_protection_nr += rdma_protection_nr;

    VLOG(1) << "[bench] rdma_protection_nr: " << rdma_protection_nr
            << ". executed: " << executed_nr << ", take: " << ns << " ns. "
            << ", ops: " << 1e9 * executed_nr / ns << " . succ_nr: " << succ_nr
            << ", fail_nr: " << fail_nr << ". ctx: " << ctx;

    ex.worker_finished(coro_id);
    ctx.yield_to_master();
}

void test_basic_client_master(
    Patronus::pointer p,
    CoroYield &yield,
    size_t test_nr,
    std::atomic<ssize_t> &atm_task_nr,
    size_t coro_nr,
    CoroExecutionContextWith<kMaxCoroNr, AdditionalCoroCtx> &ex,
    ThreadsComm &tc)
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

        if (remain <= 0)
        {
            if (ex.is_finished_all())
            {
                CHECK_LE(remain, 0);
                break;
            }
        }
    }
    const auto &comm = ex.get_private_data();
    tc.collect(comm.acquire_time, comm.succ_nr, comm.fail_nr, comm.total_nr);
}

class ConfigFactory
{
public:
    static std::vector<BenchConfig> get_conf(
        const std::string &name,
        size_t thread_nr,
        size_t coro_nr,
        size_t test_nr,
        std::chrono::nanoseconds lease_time_ns)
    {
        auto warmup_conf = BenchConfig::get_empty_conf(
            name, thread_nr, coro_nr, test_nr, lease_time_ns);
        warmup_conf.should_report = false;

        auto query_conf = warmup_conf;
        query_conf.should_report = true;
        return pipeline({warmup_conf, query_conf});
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
                      ThreadsComm &tc,
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
        tc.clear();
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
            [p, &ex, test_nr = conf.test_nr, coro_nr, &tc](CoroYield &yield) {
                test_basic_client_master(
                    p, yield, test_nr, g_total_test_nr, coro_nr, ex, tc);
            });

        master();
    }
    bar.wait();
    auto bench_ns = timer.pin();

    double ops = 1e9 * conf.test_nr / bench_ns;
    double avg_ns = 1.0 * bench_ns / conf.test_nr;
    LOG_IF(INFO, is_master)
        << "[bench] total op: " << conf.test_nr << ", ns: " << bench_ns
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
        auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                      conf.lease_time_ns)
                      .count();
        col_x_lease_time.push_back(ns);
        col_test_op_nr.push_back(conf.test_nr);
        col_ns.push_back(bench_ns);

        col_succ_nr.push_back(tc.succ_nr_);
        col_fail_nr.push_back(tc.fail_nr_);
        col_total_nr.push_back(tc.total_nr_);

        size_t early_unlock_nr{0};
        int64_t lease_time_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                conf.lease_time_ns)
                .count();

        auto min =
            std::chrono::duration_cast<std::chrono::nanoseconds>(0ns).count();
        auto max =
            std::chrono::duration_cast<std::chrono::nanoseconds>(10ms).count();
        auto rng =
            std::chrono::duration_cast<std::chrono::nanoseconds>(1us).count();
        OnePassBucketMonitor late_ns(min, max, rng);

        uint64_t before_time = 0;
        for (auto time : tc.acquire_time_)
        {
            if (before_time != 0)
            {
                CHECK_GT(time, before_time);
                int64_t diff = time - before_time;
                if (unlikely(diff < lease_time_ns))
                {
                    early_unlock_nr++;
                }
                if (unlikely(diff > lease_time_ns))
                {
                    auto more_than = diff - lease_time_ns;
                    late_ns.collect(more_than);
                }
            }
            before_time = time;
        }
        col_early_unlock_nr.push_back(early_unlock_nr);
        col_late_min_ns.push_back(late_ns.min());
        col_late_p5_ns.push_back(late_ns.percentile(0.5));
        col_late_p9_ns.push_back(late_ns.percentile(0.9));
        col_late_p99_ns.push_back(late_ns.percentile(0.99));
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

void benchmark(Patronus::pointer p,
               boost::barrier &bar,
               ThreadsComm &tc,
               bool is_client)
{
    uint64_t key = 0;
    bool is_master = p->get_thread_id() == 0;
    bar.wait();

    // very easy to saturate, no need to test many threads
    for (auto lease_time : {10us, 100us, 1 * 1000us, 10 * 1000us, 100 * 1000us})
    {
        for (size_t thread_nr : {kThreadNr})
        {
            std::this_thread::sleep_for(10 * lease_time);

            key++;
            auto expect_bench_time = 4s;
            auto test_nr =
                expect_bench_time / lease_time * thread_nr * kAccurateFactor;
            test_nr = std::min(test_nr, 100_K);
            auto confs = ConfigFactory::get_conf(
                "lease_lock", thread_nr, kMaxCoroNr, test_nr, lease_time);

            if (is_client)
            {
                for (const auto &conf : confs)
                {
                    LOG_IF(INFO, is_master) << "[conf] " << conf;
                    benchmark_client(p, bar, tc, conf, is_master, key);
                }
            }
            else
            {
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

    ThreadsComm tc;
    tc.clear();

    if (is_client)
    {
        patronus->registerClientThread();
    }
    else
    {
        patronus->registerServerThread();
    }

    for (size_t i = 0; i < kThreadNr - 1; ++i)
    {
        threads.emplace_back([patronus, &bar, &tc, is_client]() {
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

            benchmark(patronus, bar, tc, is_client);
        });
    }
    LOG(INFO) << "[bench] " << pre_patronus_explain(*patronus)
              << ". tid: " << patronus->get_thread_id();
    benchmark(patronus, bar, tc, is_client);

    for (auto &t : threads)
    {
        t.join();
    }

    StrDataFrame df;
    df.load_index(std::move(col_idx));
    df.load_column<size_t>("x_thread_nr", std::move(col_x_thread_nr));
    df.load_column<size_t>("x_coro_nr", std::move(col_x_coro_nr));
    df.load_column<uint64_t>("x_lease_time(ns)", std::move(col_x_lease_time));

    df.load_column<size_t>("test_nr(total)", std::move(col_test_op_nr));
    df.load_column<size_t>("test_ns(total)", std::move(col_ns));

    df.load_column<size_t>("early_unlock_nr", std::move(col_early_unlock_nr));
    df.load_column<size_t>("late_ns(min)", std::move(col_late_min_ns));
    df.load_column<size_t>("late_ns(p5)", std::move(col_late_p5_ns));
    df.load_column<size_t>("late_ns(p9)", std::move(col_late_p9_ns));
    df.load_column<size_t>("late_ns(p99)", std::move(col_late_p99_ns));
    df.load_column<size_t>("succ_nr", std::move(col_succ_nr));
    df.load_column<size_t>("fail_nr", std::move(col_fail_nr));
    df.load_column<size_t>("total_nr", std::move(col_total_nr));

    auto div_f = gen_F_div<size_t, size_t, double>();
    auto div_f2 = gen_F_div<double, size_t, double>();
    auto ops_f = gen_F_ops<size_t, size_t, double>();
    auto mul_f = gen_F_mul<double, size_t, double>();
    auto mul_f2 = gen_F_mul<size_t, size_t, size_t>();

    auto filename = binary_to_csv_filename(argv[0], FLAGS_exec_meta);
    df.write<std::ostream, std::string, size_t, double>(std::cout,
                                                        io_format::csv2);
    df.write<std::string, size_t, double>(filename.c_str(), io_format::csv2);

    patronus->keeper_barrier("finished", 100ms);

    LOG(INFO) << "finished. ctrl+C to quit.";
}