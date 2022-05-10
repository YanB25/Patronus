#include <algorithm>
#include <chrono>
#include <iostream>
#include <queue>
#include <set>

#include "Common.h"
#include "PerThread.h"
#include "Rdma.h"
#include "boost/thread/barrier.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "patronus/All.h"
#include "util/DataFrameF.h"
#include "util/PerformanceReporter.h"
#include "util/Rand.h"
#include "util/TimeConv.h"

using namespace define::literals;
using namespace patronus;
using namespace std::chrono_literals;

constexpr static size_t kClientThreadNr = kMaxAppThread;
constexpr static size_t kServerThreadNr = NR_DIRECTORY;

constexpr static size_t kTestTimePerThread = 1_M;
// constexpr static size_t kTestTimePerThread = 100;

std::vector<std::string> col_idx;
std::vector<size_t> col_x_alloc_size;
std::vector<size_t> col_x_thread_nr;
std::vector<size_t> col_x_coro_nr;
std::vector<size_t> col_x_lease_time_ns;
std::vector<size_t> col_alloc_nr;
std::vector<size_t> col_alloc_ns;
std::vector<uint64_t> col_lat_min;
std::vector<uint64_t> col_lat_p5;
std::vector<uint64_t> col_lat_p9;
std::vector<uint64_t> col_lat_p99;

using namespace hmdf;

DEFINE_string(exec_meta, "", "The meta data of this execution");

constexpr static size_t kCoroCnt = 32;  // max
struct CoroCommunication
{
    CoroCall workers[kCoroCnt];
    CoroCall master;
    ssize_t thread_remain_task;
    double lat_min;
    double lat_p5;
    double lat_p9;
    double lat_p99;
    std::vector<bool> finish_all;
};

struct BenchConfig
{
    size_t thread_nr;
    size_t coro_nr;
    size_t block_size;
    size_t task_nr;
    flag_t acquire_flag;
    flag_t relinquish_flag;
    bool do_not_call_relinquish{false};
    std::chrono::nanoseconds acquire_ns;
    std::string name;
    bool report;

    static BenchConfig get_empty_conf(const std::string &name,
                                      size_t thread_nr,
                                      size_t coro_nr,
                                      size_t block_size,
                                      size_t task_nr)
    {
        BenchConfig ret;
        ret.name = name;
        ret.thread_nr = thread_nr;
        ret.coro_nr = coro_nr;
        ret.block_size = block_size;
        ret.task_nr = task_nr;
        return ret;
    }
};

class BenchConfigFactory
{
public:
    static std::vector<BenchConfig> get_rlease_nothing(const std::string &name,
                                                       size_t thread_nr,
                                                       size_t coro_nr,
                                                       size_t block_size,
                                                       size_t task_nr)
    {
        flag_t acquire_flag = (flag_t) AcquireRequestFlag::kNoGc |
                              (flag_t) AcquireRequestFlag::kNoBindAny;
        flag_t relinquish_flag = (flag_t) LeaseModifyFlag::kNoRelinquishUnbind;
        auto acquire_ns = 0ns;

        auto conf = BenchConfig::get_empty_conf(
            name, thread_nr, coro_nr, block_size, task_nr);
        conf.acquire_flag = acquire_flag;
        conf.relinquish_flag = relinquish_flag;
        conf.acquire_ns = acquire_ns;
        return pipeline({conf, conf});
    }
    static std::vector<BenchConfig> get_alloc_only(const std::string &name,
                                                   size_t thread_nr,
                                                   size_t coro_nr,
                                                   size_t block_size,
                                                   size_t task_nr)
    {
        flag_t acquire_flag = (flag_t) AcquireRequestFlag::kNoGc |
                              (flag_t) AcquireRequestFlag::kNoBindAny |
                              (flag_t) AcquireRequestFlag::kWithAllocation;
        flag_t relinquish_flag = (flag_t) LeaseModifyFlag::kNoRelinquishUnbind |
                                 (flag_t) LeaseModifyFlag::kWithDeallocation;
        auto acquire_ns = 0ns;

        auto conf = BenchConfig::get_empty_conf(
            name, thread_nr, coro_nr, block_size, task_nr);
        conf.acquire_flag = acquire_flag;
        conf.relinquish_flag = relinquish_flag;
        conf.acquire_ns = acquire_ns;
        return pipeline({conf, conf});
    }
    static std::vector<BenchConfig> get_rlease_one_bind(const std::string &name,
                                                        size_t thread_nr,
                                                        size_t coro_nr,
                                                        size_t block_size,
                                                        size_t task_nr)
    {
        flag_t acquire_flag = (flag_t) AcquireRequestFlag::kNoGc |
                              (flag_t) AcquireRequestFlag::kNoBindPR;
        flag_t relinquish_flag = (flag_t) LeaseModifyFlag::kNoRelinquishUnbind;
        auto acquire_ns = 0ns;
        auto conf = BenchConfig::get_empty_conf(
            name, thread_nr, coro_nr, block_size, task_nr);
        conf.acquire_flag = acquire_flag;
        conf.relinquish_flag = relinquish_flag;
        conf.acquire_ns = acquire_ns;
        return pipeline({conf, conf});
    }
    static std::vector<BenchConfig> get_rlease_one_bind_one_unbind(
        const std::string &name,
        size_t thread_nr,
        size_t coro_nr,
        size_t block_size,
        size_t task_nr)
    {
        flag_t acquire_flag = (flag_t) AcquireRequestFlag::kNoGc |
                              (flag_t) AcquireRequestFlag::kNoBindPR;
        flag_t relinquish_flag = (flag_t) 0;
        auto acquire_ns = 0ns;
        auto conf = BenchConfig::get_empty_conf(
            name, thread_nr, coro_nr, block_size, task_nr);
        conf.acquire_flag = acquire_flag;
        conf.relinquish_flag = relinquish_flag;
        conf.acquire_ns = acquire_ns;
        return pipeline({conf, conf});
    }
    static std::vector<BenchConfig> get_rlease_one_bind_one_unbind_over_mr(
        const std::string &name,
        size_t thread_nr,
        size_t coro_nr,
        size_t block_size,
        size_t task_nr)
    {
        flag_t acquire_flag = (flag_t) AcquireRequestFlag::kNoGc |
                              (flag_t) AcquireRequestFlag::kNoBindPR |
                              (flag_t) AcquireRequestFlag::kUseMR;
        flag_t relinquish_flag = (flag_t) LeaseModifyFlag::kUseMR;
        auto acquire_ns = 0ns;
        auto conf = BenchConfig::get_empty_conf(
            name, thread_nr, coro_nr, block_size, task_nr);
        conf.acquire_flag = acquire_flag;
        conf.relinquish_flag = relinquish_flag;
        conf.acquire_ns = acquire_ns;
        return pipeline({conf, conf});
    }
    static std::vector<BenchConfig> get_rlease_one_bind_one_unbind_autogc(
        const std::string &name,
        size_t thread_nr,
        size_t coro_nr,
        size_t block_size,
        size_t task_nr)
    {
        flag_t acquire_flag = (flag_t) AcquireRequestFlag::kNoBindPR;
        flag_t relinquish_flag = (flag_t) 0;
        auto acquire_ns = 1ns;
        auto conf = BenchConfig::get_empty_conf(
            name, thread_nr, coro_nr, block_size, task_nr);
        conf.acquire_flag = acquire_flag;
        conf.relinquish_flag = relinquish_flag;
        conf.acquire_ns = acquire_ns;
        conf.do_not_call_relinquish = true;
        return pipeline({conf, conf});
    }
    static std::vector<BenchConfig> get_rlease_no_unbind(
        const std::string &name,
        size_t thread_nr,
        size_t coro_nr,
        size_t block_size,
        size_t task_nr)
    {
        flag_t acquire_flag = (flag_t) AcquireRequestFlag::kNoGc;
        flag_t relinquish_flag = (flag_t) LeaseModifyFlag::kNoRelinquishUnbind;
        auto acquire_ns = 0ns;
        auto conf = BenchConfig::get_empty_conf(
            name, thread_nr, coro_nr, block_size, task_nr);
        conf.acquire_flag = acquire_flag;
        conf.relinquish_flag = relinquish_flag;
        conf.acquire_ns = acquire_ns;
        return pipeline({conf, conf});
    }
    static std::vector<BenchConfig> get_rlease_full(const std::string &name,
                                                    size_t thread_nr,
                                                    size_t coro_nr,
                                                    size_t block_size,
                                                    size_t task_nr)
    {
        flag_t acquire_flag = (flag_t) AcquireRequestFlag::kNoGc;
        flag_t relinquish_flag = 0;
        auto acquire_ns = 0ns;
        auto conf = BenchConfig::get_empty_conf(
            name, thread_nr, coro_nr, block_size, task_nr);
        conf.acquire_flag = acquire_flag;
        conf.relinquish_flag = relinquish_flag;
        conf.acquire_ns = acquire_ns;
        return pipeline({conf, conf});
    }
    static std::vector<BenchConfig> get_rlease_full_over_mr(
        const std::string &name,
        size_t thread_nr,
        size_t coro_nr,
        size_t block_size,
        size_t task_nr)
    {
        flag_t acquire_flag = (flag_t) AcquireRequestFlag::kNoGc |
                              (flag_t) AcquireRequestFlag::kUseMR;
        flag_t relinquish_flag = (flag_t) LeaseModifyFlag::kUseMR;
        auto acquire_ns = 0ns;
        auto conf = BenchConfig::get_empty_conf(
            name, thread_nr, coro_nr, block_size, task_nr);
        conf.acquire_flag = acquire_flag;
        conf.relinquish_flag = relinquish_flag;
        conf.acquire_ns = acquire_ns;
        return pipeline({conf, conf});
    }
    static std::vector<BenchConfig> get_rlease_full_autogc(
        const std::string &name,
        size_t thread_nr,
        size_t coro_nr,
        size_t block_size,
        size_t task_nr)
    {
        flag_t acquire_flag = 0;
        flag_t relinquish_flag = 0;
        auto acquire_ns = 1ns;  // immediately gc.
        auto conf = BenchConfig::get_empty_conf(
            name, thread_nr, coro_nr, block_size, task_nr);
        conf.acquire_flag = acquire_flag;
        conf.relinquish_flag = relinquish_flag;
        conf.acquire_ns = acquire_ns;
        conf.do_not_call_relinquish = true;
        return pipeline({conf, conf});
    }
    static std::vector<BenchConfig> get_rlease_alloc(const std::string &name,
                                                     size_t thread_nr,
                                                     size_t coro_nr,
                                                     size_t block_size,
                                                     size_t task_nr)
    {
        flag_t acquire_flag = (flag_t) AcquireRequestFlag::kWithAllocation |
                              (flag_t) AcquireRequestFlag::kNoBindPR |
                              (flag_t) AcquireRequestFlag::kNoGc;
        flag_t relinquish_flag = (flag_t) LeaseModifyFlag::kWithDeallocation;
        auto acquire_ns = 0ns;
        auto conf = BenchConfig::get_empty_conf(
            name, thread_nr, coro_nr, block_size, task_nr);
        conf.acquire_flag = acquire_flag;
        conf.relinquish_flag = relinquish_flag;
        conf.acquire_ns = acquire_ns;
        return pipeline({conf, conf});
    }
    static std::vector<BenchConfig> get_rlease_alloc_no_unbind(
        const std::string &name,
        size_t thread_nr,
        size_t coro_nr,
        size_t block_size,
        size_t task_nr)
    {
        flag_t acquire_flag = (flag_t) AcquireRequestFlag::kWithAllocation |
                              (flag_t) AcquireRequestFlag::kNoBindPR |
                              (flag_t) AcquireRequestFlag::kNoGc;
        flag_t relinquish_flag = (flag_t) LeaseModifyFlag::kNoRelinquishUnbind |
                                 (flag_t) LeaseModifyFlag::kWithDeallocation;
        auto acquire_ns = 0ns;
        auto conf = BenchConfig::get_empty_conf(
            name, thread_nr, coro_nr, block_size, task_nr);
        conf.acquire_flag = acquire_flag;
        conf.relinquish_flag = relinquish_flag;
        conf.acquire_ns = acquire_ns;
        return pipeline({conf, conf});
    }

private:
    static std::vector<BenchConfig> pipeline(std::vector<BenchConfig> &&confs)
    {
        for (auto &conf : confs)
        {
            conf.report = false;
        }
        confs.back().report = true;
        return confs;
    }
};

void reg_result(const std::string &name,
                size_t test_times,
                size_t total_ns,
                size_t block_size,
                size_t thread_nr,
                size_t coro_nr,
                std::chrono::nanoseconds acquire_ns,
                double lat_min,
                double lat_p5,
                double lat_p9,
                double lat_p99)
{
    col_idx.push_back(name);
    col_x_alloc_size.push_back(block_size);
    col_x_thread_nr.push_back(thread_nr);
    col_x_coro_nr.push_back(coro_nr);
    auto ns = util::time::to_ns(acquire_ns);
    col_x_lease_time_ns.push_back(ns);
    col_alloc_nr.push_back(test_times);
    col_alloc_ns.push_back(total_ns);
    col_lat_min.push_back(lat_min);
    col_lat_p5.push_back(lat_p5);
    col_lat_p9.push_back(lat_p9);
    col_lat_p99.push_back(lat_p99);
}

void bench_alloc_thread_coro_master(Patronus::pointer patronus,
                                    CoroYield &yield,
                                    size_t test_times,
                                    std::atomic<ssize_t> &work_nr,
                                    CoroCommunication &coro_comm,
                                    size_t coro_nr)
{
    auto tid = patronus->get_thread_id();

    CoroContext mctx(tid, &yield, coro_comm.workers);
    CHECK(mctx.is_master());

    LOG_IF(WARNING, test_times % 1000 != 0)
        << "test_times % 1000 != 0. Will introduce 1/1000 performance error "
           "per thread";

    ssize_t task_per_sync = test_times / 1000;
    LOG_IF(WARNING, task_per_sync <= (ssize_t) coro_nr)
        << "test_times < coro_nr.";

    task_per_sync =
        std::max(task_per_sync, ssize_t(coro_nr));  // at least coro_nr

    coro_comm.thread_remain_task = task_per_sync;
    ssize_t remain =
        work_nr.fetch_sub(task_per_sync, std::memory_order_relaxed) -
        task_per_sync;

    VLOG(1) << "[bench] thread_remain_task init to " << task_per_sync
            << ", task_per_sync: " << task_per_sync
            << ", remain (after fetched): " << remain;

    for (size_t i = 0; i < coro_nr; ++i)
    {
        mctx.yield_to_worker(i);
    }

    static thread_local coro_t coro_buf[2 * kCoroCnt];
    while (true)
    {
        if (coro_comm.thread_remain_task <= 2 * ssize_t(coro_nr))
        {
            auto cur_task_nr = std::min(remain, task_per_sync);
            // VLOG(1) << "[coro] before cur_task_nr: " << cur_task_nr
            //         << ", remain: " << remain
            //         << ", thread_remain_task:" <<
            //         coro_comm.thread_remain_task
            //         << ", task_per_sync: " << task_per_sync;
            if (cur_task_nr >= 0)
            {
                remain =
                    work_nr.fetch_sub(cur_task_nr, std::memory_order_relaxed) -
                    cur_task_nr;
                if (remain >= 0)
                {
                    coro_comm.thread_remain_task += cur_task_nr;
                }
                // NOTE:
                // The worker may do slightly more task, because here, the
                // remain may be below zeros
            }
            // VLOG(1) << "[coro] after remain: " << remain
            //         << ", remain: " << remain
            //         << ", thread_remain_task: " <<
            //         coro_comm.thread_remain_task
            //         << ", task_per_sync: " << task_per_sync;
        }

        auto nr =
            patronus->try_get_client_continue_coros(coro_buf, 2 * kCoroCnt);

        for (size_t i = 0; i < nr; ++i)
        {
            auto coro_id = coro_buf[i];
            DVLOG(1) << "[coro] yielding due to CQE";
            mctx.yield_to_worker(coro_id);
        }

        if (remain <= 0)
        {
            bool finish_all = std::all_of(std::begin(coro_comm.finish_all),
                                          std::end(coro_comm.finish_all),
                                          [](bool i) { return i; });
            if (finish_all)
            {
                CHECK_LE(remain, 0);
                break;
            }
        }
    }

    // LOG(WARNING) << "[coro] all worker finish their work. exiting...";
}

void bench_alloc_thread_coro_worker(Patronus::pointer patronus,
                                    size_t coro_id,
                                    CoroYield &yield,
                                    CoroCommunication &coro_comm,
                                    bool is_master,
                                    size_t alloc_size,
                                    flag_t acquire_flag,
                                    flag_t relinquish_flag,
                                    std::chrono::nanoseconds acquire_ns,
                                    bool do_not_call_relinquish)
{
    auto tid = patronus->get_thread_id();
    auto dir_id = tid % kServerThreadNr;
    auto server_nid = ::config::get_server_nids().front();

    CoroContext ctx(tid, &yield, &coro_comm.master, coro_id);

    size_t fail_nr = 0;
    size_t succ_nr = 0;

    auto min = util::time::to_ns(0ns);
    auto max = util::time::to_ns(10ms);
    auto range = util::time::to_ns(1us);
    OnePassBucketMonitor lat_m(min, max, range);

    ChronoTimer timer;
    ChronoTimer op_timer;
    while (coro_comm.thread_remain_task > 0)
    {
        bool succ = true;
        // VLOG(4) << "[coro] tid " << tid << " get_rlease. coro: " << ctx;
        if (is_master && coro_id == 0)
        {
            op_timer.pin();
        }
        auto lease = patronus->get_rlease(server_nid,
                                          dir_id,
                                          nullgaddr,
                                          0 /* alloc_hint */,
                                          alloc_size,
                                          acquire_ns,
                                          acquire_flag,
                                          &ctx);
        succ = lease.success();

        if (succ)
        {
            succ_nr++;
            if (!do_not_call_relinquish)
            {
                patronus->relinquish(
                    lease, 0 /* hint */, relinquish_flag, &ctx);
            }
            coro_comm.thread_remain_task--;
            // LOG_IF(INFO, succ_nr % 10_K == 0)
            //     << "[detail] finished 10K succeess. total: " << succ_nr
            //     << ". coro: " << ctx;
        }
        else
        {
            fail_nr++;
            // LOG_IF(INFO, fail_nr % 1_K == 0)
            //     << "[detail] finished 1K failed. total: " << fail_nr
            //     << ". coro: " << ctx;
            CHECK_EQ(lease.ec(), AcquireRequestStatus::kMagicMwErr)
                << "** only this kind of error is possible.";
        }
        if (is_master && coro_id == 0)
        {
            auto ns = op_timer.pin();
            lat_m.collect(ns);
        }
    }
    auto total_ns = timer.pin();
    if (is_master && coro_id == 0)
    {
        coro_comm.lat_min = lat_m.min();
        coro_comm.lat_p5 = lat_m.percentile(0.5);
        coro_comm.lat_p9 = lat_m.percentile(0.9);
        coro_comm.lat_p99 = lat_m.percentile(0.99);
    }

    coro_comm.finish_all[coro_id] = true;

    VLOG(1) << "[bench] tid " << tid << " got " << fail_nr
            << " failed lease. succ lease: " << succ_nr << " within "
            << total_ns << " ns. coro: " << ctx;
    ctx.yield_to_master();
    CHECK(false) << "yield back to me.";
}

struct BenchResult
{
    double lat_min;
    double lat_p5;
    double lat_p9;
    double lat_p99;
};

BenchResult bench_alloc_thread_coro(Patronus::pointer patronus,
                                    size_t alloc_size,
                                    size_t test_times,
                                    bool is_master,
                                    std::atomic<ssize_t> &work_nr,
                                    size_t coro_nr,
                                    flag_t acquire_flag,
                                    flag_t relinquish_flag,
                                    std::chrono::nanoseconds acquire_ns,
                                    bool do_not_call_relinquish)
{
    auto tid = patronus->get_thread_id();
    auto dir_id = tid % kServerThreadNr;

    VLOG(1) << "[coro] client tid " << tid << " bind to core " << tid
            << ", using dir_id " << dir_id;
    CoroCommunication coro_comm;
    coro_comm.finish_all.resize(coro_nr);

    for (size_t i = 0; i < coro_nr; ++i)
    {
        coro_comm.workers[i] =
            CoroCall([patronus,
                      coro_id = i,
                      &coro_comm,
                      alloc_size,
                      acquire_flag,
                      relinquish_flag,
                      acquire_ns,
                      is_master,
                      do_not_call_relinquish](CoroYield &yield) {
                bench_alloc_thread_coro_worker(patronus,
                                               coro_id,
                                               yield,
                                               coro_comm,
                                               is_master,
                                               alloc_size,
                                               acquire_flag,
                                               relinquish_flag,
                                               acquire_ns,
                                               do_not_call_relinquish);
            });
    }

    coro_comm.master =
        CoroCall([patronus, test_times, &work_nr, &coro_comm, coro_nr](
                     CoroYield &yield) {
            bench_alloc_thread_coro_master(
                patronus, yield, test_times, work_nr, coro_comm, coro_nr);
        });

    coro_comm.master();
    return BenchResult{coro_comm.lat_min,
                       coro_comm.lat_p5,
                       coro_comm.lat_p9,
                       coro_comm.lat_p99};
}

// void bench_template_coro(Patronus::pointer patronus,
//                          size_t test_times,
//                          size_t alloc_size,
//                          size_t coro_nr,
//                          std::atomic<ssize_t> &work_nr,
//                          flag_t acquire_flag,
//                          flag_t relinquish_flag,
//                          std::chrono::nanoseconds acquire_ns)
// {
//     auto tid = patronus->get_thread_id();
//     auto dir_id = tid % kServerThreadNr;
//     CHECK_LT(dir_id, NR_DIRECTORY)
//         << "Failed to run this case. Two threads should not share the same "
//            "directory, otherwise the one thread will poll CQE from other "
//            "threads.";
//     bench_alloc_thread_coro(patronus,
//                             alloc_size,
//                             test_times,
//                             is_master,
//                             work_nr,
//                             coro_nr,
//                             acquire_flag,
//                             relinquish_flag,
//                             acquire_ns);
// }

void bench_template(const std::string &name,
                    Patronus::pointer patronus,
                    boost::barrier &bar,
                    std::atomic<ssize_t> &work_nr,
                    size_t test_times,
                    size_t alloc_size,
                    size_t thread_nr,
                    size_t coro_nr,
                    bool is_master,
                    bool report,
                    flag_t acquire_flag,
                    flag_t relinquish_flag,
                    std::chrono::nanoseconds acquire_ns,
                    bool do_not_call_relinquish)

{
    if (is_master)
    {
        work_nr = test_times;
        LOG(INFO) << "[bench] BENCH: " << name << ", thread_nr: " << thread_nr
                  << ", alloc_size: " << alloc_size << ", coro_nr: " << coro_nr
                  << ", report: " << report;
    }

    bar.wait();
    ChronoTimer timer;

    auto tid = patronus->get_thread_id();
    BenchResult r;

    if (tid < thread_nr)
    {
        r = bench_alloc_thread_coro(patronus,
                                    alloc_size,
                                    test_times,
                                    is_master,
                                    work_nr,
                                    coro_nr,
                                    acquire_flag,
                                    relinquish_flag,
                                    acquire_ns,
                                    do_not_call_relinquish);
    }

    bar.wait();
    auto total_ns = timer.pin();
    if (is_master && report)
    {
        reg_result(name,
                   test_times,
                   total_ns,
                   alloc_size,
                   thread_nr,
                   coro_nr,
                   acquire_ns,
                   r.lat_min,
                   r.lat_p5,
                   r.lat_p9,
                   r.lat_p99);
    }
    bar.wait();
}

void run_benchmark_server(Patronus::pointer patronus,
                          const BenchConfig &conf,
                          bool is_master,
                          uint64_t key)
{
    std::ignore = conf;
    if (is_master)
    {
        patronus->finished(key);
    }

    auto tid = patronus->get_thread_id();
    LOG(INFO) << "[coro] server thread tid " << tid;

    LOG(INFO) << "[bench] BENCH: " << conf.name
              << ", thread_nr: " << conf.thread_nr
              << ", alloc_size: " << conf.block_size
              << ", coro_nr: " << conf.coro_nr << ", report: " << conf.report;

    patronus->server_serve(key);
}
std::atomic<ssize_t> shared_task_nr;

void run_benchmark_client(Patronus::pointer patronus,
                          const BenchConfig &conf,
                          boost::barrier &bar,
                          bool is_master,
                          uint64_t &key)
{
    if (is_master)
    {
        shared_task_nr = conf.task_nr;
    }
    bar.wait();

    bench_template(conf.name,
                   patronus,
                   bar,
                   shared_task_nr,
                   conf.task_nr,
                   conf.block_size,
                   conf.thread_nr,
                   conf.coro_nr,
                   is_master,
                   conf.report,
                   conf.acquire_flag,
                   conf.relinquish_flag,
                   conf.acquire_ns,
                   conf.do_not_call_relinquish);
    bar.wait();
    if (is_master)
    {
        patronus->finished(key);
    }
}

void run_benchmark(Patronus::pointer patronus,
                   const std::vector<BenchConfig> &configs,
                   boost::barrier &bar,
                   bool is_client,
                   bool is_master,
                   uint64_t &key)
{
    for (const auto &conf : configs)
    {
        key++;
        if (is_client)
        {
            run_benchmark_client(patronus, conf, bar, is_master, key);
        }
        else
        {
            run_benchmark_server(patronus, conf, is_master, key);
        }
        if (is_master)
        {
            patronus->keeper_barrier(std::to_string(key), 100ms);
        }
    }
}

void benchmark(Patronus::pointer patronus,
               boost::barrier &bar,
               bool is_client,
               bool is_master)
{
    bar.wait();

    size_t key = 0;
    // for (size_t thread_nr : {1, 2, 4, 8, 16})
    // for (size_t thread_nr : {1, 4, 16})
    // for (size_t thread_nr : {1, 16})
    for (size_t thread_nr : {16})
    {
        CHECK_LE(thread_nr, kMaxAppThread);
        // for (size_t block_size : {64ul, 2_MB, 128_MB})
        for (size_t block_size : {64ul})
        {
            // for (size_t coro_nr : {1, 32})
            for (size_t coro_nr : {32})
            {
                auto total_test_times = kTestTimePerThread * thread_nr;

                {
                    auto configs = BenchConfigFactory::get_rlease_nothing(
                        "w/o(*)",
                        thread_nr,
                        coro_nr,
                        block_size,
                        total_test_times);
                    run_benchmark(
                        patronus, configs, bar, is_client, is_master, key);
                }
                {
                    auto configs =
                        BenchConfigFactory::get_alloc_only("alloc w/o(*)",
                                                           thread_nr,
                                                           coro_nr,
                                                           block_size,
                                                           total_test_times);
                    run_benchmark(
                        patronus, configs, bar, is_client, is_master, key);
                }
                {
                    auto configs = BenchConfigFactory::get_rlease_one_bind(
                        "w(buf) w/o(pr unbind gc)",
                        thread_nr,
                        coro_nr,
                        block_size,
                        total_test_times);
                    run_benchmark(
                        patronus, configs, bar, is_client, is_master, key);
                }
                {
                    auto configs =
                        BenchConfigFactory::get_rlease_one_bind_one_unbind(
                            "w(buf unbind) w/o(pr gc)",
                            thread_nr,
                            coro_nr,
                            block_size,
                            total_test_times);
                    run_benchmark(
                        patronus, configs, bar, is_client, is_master, key);
                }
                {
                    auto configs = BenchConfigFactory::
                        get_rlease_one_bind_one_unbind_over_mr(
                            "[MR] w(buf unbind) w/o(pr gc)",
                            thread_nr,
                            coro_nr,
                            block_size,
                            total_test_times / 100);
                    run_benchmark(
                        patronus, configs, bar, is_client, is_master, key);
                }
                {
                    auto configs = BenchConfigFactory::
                        get_rlease_one_bind_one_unbind_autogc(
                            "w(buf unbind gc) w/o(pr)",
                            thread_nr,
                            coro_nr,
                            block_size,
                            total_test_times);
                    run_benchmark(
                        patronus, configs, bar, is_client, is_master, key);
                }
                {
                    auto configs = BenchConfigFactory::get_rlease_full(
                        "w(buf pr unbind) w/o(gc)",
                        thread_nr,
                        coro_nr,
                        block_size,
                        total_test_times);
                    run_benchmark(
                        patronus, configs, bar, is_client, is_master, key);
                }
                {
                    auto configs = BenchConfigFactory::get_rlease_full_over_mr(
                        "[MR] w(buf pr unbind) w/o(gc)",
                        thread_nr,
                        coro_nr,
                        block_size,
                        // MR is so slow
                        total_test_times / 100);
                    run_benchmark(
                        patronus, configs, bar, is_client, is_master, key);
                }
                {
                    auto configs = BenchConfigFactory::get_rlease_full_autogc(
                        "w(buf pr unbind gc)",
                        thread_nr,
                        coro_nr,
                        block_size,
                        total_test_times);
                    run_benchmark(
                        patronus, configs, bar, is_client, is_master, key);
                }
            }
        }
    }

    bar.wait();
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    PatronusConfig config;
    config.machine_nr = ::config::kMachineNr;
    config.block_class = {2_MB};
    config.block_ratio = {1};

    auto patronus = Patronus::ins(config);

    auto nid = patronus->get_node_id();
    if (::config::is_client(nid))
    {
        std::vector<std::thread> threads;
        boost::barrier bar(kClientThreadNr);
        for (size_t i = 0; i < kClientThreadNr - 1; ++i)
        {
            // used by all the threads to synchronize their works
            threads.emplace_back([patronus, &bar]() {
                patronus->registerClientThread();
                bar.wait();
                benchmark(
                    patronus, bar, true /* is_client */, false /* is_master */);
            });
        }
        patronus->registerClientThread();
        bar.wait();
        patronus->keeper_barrier("begin", 100ms);
        benchmark(patronus, bar, true /* is_client */, true /* is_master */);

        for (auto &t : threads)
        {
            t.join();
        }

        auto tid = patronus->get_thread_id();
        LOG(INFO) << "Client calling finished with tid " << tid;
    }
    else
    {
        boost::barrier bar(kServerThreadNr);
        std::vector<std::thread> threads;
        for (size_t i = 0; i < kServerThreadNr - 1; ++i)
        {
            threads.emplace_back([patronus, &bar]() {
                patronus->registerServerThread();
                bar.wait();
                benchmark(patronus,
                          bar,
                          false /* is_client */,
                          false /* is_master */);
            });
        }
        patronus->registerServerThread();
        bar.wait();
        patronus->keeper_barrier("begin", 100ms);
        benchmark(patronus, bar, false /* is_client */, true /* is_master */);

        for (auto &t : threads)
        {
            t.join();
        }
    }

    StrDataFrame df;
    df.load_index(std::move(col_idx));
    df.load_column<size_t>("x_alloc_size", std::move(col_x_alloc_size));
    df.load_column<size_t>("x_thread_nr", std::move(col_x_thread_nr));
    df.load_column<size_t>("x_coro_nr", std::move(col_x_coro_nr));
    df.load_column<size_t>("x_lease_time(ns)", std::move(col_x_lease_time_ns));
    df.load_column<size_t>("alloc_nr(total)", std::move(col_alloc_nr));
    df.load_column<size_t>("alloc_ns(total)", std::move(col_alloc_ns));

    auto div_f = gen_F_div<size_t, size_t, double>();
    auto div_f2 = gen_F_div<double, size_t, double>();
    auto ops_f = gen_F_ops<size_t, size_t, double>();
    auto mul_f = gen_F_mul<double, size_t, double>();
    df.consolidate<size_t, size_t, double>(
        "alloc_ns(total)", "alloc_nr(total)", "alloc lat", div_f, false);
    df.consolidate<size_t, size_t, double>(
        "alloc_nr(total)", "alloc_ns(total)", "alloc ops(total)", ops_f, false);
    df.consolidate<double, size_t, double>(
        "alloc ops(total)", "x_thread_nr", "alloc ops(thread)", div_f2, false);

    // calculate cluster numbers
    auto client_nr = ::config::get_client_nids().size();
    auto replace_mul_f = gen_replace_F_mul<double>(client_nr);
    df.load_column<double>("alloc ops(cluster)",
                           df.get_column<double>("alloc ops(total)"));
    df.replace<double>("alloc ops(cluster)", replace_mul_f);

    df.load_column<uint64_t>("lat min(ns)", std::move(col_lat_min));
    df.load_column<uint64_t>("lat p5(ns)", std::move(col_lat_p5));
    df.load_column<uint64_t>("lat p9(ns)", std::move(col_lat_p9));
    df.load_column<uint64_t>("lat p99(ns)", std::move(col_lat_p99));

    auto filename = binary_to_csv_filename(argv[0], FLAGS_exec_meta);
    df.write<std::ostream, std::string, size_t, double>(std::cout,
                                                        io_format::csv2);
    df.write<std::string, size_t, double>(filename.c_str(), io_format::csv2);

    patronus->keeper_barrier("finished", 100ms);
    LOG(INFO) << "Exiting...";
}
