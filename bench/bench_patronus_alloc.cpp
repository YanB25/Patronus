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
#include "util/Rand.h"

using namespace define::literals;
using namespace patronus;
using namespace std::chrono_literals;

constexpr uint32_t kMachineNr = 2;
constexpr uint16_t kClientNodeId = 1;
constexpr uint16_t kServerNodeId = 0;
// constexpr static size_t kMwPoolTotalSize = 8192;
constexpr static size_t kThreadNr = 16;
static_assert(kThreadNr <= kMaxAppThread);

std::vector<std::string> col_idx;
std::vector<size_t> col_x_alloc_size;
std::vector<size_t> col_x_thread_nr;
std::vector<size_t> col_x_coro_nr;
std::vector<size_t> col_alloc_nr;
std::vector<size_t> col_alloc_ns;

using namespace hmdf;

DEFINE_string(exec_meta, "", "The meta data of this execution");

constexpr static size_t kCoroCnt = 16;  // max
struct CoroCommunication
{
    CoroCall workers[kCoroCnt];
    CoroCall master;
    ssize_t thread_remain_task;
    std::vector<bool> finish_all;
};

void reg_result(const std::string &name,
                size_t test_times,
                size_t total_ns,
                size_t block_size,
                size_t thread_nr,
                size_t coro_nr)
{
    col_idx.push_back(name);
    col_x_alloc_size.push_back(block_size);
    col_x_thread_nr.push_back(thread_nr);
    col_x_coro_nr.push_back(coro_nr);
    col_alloc_nr.push_back(test_times);
    col_alloc_ns.push_back(total_ns);
}

void bench_alloc_thread_coro_master(Patronus::pointer patronus,
                                    CoroYield &yield,
                                    size_t test_times,
                                    std::atomic<ssize_t> &work_nr,
                                    CoroCommunication &coro_comm,
                                    size_t coro_nr)
{
    auto tid = patronus->get_thread_id();
    auto mid = tid;

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

        auto nr = patronus->try_get_client_continue_coros(
            mid, coro_buf, 2 * kCoroCnt);

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
                                    size_t alloc_size,
                                    uint8_t acquire_flag,
                                    uint8_t relinquish_flag)
{
    auto tid = patronus->get_thread_id();
    auto dir_id = tid;

    CoroContext ctx(tid, &yield, &coro_comm.master, coro_id);

    // VLOG(1) << "[coro] worker tid: " << tid << " coro: " << ctx;
    size_t fail_nr = 0;
    size_t succ_nr = 0;

    ChronoTimer timer;
    while (coro_comm.thread_remain_task > 0)
    {
        bool succ = true;
        VLOG(4) << "[coro] tid " << tid << " get_rlease. coro: " << ctx;
        auto lease = patronus->get_rlease(
            kServerNodeId, dir_id, 0, alloc_size, 0ns, acquire_flag, &ctx);
        succ = lease.success();

        if (succ)
        {
            succ_nr++;
            VLOG(4) << "[coro] tid " << tid << " relinquish. coro: " << ctx;
            patronus->relinquish(lease, relinquish_flag, &ctx);
            coro_comm.thread_remain_task--;
        }
        else
        {
            fail_nr++;
            DVLOG(4) << "[coro] tid " << tid
                     << "get_rlease failed. coro: " << ctx;
        }
    }
    auto total_ns = timer.pin();

    coro_comm.finish_all[coro_id] = true;

    VLOG(1) << "[bench] tid " << tid << " got " << fail_nr
            << " failed lease. succ lease: " << succ_nr << " within "
            << total_ns << " ns. coro: " << ctx;
    ctx.yield_to_master();
    CHECK(false) << "yield back to me.";
}

void bench_alloc_thread_coro(Patronus::pointer patronus,
                             size_t alloc_size,
                             size_t test_times,
                             std::atomic<ssize_t> &work_nr,
                             size_t coro_nr,
                             uint8_t acquire_flag,
                             uint8_t relinquish_flag)
{
    auto tid = patronus->get_thread_id();
    auto dir_id = tid;

    VLOG(1) << "[coro] client tid " << tid << " bind to core " << tid
            << ", using dir_id " << dir_id;
    CoroCommunication coro_comm;
    coro_comm.finish_all.resize(coro_nr);

    for (size_t i = 0; i < coro_nr; ++i)
    {
        coro_comm.workers[i] = CoroCall([patronus,
                                         coro_id = i,
                                         &coro_comm,
                                         alloc_size,
                                         acquire_flag,
                                         relinquish_flag](CoroYield &yield) {
            bench_alloc_thread_coro_worker(patronus,
                                           coro_id,
                                           yield,
                                           coro_comm,
                                           alloc_size,
                                           acquire_flag,
                                           relinquish_flag);
        });
    }

    coro_comm.master =
        CoroCall([patronus, test_times, &work_nr, &coro_comm, coro_nr](
                     CoroYield &yield) {
            bench_alloc_thread_coro_master(
                patronus, yield, test_times, work_nr, coro_comm, coro_nr);
        });

    coro_comm.master();
}

void bench_template_coro(Patronus::pointer patronus,
                         size_t test_times,
                         size_t alloc_size,
                         size_t coro_nr,
                         std::atomic<ssize_t> &work_nr,
                         uint8_t acquire_flag,
                         uint8_t relinquish_flag)
{
    auto tid = patronus->get_thread_id();
    auto dir_id = tid;
    CHECK_LT(dir_id, NR_DIRECTORY)
        << "Failed to run this case. Two threads should not share the same "
           "directory, otherwise the one thread will poll CQE from other "
           "threads.";
    bench_alloc_thread_coro(patronus,
                            alloc_size,
                            test_times,
                            work_nr,
                            coro_nr,
                            acquire_flag,
                            relinquish_flag);
}

void bench_template(const std::string &name,
                    Patronus::pointer patronus,
                    boost::barrier &bar,
                    std::atomic<ssize_t> &work_nr,
                    size_t test_times,
                    size_t alloc_size,
                    size_t thread_nr,
                    size_t coro_nr,
                    bool is_master,
                    bool warm_up,
                    uint8_t acquire_flag,
                    uint8_t relinquish_flag)

{
    if (is_master)
    {
        work_nr = test_times;
        LOG(INFO) << "[bench] BENCH: " << name << ", thread_nr: " << thread_nr
                  << ", alloc_size: " << alloc_size << ", coro_nr: " << coro_nr
                  << ", warm_up: " << warm_up;
    }

    bar.wait();
    ChronoTimer timer;

    auto tid = patronus->get_thread_id();
    auto dir_id = tid;

    CHECK_LT(dir_id, NR_DIRECTORY)
        << "Failed to run this case. Two threads should not share the same "
           "directory, otherwise the one thread will poll CQE from other "
           "threads.";

    if (tid < thread_nr)
    {
        bench_alloc_thread_coro(patronus,
                                alloc_size,
                                test_times,
                                work_nr,
                                coro_nr,
                                acquire_flag,
                                relinquish_flag);
    }

    bar.wait();
    auto total_ns = timer.pin();
    if (is_master && !warm_up)
    {
        reg_result(name, test_times, total_ns, alloc_size, thread_nr, coro_nr);
    }
    bar.wait();
}

void bench_patronus_get_rlease_nothing(Patronus::pointer patronus,
                                       boost::barrier &bar,
                                       std::atomic<ssize_t> &work_nr,
                                       size_t test_times,
                                       size_t alloc_size,
                                       size_t thread_nr,
                                       size_t coro_nr,
                                       bool is_master,
                                       bool warm_up)
{
    uint8_t acquire_flag = (uint8_t) AcquireRequestFlag::kNoGc |
                           (uint8_t) AcquireRequestFlag::kDebugNoBindAny;
    uint8_t relinquish_flag = (uint8_t) LeaseModifyFlag::kNoRelinquishUnbind;
    return bench_template("get_rlease w/o(*)",
                          patronus,
                          bar,
                          work_nr,
                          test_times,
                          alloc_size,
                          thread_nr,
                          coro_nr,
                          is_master,
                          warm_up,
                          acquire_flag,
                          relinquish_flag);
}

void bench_patronus_get_rlease_one_bind(Patronus::pointer patronus,
                                        boost::barrier &bar,
                                        std::atomic<ssize_t> &work_nr,
                                        size_t test_times,
                                        size_t alloc_size,
                                        size_t thread_nr,
                                        size_t coro_nr,
                                        bool is_master,
                                        bool warm_up)
{
    uint8_t acquire_flag = (uint8_t) AcquireRequestFlag::kNoGc |
                           (uint8_t) AcquireRequestFlag::kDebugNoBindPR;
    uint8_t relinquish_flag = (uint8_t) LeaseModifyFlag::kNoRelinquishUnbind;
    return bench_template("get_rlease w(buf) w/o(pr unbind gc)",
                          patronus,
                          bar,
                          work_nr,
                          test_times,
                          alloc_size,
                          thread_nr,
                          coro_nr,
                          is_master,
                          warm_up,
                          acquire_flag,
                          relinquish_flag);
}

void bench_patronus_get_rlease_no_unbind(Patronus::pointer patronus,
                                         boost::barrier &bar,
                                         std::atomic<ssize_t> &work_nr,
                                         size_t test_times,
                                         size_t alloc_size,
                                         size_t thread_nr,
                                         size_t coro_nr,
                                         bool is_master,
                                         bool warm_up)
{
    uint8_t acquire_flag = (uint8_t) AcquireRequestFlag::kNoGc;
    uint8_t relinquish_flag = (uint8_t) LeaseModifyFlag::kNoRelinquishUnbind;
    return bench_template("get_rlease w(pr buf) w/o(unbind gc)",
                          patronus,
                          bar,
                          work_nr,
                          test_times,
                          alloc_size,
                          thread_nr,
                          coro_nr,
                          is_master,
                          warm_up,
                          acquire_flag,
                          relinquish_flag);
}

void bench_patronus_get_rlease_full(Patronus::pointer patronus,
                                    boost::barrier &bar,
                                    std::atomic<ssize_t> &work_nr,
                                    size_t test_times,
                                    size_t alloc_size,
                                    size_t thread_nr,
                                    size_t coro_nr,
                                    bool is_master,
                                    bool warm_up)
{
    uint8_t acquire_flag = (uint8_t) AcquireRequestFlag::kNoGc;
    uint8_t relinquish_flag = 0;
    return bench_template("get_rlease w(pr buf unbind) w/o(gc)",
                          patronus,
                          bar,
                          work_nr,
                          test_times,
                          alloc_size,
                          thread_nr,
                          coro_nr,
                          is_master,
                          warm_up,
                          acquire_flag,
                          relinquish_flag);
}

void bench_patronus_get_rlease_full_auto_gc(Patronus::pointer patronus,
                                            boost::barrier &bar,
                                            std::atomic<ssize_t> &work_nr,
                                            size_t test_times,
                                            size_t alloc_size,
                                            size_t thread_nr,
                                            size_t coro_nr,
                                            bool is_master,
                                            bool warm_up)
{
    uint8_t acquire_flag = 0;
    uint8_t relinquish_flag = 0;
    return bench_template("get_rlease w(pr buf unbind gc)",
                          patronus,
                          bar,
                          work_nr,
                          test_times,
                          alloc_size,
                          thread_nr,
                          coro_nr,
                          is_master,
                          warm_up,
                          acquire_flag,
                          relinquish_flag);
}
void bench_patronus_alloc(Patronus::pointer patronus,
                          boost::barrier &bar,
                          std::atomic<ssize_t> &work_nr,
                          size_t test_times,
                          size_t alloc_size,
                          size_t thread_nr,
                          size_t coro_nr,
                          bool is_master,
                          bool warm_up)
{
    uint8_t acquire_flag = (uint8_t) AcquireRequestFlag::kTypeAllocation |
                           (uint8_t) AcquireRequestFlag::kNoGc;
    uint8_t relinquish_flag = (uint8_t) LeaseModifyFlag::kTypeDeallocation;
    return bench_template("alloc w(unbind)",
                          patronus,
                          bar,
                          work_nr,
                          test_times,
                          alloc_size,
                          thread_nr,
                          coro_nr,
                          is_master,
                          warm_up,
                          acquire_flag,
                          relinquish_flag);
}
void bench_patronus_alloc_no_unbind(Patronus::pointer patronus,
                                    boost::barrier &bar,
                                    std::atomic<ssize_t> &work_nr,
                                    size_t test_times,
                                    size_t alloc_size,
                                    size_t thread_nr,
                                    size_t coro_nr,
                                    bool is_master,
                                    bool warm_up)
{
    uint8_t acquire_flag = (uint8_t) AcquireRequestFlag::kTypeAllocation |
                           (uint8_t) AcquireRequestFlag::kNoGc;
    uint8_t relinquish_flag = (uint8_t) LeaseModifyFlag::kNoRelinquishUnbind |
                              (uint8_t) LeaseModifyFlag::kTypeDeallocation;
    return bench_template("alloc w/o(unbind)",
                          patronus,
                          bar,
                          work_nr,
                          test_times,
                          alloc_size,
                          thread_nr,
                          coro_nr,
                          is_master,
                          warm_up,
                          acquire_flag,
                          relinquish_flag);
}

void server(Patronus::pointer patronus, bool is_master)
{
    patronus->registerServerThread();
    if (is_master)
    {
        patronus->finished();
    }

    auto tid = patronus->get_thread_id();
    auto mid = tid;
    LOG(INFO) << "[coro] server thread tid " << tid << " for mid " << mid;

    patronus->server_serve(mid);
}

void client(Patronus::pointer patronus,
            boost::barrier &bar,
            std::atomic<ssize_t> &task_nr,
            bool is_master)
{
    patronus->registerClientThread();

    bar.wait();

    for (size_t thread_nr : {1, 2, 4, 8, 16})
    // for (size_t thread_nr : {1, 2})
    {
        CHECK_LE(thread_nr, kThreadNr);
        // for (size_t block_size : {64ul, 2_MB, 128_MB})
        for (size_t block_size : {64ul})
        {
            for (size_t coro_nr : {16})
            {
                for (bool warm_up : {true, false})
                {
                    auto test_times = 1_M;
                    auto total_test_times = test_times * thread_nr;

                    // bench_patronus_get_rlease_nothing(patronus,
                    //                                   bar,
                    //                                   task_nr,
                    //                                   total_test_times,
                    //                                   block_size,
                    //                                   thread_nr,
                    //                                   coro_nr,
                    //                                   is_master,
                    //                                   warm_up);
                    // bench_patronus_get_rlease_one_bind(patronus,
                    //                                    bar,
                    //                                    task_nr,
                    //                                    total_test_times,
                    //                                    block_size,
                    //                                    thread_nr,
                    //                                    coro_nr,
                    //                                    is_master,
                    //                                    warm_up);
                    // bench_patronus_get_rlease_no_unbind(patronus,
                    //                                     bar,
                    //                                     task_nr,
                    //                                     total_test_times,
                    //                                     block_size,
                    //                                     thread_nr,
                    //                                     coro_nr,
                    //                                     is_master,
                    //                                     warm_up);
                    // bench_patronus_get_rlease_full(patronus,
                    //                                bar,
                    //                                task_nr,
                    //                                total_test_times,
                    //                                block_size,
                    //                                thread_nr,
                    //                                coro_nr,
                    //                                is_master,
                    //                                warm_up);
                    // bench_patronus_get_rlease_full_auto_gc(patronus,
                    //                                        bar,
                    //                                        task_nr,
                    //                                        total_test_times,
                    //                                        block_size,
                    //                                        thread_nr,
                    //                                        coro_nr,
                    //                                        is_master,
                    //                                        warm_up);
                    bench_patronus_alloc(patronus,
                                         bar,
                                         task_nr,
                                         total_test_times,
                                         block_size,
                                         thread_nr,
                                         coro_nr,
                                         is_master,
                                         warm_up);
                    bench_patronus_alloc_no_unbind(patronus,
                                                   bar,
                                                   task_nr,
                                                   total_test_times,
                                                   block_size,
                                                   thread_nr,
                                                   coro_nr,
                                                   is_master,
                                                   warm_up);
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
    config.machine_nr = kMachineNr;
    config.block_class = {2_MB};
    config.block_ratio = {1};

    auto patronus = Patronus::ins(config);

    auto nid = patronus->get_node_id();
    if (nid == kClientNodeId)
    {
        std::vector<std::thread> threads;
        boost::barrier bar(kThreadNr);
        std::atomic<ssize_t> task_nr{0};
        for (size_t i = 0; i + 1 < kThreadNr; ++i)
        {
            // used by all the threads to synchronize their works
            threads.emplace_back([patronus, &bar, &task_nr]() {
                client(patronus, bar, task_nr, false /* is_master */);
            });
        }
        client(patronus, bar, task_nr, true /* is_master */);

        for (auto &t : threads)
        {
            t.join();
        }

        auto tid = patronus->get_thread_id();
        LOG(INFO) << "Client calling finished with tid " << tid;
        patronus->finished();
    }
    else
    {
        std::vector<std::thread> threads;
        for (size_t i = 0; i + 1 < kThreadNr; ++i)
        {
            threads.emplace_back(
                [patronus]() { server(patronus, false /* is_master */); });
        }
        server(patronus, true /* is_master */);

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

    auto filename = binary_to_csv_filename(argv[0], FLAGS_exec_meta);
    df.write<std::ostream, std::string, size_t, double>(std::cout,
                                                        io_format::csv2);
    df.write<std::string, size_t, double>(filename.c_str(), io_format::csv2);

    LOG(INFO) << "Exiting...";
}
