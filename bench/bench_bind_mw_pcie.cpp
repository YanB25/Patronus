#include <algorithm>
#include <random>

#include "DSM.h"
#include "Timer.h"
#include "boost/thread/barrier.hpp"
#include "gflags/gflags.h"
#include "patronus/Patronus.h"
#include "util/DataFrameF.h"
#include "util/PerformanceReporter.h"
#include "util/Pre.h"
#include "util/Rand.h"
#include "util/TimeConv.h"
#include "util/monitor.h"

using namespace patronus;

using namespace hmdf;

DEFINE_string(exec_meta, "", "The meta data of this execution");
DEFINE_uint64(thread_nr, 4, "The number of threads in memory nodes");
DEFINE_uint64(mw_batch, 4, "The batch size to post MWs");
DEFINE_uint64(mw_alloc, 8, "The number of MWs to allocate");

void bench_mw_do(DSM::pointer dsm,
                 size_t tid,
                 size_t test_times,
                 size_t bind_size,
                 std::vector<ibv_mw *> &mw_pool,
                 size_t batch_size)
{
    // we are mocking server, so it should be tid.
    size_t dir_id = tid % NR_DIRECTORY;

    ssize_t remain_task = test_times;
    ssize_t ongoing_token = batch_size;

    auto *qp = dsm->get_dir_qp(0, 0, dir_id);
    auto *mr = dsm->get_dir_mr(dir_id);
    auto *cq = dsm->get_dir_cq(dir_id);
    auto srv_buffer = dsm->get_server_internal_dsm_buffer();
    ibv_wc wcs[128];
    size_t mw_index = 0;
    while (remain_task > 0)
    {
        while (ongoing_token > 0)
        {
            ibv_mw_bind mw_bind;
            mw_bind.wr_id = 0;
            mw_bind.bind_info.mr = mr;
            // get a little bit random by + 64 * mw_index
            mw_bind.bind_info.addr = (uint64_t)(srv_buffer.buffer) +
                                     64 * fast_pseudo_rand_int(0, 1_GB / 64);
            // mw_bind.bind_info.addr = (uint64_t)(srv_buffer.buffer);
            mw_bind.bind_info.length = bind_size;
            mw_bind.bind_info.mw_access_flags = 31;  // magic, all access perm
            mw_bind.send_flags |= IBV_SEND_SIGNALED;

            auto *mw = mw_pool[(mw_index++) % mw_pool.size()];
            int ret = ibv_bind_mw(qp, mw, &mw_bind);
            PLOG_IF(FATAL, ret) << "Failed to call ibv_bind_mw.";
            ongoing_token--;
            remain_task--;
        }
        int polled = ibv_poll_cq(cq, 128, wcs);
        PLOG_IF(FATAL, polled < 0) << "Failed to call ibv_poll_cq";
        ongoing_token += polled;
    }
}

void do_bench_mw(DSM::pointer dsm,
                 size_t test_times,
                 size_t bind_size,
                 size_t batch_size,
                 std::vector<ibv_mw *> &mw_pool)
{
    auto tid = dsm->get_thread_id();
    auto dir_id = tid;

    ssize_t remain_task = test_times;
    // ssize_t ongoing_token = batch_size;
    ssize_t ongoing_token = batch_size;

    auto this_nid = dsm->get_node_id();
    size_t next_nid = (this_nid + 1) % dsm->getClusterSize();

    auto *qp = dsm->get_dir_qp(next_nid, tid, dir_id);
    auto *mr = dsm->get_dir_mr(dir_id);
    auto *cq = dsm->get_dir_cq(dir_id);
    auto srv_buffer = dsm->get_server_internal_dsm_buffer();
    ibv_wc wcs[128];
    size_t mw_index = 0;
    while (remain_task > 0)
    {
        while (ongoing_token > 0)
        {
            ibv_mw_bind mw_bind;
            memset(&mw_bind, 0, sizeof(mw_bind));
            mw_bind.wr_id = 0;
            mw_bind.bind_info.mr = mr;
            // get a little bit random by + 64 * mw_index
            // mw_bind.bind_info.addr = (uint64_t)(srv_buffer.buffer) +
            //                          64 * fast_pseudo_rand_int(0, 1_GB / 64);
            mw_bind.bind_info.addr = (uint64_t)(srv_buffer.buffer);
            // std::ignore = fast_pseudo_rand_int(0, 2_GB / 64);
            // mw_bind.bind_info.addr = (uint64_t)(srv_buffer.buffer);
            mw_bind.bind_info.length = bind_size;
            mw_bind.bind_info.mw_access_flags = 31;  // magic, all access perm
            mw_bind.send_flags |= IBV_SEND_SIGNALED;

            // explain:
            // the first @constantly_active_pool.size() MW
            // will always be in the pool, so that they are
            // always active.
            ibv_mw *mw = mw_pool[(mw_index++) % mw_pool.size()];
            int ret = ibv_bind_mw(qp, mw, &mw_bind);
            PLOG_IF(FATAL, ret)
                << "Failed to call ibv_bind_mw. qp: " << (void *) qp
                << ", mw: " << (void *) mw << ", mr: " << (void *) mr
                << ", addr: " << (void *) mw_bind.bind_info.addr
                << ", length: " << bind_size;
            ongoing_token--;
            remain_task--;
        }
        int polled = ibv_poll_cq(cq, 128, wcs);
        PLOG_IF(FATAL, polled < 0) << "Failed to call ibv_poll_cq";
        ongoing_token += polled;
    }
}

void enter_bench_mw(DSM::pointer dsm,
                    boost::barrier &bar,
                    size_t bind_size,
                    size_t test_times)
{
    size_t thread_nr = FLAGS_thread_nr;

    auto tid = dsm->get_thread_id();
    bool need_to_run = (size_t) tid < thread_nr;

    CHECK_GE(FLAGS_mw_alloc, FLAGS_mw_batch) << "Recommend turn up kAllocMwNr.";

    std::vector<ibv_mw *> mw_pool;
    if (need_to_run)
    {
        auto dir_id = tid;
        for (size_t i = 0; i < FLAGS_mw_alloc; ++i)
        {
            mw_pool.push_back(dsm->alloc_mw(dir_id));
        }
    }

    bar.wait();
    ChronoTimer timer;
    if (need_to_run)
    {
        do_bench_mw(dsm, test_times, bind_size, FLAGS_mw_batch, mw_pool);
    }
    bar.wait();
    auto ns = timer.pin();

    size_t total_test_times = test_times * thread_nr;
    LOG_IF(INFO, tid == 0) << "[bench] bind MW with threads " << thread_nr
                           << ", " << util::pre_ops(total_test_times, ns, true)
                           << ", per_thread: "
                           << util::pre_ops(test_times, ns, true);

    for (ibv_mw *mw : mw_pool)
    {
        dsm->free_mw(mw);
    }
    mw_pool.clear();

    bar.wait();
}

void bench_mw_launcher(size_t bind_size, size_t test_times)
{
    DSMConfig config;
    config.machineNR = ::config::kMachineNr;
    auto dsm = DSM::getInstance(config);
    dsm->registerThread();
    bool succ = dsm->registerThread();
    LOG(INFO) << "I am master thread. register: " << succ;

    std::vector<std::thread> threads;
    boost::barrier bar(kMaxAppThread);

    for (size_t i = 1; i < kMaxAppThread; ++i)
    {
        threads.emplace_back([dsm, &bar, bind_size, test_times]() {
            dsm->registerThread();
            enter_bench_mw(dsm, bar, bind_size, test_times);
        });
    }
    enter_bench_mw(dsm, bar, bind_size, test_times);

    for (auto &thread : threads)
    {
        thread.join();
    }
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    bench_mw_launcher(64, 10_M);

    LOG(INFO) << "finished. ctrl+C to quit.";
}