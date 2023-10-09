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

std::vector<std::string> col_idx;
std::vector<size_t> col_x_test_time;
std::vector<size_t> col_x_bind_size;
std::vector<uint64_t> col_lat_min;
std::vector<uint64_t> col_lat_p5;
std::vector<uint64_t> col_lat_p9;
std::vector<uint64_t> col_lat_p99;

constexpr static size_t kDirID = 0;

void bench_mw_cli(Patronus::pointer p, size_t test_time, size_t bind_size)
{
    // Sequence<uint32_t> rkey_seq;

    auto dsm = p->get_dsm();
    auto *mw = dsm->alloc_mw(kDirID);
    auto srv_buffer = dsm->get_server_internal_dsm_buffer();
    uint64_t min = util::time::to_ns(0ns);
    uint64_t max = util::time::to_ns(5ms);
    uint64_t rng = util::time::to_ns(1ns);
    OnePassBucketMonitor<uint64_t> lat_m(min, max, rng);
    for (size_t i = 0; i < test_time; ++i)
    {
        ChronoTimer timer;
        dsm->bind_memory_region_sync(
            mw, 0, 0, srv_buffer.buffer, bind_size, kDirID, 0, nullptr);
        auto ns = timer.pin();
        lat_m.collect(ns);
        // rkey_seq.push_back(mw->rkey);
    }
    dsm->free_mw(mw);
    LOG(INFO) << lat_m;

    CHECK_EQ(lat_m.overflow_nr(), 0) << lat_m;

    col_idx.push_back("mw");
    col_x_test_time.push_back(test_time);
    col_x_bind_size.push_back(bind_size);
    col_lat_min.push_back(lat_m.min());
    col_lat_p5.push_back(lat_m.percentile(0.5));
    col_lat_p9.push_back(lat_m.percentile(0.9));
    col_lat_p99.push_back(lat_m.percentile(0.99));

    // LOG(INFO) << "rkeys are: " << util::pre_vec(rkey_seq.to_vector(), 1024);
}

void bench_mr_cli(Patronus::pointer p, size_t test_time, size_t bind_size)
{
    // auto *buffer = CHECK_NOTNULL(hugePageAlloc(128_MB));
    auto dsm = p->get_dsm();

    auto *buffer = (char *) dsm->get_base_addr();
    CHECK_LE(bind_size, dsm->get_base_size());

    uint64_t min = util::time::to_ns(0ns);
    uint64_t max = util::time::to_ns(2s);
    uint64_t rng = util::time::to_ns(1us);
    OnePassBucketMonitor<uint64_t> lat_m(min, max, rng);

    auto *rdma_ctx = dsm->get_rdma_context(kDirID);
    for (size_t i = 0; i < test_time; ++i)
    {
        ChronoTimer timer;
        auto *mr = CHECK_NOTNULL(
            createMemoryRegion((uint64_t) buffer, bind_size, rdma_ctx));
        auto ns = timer.pin();
        lat_m.collect(ns);

        CHECK(destroyMemoryRegion(mr));
    }
    CHECK_EQ(lat_m.overflow_nr(), 0) << lat_m;
    LOG(INFO) << lat_m;

    col_idx.push_back("mr");
    col_x_test_time.push_back(test_time);
    col_x_bind_size.push_back(bind_size);
    col_lat_min.push_back(lat_m.min());
    col_lat_p5.push_back(lat_m.percentile(0.5));
    col_lat_p9.push_back(lat_m.percentile(0.9));
    col_lat_p99.push_back(lat_m.percentile(0.99));

    // CHECK(hugePageFree(buffer, 128_MB));
}

void bench_mw(Patronus::pointer p,
              size_t test_time,
              size_t bind_size,
              bool is_client)
{
    if (is_client)
    {
        bench_mw_cli(p, test_time, bind_size);
    }
    else
    {
    }
}
void bench_qp_flag(DSM::pointer dsm, size_t test_time)
{
    auto *qp = dsm->get_dir_qp(0, 0, kDirID);

    int qp_access_flags = IBV_ACCESS_REMOTE_WRITE;
    ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    for (size_t i = 0; i < test_time; ++i)
    {
        qp_access_flags = IBV_ACCESS_REMOTE_WRITE ? IBV_ACCESS_REMOTE_READ
                                                  : IBV_ACCESS_REMOTE_WRITE;
        attr.qp_access_flags = qp_access_flags;
        auto ret = ibv_modify_qp(qp, &attr, IBV_QP_ACCESS_FLAGS);
        PLOG_IF(FATAL, ret) << "Failed to ibv_modify_qp";
    }
}

void bench_qp_state(Patronus::pointer p, size_t test_time)
{
    auto dsm = p->get_dsm();

    uint64_t min = util::time::to_ns(0ns);
    uint64_t max = util::time::to_ns(10ms);
    uint64_t rng = util::time::to_ns(1ns);
    OnePassBucketMonitor<uint64_t> lat_m(min, max, rng);

    for (size_t i = 0; i < test_time; ++i)
    {
        ChronoTimer timer;
        CHECK(dsm->recoverDirQP(0, 0, kDirID));
        auto ns = timer.pin();
        lat_m.collect(ns);
    }

    LOG(INFO) << lat_m;
    CHECK_EQ(lat_m.overflow_nr(), 0) << lat_m;

    col_idx.push_back("QP(state)");
    col_x_test_time.push_back(test_time);
    col_x_bind_size.push_back(0);
    col_lat_min.push_back(lat_m.min());
    col_lat_p5.push_back(lat_m.percentile(0.5));
    col_lat_p9.push_back(lat_m.percentile(0.9));
    col_lat_p99.push_back(lat_m.percentile(0.99));
}

/**
 * @brief use the rereg API of MR
 */
void bench_rereg_mr(size_t bind_size, size_t test_times, bool use_huge_page)
{
    bindCore(1);

    // 1) allocate the memory
    void *mm = nullptr;
    if (use_huge_page)
    {
        mm = CHECK_NOTNULL(hugePageAlloc(bind_size));
    }
    else
    {
        mm = malloc(bind_size);
    }

    // 2) pre-fault
    char *mm_ = (char *) mm;
    if (use_huge_page)
    {
        for (size_t i = 0; i < bind_size; i += 2_MB)
        {
            mm_[i] = 1;
        }
    }
    else
    {
        for (size_t i = 0; i < bind_size; i += 4_KB)
        {
            mm_[i] = 1;
        }
    }

    // 3) set up RDMA logic
    RdmaContext rdma_ctx;
    CHECK(createContext(&rdma_ctx));

    auto *mr =
        CHECK_NOTNULL(createMemoryRegion((uint64_t) mm, bind_size, &rdma_ctx));
    int access_no = IBV_ACCESS_LOCAL_WRITE;
    int access_rw = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                    IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
    int accesses[2] = {access_no, access_rw};

    ChronoTimer timer;
    for (size_t i = 0; i < test_times; ++i)
    {
        CHECK(reregisterMemoryRegionAccess(mr, accesses[i % 2], &rdma_ctx));
    }
    auto ns = timer.pin();
    LOG(INFO) << "[bench] reregister: " << util::pre_ns(ns) << ". "
              << util::pre_ops(test_times, ns, true);
}

void pre_fault(char *mm, size_t size, bool use_huge_page)
{
    if (use_huge_page)
    {
        for (size_t i = 0; i < size; i += 2_MB)
        {
            mm[i] = 1;
        }
    }
    else
    {
        for (size_t i = 0; i < size; i += 4_KB)
        {
            mm[i] = 1;
        }
    }
}
/**
 * @brief use the dereg API
 */
void bench_dereg_mr(size_t bind_size, size_t test_times, bool use_huge_page)
{
    /**
     * 2GB 1000, change permission (with KEEP_VALID flags, rereg failed.)
     * [bench] takes 16.3936 s. 60.9995 ops[1000 in 16.3936 s], lat: 16.3936 ms
     *
     * 2GB 1000, change translate
     * [bench] takes 20.1896 s. 49.5304 ops[1000 in 20.1896 s], lat: 20.1896 ms
     *
     * 2GB 1000, register + deregister
     * [bench] takes 8.67359 s. 115.293 ops[1000 in 8.67359 s], lat: 8.67359 ms
     */
    bindCore(1);

    // 1) allocate the memory
    void *mm = nullptr;
    if (use_huge_page)
    {
        mm = CHECK_NOTNULL(hugePageAlloc(bind_size));
    }
    else
    {
        mm = malloc(bind_size);
    }

    // 2) pre-fault
    pre_fault((char *) mm, bind_size, use_huge_page);

    // 3) set up RDMA logic
    RdmaContext rdma_ctx;
    CHECK(createContext(&rdma_ctx));

    auto *mr =
        CHECK_NOTNULL(createMemoryRegion((uint64_t) mm, bind_size, &rdma_ctx));

    ChronoTimer timer;
    for (size_t i = 0; i < test_times; ++i)
    {
        CHECK(destroyMemoryRegion(mr));
        mr = CHECK_NOTNULL(
            createMemoryRegion((uint64_t) mm, bind_size, &rdma_ctx));
    }
    auto ns = timer.pin();
    LOG(INFO) << "[bench] deregister + register: " << util::pre_ns(ns) << ". "
              << util::pre_ops(test_times, ns, true);
}

void bench_mr_multiple_thread(size_t bind_size,
                              size_t test_times,
                              size_t thread_nr)
{
    std::vector<std::thread> threads;
    for (size_t i = 0; i < thread_nr; ++i)
    {
        threads.emplace_back([bind_size, test_times, tid = i]() {
            // TODO: change into reregister mr
            bindCore(tid + 1);
            auto *mm = CHECK_NOTNULL(hugePageAlloc(bind_size));
            RdmaContext rdma_ctx;
            CHECK(createContext(&rdma_ctx));
            auto *mr = CHECK_NOTNULL(
                createMemoryRegion((uint64_t) mm, bind_size, &rdma_ctx));
            int access_ro = IBV_ACCESS_REMOTE_READ;
            int access_rw = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                            IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
            int accesses[2] = {access_ro, access_rw};
            for (size_t i = 0; i < test_times; ++i)
            {
                CHECK(reregisterMemoryRegionAccess(
                    mr, accesses[i % 2], &rdma_ctx));
            }
        });
    }

    for (auto &t : threads)
    {
        t.join();
    }
}

void benchmark()
{
    RdmaContext rdma_ctx;
    CHECK(createContext(&rdma_ctx));
    ChronoTimer timer;
    // auto test_times = 100;
    // bench_mr(rdma_ctx, 2_GB, test_times);
    // bench_mw(p);

    // auto test_times = 20000;
    // bench_qp_flag(dsm, test_times);

    // auto ns = timer.pin();
    // LOG(INFO) << "[system] benchmark takes " << util::pre_ns(ns) << ", "
    //           << util::pre_ns(1.0 * ns / test_times) << " per op.";

    // auto test_times = 800;
    // size_t thread_nr = 16;
    // bench_mr_multiple_thread(1_GB, test_times, thread_nr);
    // auto ns = timer.pin();
    // LOG(INFO) << "[bench] takes " << util::pre_ns(ns) << " with " <<
    // thread_nr
    //           << " threads. total: "
    //           << util::pre_ops(test_times * thread_nr, ns, true)
    //           << " per-thread: " << util::pre_ops(test_times, ns, true);
}

void bench_alloc_mw()
{
    DSMConfig config;
    config.machineNR = ::config::kMachineNr;

    auto dsm = DSM::getInstance(config);

    ChronoTimer timer;
    size_t test_times = 10_K;
    for (size_t i = 0; i < test_times; ++i)
    {
        dsm->alloc_mw(0);
    }
    auto ns = timer.pin();
    LOG(INFO) << "[bench] dsm->alloc_mw: "
              << util::pre_ops(test_times, ns, true);
}

void bench_qp_query_lat()
{
    DSMConfig config;
    config.machineNR = ::config::kMachineNr;

    auto dsm = DSM::getInstance(config);

    ChronoTimer timer;
    size_t test_times = 100_K;
    for (size_t i = 0; i < test_times; ++i)
    {
        auto client_nid = ::config::get_client_nids().front();
        auto tid = 0;
        auto dir = 0;
        std::ignore = rdmaQueryQueuePair(dsm->get_dir_qp(client_nid, tid, dir));
    }
    auto ns = timer.pin();
    LOG(INFO) << "[bench] rdmaQueryQueuePair: "
              << util::pre_ops(test_times, ns, true);
}

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
            // mw_bind.bind_info.addr =
            //     (uint64_t)(srv_buffer.buffer) +
            //     64 * fast_pseudo_rand_int(
            //              0, NR_DIRECTORY * 32 * kBatchSize);
            mw_bind.bind_info.addr = (uint64_t)(srv_buffer.buffer);
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
    constexpr static ssize_t kBatchSize = 4;
    constexpr static ssize_t kAllocMwNr = 1024;
    // constexpr static ssize_t kAllocMwNr = 1024;

    // for (size_t thread_nr : {1, 4, 8, 16})
    for (size_t thread_nr : {16})
    {
        auto tid = dsm->get_thread_id();
        bool need_to_run = (size_t) tid < thread_nr;
        // LOG_IF(INFO, need_to_run) << "[debug] tid " << tid << ", need to
        // run";

        static_assert(kAllocMwNr >= kBatchSize,
                      "Recommend turn up kAllocMwNr.");

        std::vector<ibv_mw *> mw_pool;
        if (need_to_run)
        {
            auto dir_id = tid;
            for (size_t i = 0; i < kAllocMwNr; ++i)
            {
                mw_pool.push_back(dsm->alloc_mw(dir_id));
            }
        }

        bar.wait();
        ChronoTimer timer;
        if (need_to_run)
        {
            do_bench_mw(dsm, test_times, bind_size, kBatchSize, mw_pool);
        }
        bar.wait();
        auto ns = timer.pin();

        size_t total_test_times = test_times * thread_nr;
        LOG_IF(INFO, tid == 0)
            << "[bench] bind MW with threads " << thread_nr << ", "
            << util::pre_ops(total_test_times, ns, true)
            << ", per_thread: " << util::pre_ops(test_times, ns, true);

        for (ibv_mw *mw : mw_pool)
        {
            dsm->free_mw(mw);
        }
        mw_pool.clear();

        bar.wait();
    }
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

    // DSMConfig config;
    // config.machineNR = ::config::kMachineNr;

    // auto dsm = DSM::getInstance(config);

    LOG(INFO) << "[bench] this benchmark is used for overhead breakdown. It "
                 "evaluates the overhead of MR and qp modification. Use linux "
                 "perf to view the overhead.";
    LOG(INFO) << "Enter benchmark";
    // benchmark();
    // bench_alloc_mw();
    // bench_qp_query_lat();

    // clang-format off
    /**
     * rereg: 137ms per op [hugepage, 16GB, 100 op] (16.7 us per hugepage)
     *  - rereg between "remote R+W", "local only"
     *  - rereg between "remote R+W", "remote RO"
     * The above two cases have the same latency. We conclude that:
     * MR reregister consistently requires page unpinning & pinning.
     * rereg: 12.4s per op [page, 16GB, 10 op] (3 us per page, i.e., 1536 us per hugepages) 
     * dereg: 136ms per op [hugepage, 16GB, 100op]
     */
    // clang-format on
    // bench_rereg_mr(16_GB, 100, true);
    // bench_rereg_mr(1, 200000, true);
    // bench_rereg_mr(4_KB, 2000, true);
    // bench_rereg_mr(2_MB, 2000, true);
    // bench_rereg_mr(4_MB, 2000, true);
    // bench_dereg_mr(16_GB, 100, true);

    bench_mw_launcher(64, 10_M);

    LOG(INFO) << "finished. ctrl+C to quit.";
}