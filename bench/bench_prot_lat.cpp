#include <algorithm>
#include <random>

#include "DSM.h"
#include "Timer.h"
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

void bench_mr(size_t bind_size, size_t test_times)
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
    RdmaContext rdma_ctx;
    CHECK(createContext(&rdma_ctx));

    auto *mm = CHECK_NOTNULL(hugePageAlloc(bind_size));
    auto *nullmm = CHECK_NOTNULL(hugePageAlloc(2_MB));
    // TODO: change into reregister mr
    auto *mr =
        CHECK_NOTNULL(createMemoryRegion((uint64_t) mm, bind_size, &rdma_ctx));
    int access_no = IBV_ACCESS_LOCAL_WRITE;
    int access_rw = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                    IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
    [[maybe_unused]] int accesses[2] = {access_no, access_rw};

    [[maybe_unused]] size_t sizes[2] = {bind_size, 2_MB};
    [[maybe_unused]] void *mms[2] = {mm, nullmm};

    ChronoTimer timer;
    for (size_t i = 0; i < test_times; ++i)
    {
        // (a)
        // CHECK(reregisterMemoryRegionAccess(mr, accesses[i % 2], &rdma_ctx));

        // (b)
        // CHECK(reregisterMemoryRegionTranslate(
        //     mr, mms[i % 2], sizes[i % 2], &rdma_ctx));

        // (c)
        if (mr)
        {
            CHECK(destroyMemoryRegion(mr));
            mr = nullptr;
        }
        else
        {
            mr = CHECK_NOTNULL(
                createMemoryRegion((uint64_t) mm, bind_size, &rdma_ctx));
        }
    }
    auto ns = timer.pin();
    LOG(INFO) << "[bench] takes " << util::pre_ns(ns) << ". "
              << util::pre_ops(test_times, ns, true)
              << ", lat: " << util::pre_ns(1.0 * ns / test_times);
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

    bench_mr(2_GB, 1000);

    LOG(INFO) << "finished. ctrl+C to quit.";
}