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
void bench_mr(Patronus::pointer p,
              size_t test_time,
              size_t bind_size,
              bool is_client)
{
    if (is_client)
    {
        bench_mr_cli(p, test_time, bind_size);
    }
    else
    {
    }
}
void bench_qp_flag(Patronus::pointer p, size_t test_time)
{
    auto dsm = p->get_dsm();
    auto *qp = dsm->get_dir_qp(0, 0, kDirID);

    uint64_t min = util::time::to_ns(0ns);
    uint64_t max = util::time::to_ns(4ms);
    uint64_t rng = util::time::to_ns(1ns);
    OnePassBucketMonitor<uint64_t> lat_m(min, max, rng);

    int qp_access_flags = IBV_ACCESS_REMOTE_WRITE;
    ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    for (size_t i = 0; i < test_time; ++i)
    {
        qp_access_flags = IBV_ACCESS_REMOTE_WRITE ? IBV_ACCESS_REMOTE_READ
                                                  : IBV_ACCESS_REMOTE_WRITE;
        attr.qp_access_flags = qp_access_flags;
        ChronoTimer timer;
        auto ret = ibv_modify_qp(qp, &attr, IBV_QP_ACCESS_FLAGS);
        auto ns = timer.pin();
        lat_m.collect(ns);
        PLOG_IF(FATAL, ret) << "Failed to ibv_modify_qp";
    }
    CHECK_EQ(lat_m.overflow_nr(), 0) << lat_m;
    LOG(INFO) << lat_m;

    col_idx.push_back("QP(flag)");
    col_x_test_time.push_back(test_time);
    col_x_bind_size.push_back(0);
    col_lat_min.push_back(lat_m.min());
    col_lat_p5.push_back(lat_m.percentile(0.5));
    col_lat_p9.push_back(lat_m.percentile(0.9));
    col_lat_p99.push_back(lat_m.percentile(0.99));
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

void benchmark(Patronus::pointer p, bool is_client)
{
    auto dsm = p->get_dsm();
    for (size_t size :
         {64_B, 4_KB, 32_KB, 256_KB, 2_MB, 16_MB, 64_MB, 512_MB, 2_GB})
    {
        size_t test_time = 1_K;

        bench_mw(p, test_time, size, is_client);
        LOG(INFO) << "[bench] bench_mr size: " << size;
        bench_mr(p, test_time, size, is_client);
    }
    auto mr_max_size = dsm->get_base_size();
    for (size_t size = 4_GB; size <= mr_max_size; size *= 2)
    {
        LOG(INFO) << "[bench] bench_mr size: " << size;
        bench_mr(p, 10, size, is_client);
    }

    LOG(INFO) << "[bench] bench_qp_flag";
    bench_qp_flag(p, 1_K);
    LOG(INFO) << "[bench] bench_qp_state";
    bench_qp_state(p, 1_K);
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    rdmaQueryDevice();

    PatronusConfig config;
    config.machine_nr = ::config::kMachineNr;
    config.block_class = {2_MB};
    config.block_ratio = {1};

    auto patronus = Patronus::ins(config);

    auto nid = patronus->get_node_id();

    if (::config::is_client(nid))
    {
        patronus->registerClientThread();
        patronus->keeper_barrier("begin", 100ms);
        benchmark(patronus, true);
    }
    else
    {
        patronus->registerServerThread();
        patronus->keeper_barrier("begin", 100ms);
        benchmark(patronus, false);
    }

    StrDataFrame df;
    df.load_index(std::move(col_idx));
    df.load_column<size_t>("x_bind_size", std::move(col_x_bind_size));
    df.load_column<size_t>("x_test_nr", std::move(col_x_test_time));
    df.load_column<uint64_t>("lat_min(ns)", std::move(col_lat_min));
    df.load_column<uint64_t>("lat_p5(ns)", std::move(col_lat_p5));
    df.load_column<uint64_t>("lat_p9(ns)", std::move(col_lat_p9));
    df.load_column<uint64_t>("lat_p99(ns)", std::move(col_lat_p99));

    auto filename = binary_to_csv_filename(argv[0], FLAGS_exec_meta);
    df.write<std::ostream, std::string, size_t, double>(std::cout,
                                                        io_format::csv2);
    df.write<std::string, size_t, double>(filename.c_str(), io_format::csv2);

    patronus->keeper_barrier("finished", 100ms);
    LOG(INFO) << "finished. ctrl+C to quit.";
}