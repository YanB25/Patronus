#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdlib>
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

using namespace util::literals;
using namespace patronus;
using namespace std::chrono_literals;

constexpr static size_t kClientThreadNr = kMaxAppThread;
constexpr static size_t kServerThreadNr = NR_DIRECTORY;

// constexpr static size_t kTestTimePerThread = 300_K;
// constexpr static size_t kTestTimePerThread = 100;

std::vector<std::string> col_idx;
std::vector<size_t> col_x_use_mn;
std::vector<size_t> col_x_link_entry_nr;
std::vector<size_t> col_x_link_entry_size;
std::vector<size_t> col_x_thread_nr;
std::vector<size_t> col_x_coro_nr;
std::vector<size_t> col_test_nr;
std::vector<size_t> col_test_ns;

std::vector<std::string> col_lat_idx;
std::vector<uint64_t> col_lat_patronus;
std::vector<uint64_t> col_lat_MR;
std::vector<uint64_t> col_lat_unprotected;

using namespace hmdf;

DEFINE_string(exec_meta, "", "The meta data of this execution");

constexpr static size_t kCoroCnt = 32;  // max
struct CoroCommunication
{
    CoroCall workers[kCoroCnt];
    CoroCall master;
    ssize_t thread_remain_task;
    std::vector<bool> finish_all;
};

struct BenchConfig
{
    size_t thread_nr;
    size_t coro_nr;
    size_t link_entry_nr;
    size_t link_entry_size;
    size_t task_nr;
    bool use_mn;
    std::string name;
    bool report;

    static BenchConfig get_conf(const std::string &name,
                                size_t thread_nr,
                                size_t coro_nr,
                                size_t link_entry_nr,
                                size_t link_entry_size,
                                bool use_mn,
                                size_t task_nr)
    {
        BenchConfig ret;
        ret.name = name;
        ret.thread_nr = thread_nr;
        ret.coro_nr = coro_nr;
        ret.link_entry_nr = link_entry_nr;
        ret.link_entry_size = link_entry_size;
        ret.task_nr = task_nr;
        ret.use_mn = use_mn;
        ret.report = true;
        return ret;
    }
};

void reg_result(size_t total_ns, const BenchConfig &conf)
{
    col_idx.push_back(conf.name);
    col_x_use_mn.push_back(conf.use_mn);
    col_x_link_entry_nr.push_back(conf.link_entry_nr);
    col_x_link_entry_size.push_back(conf.link_entry_size);
    col_x_thread_nr.push_back(conf.thread_nr);
    col_x_coro_nr.push_back(conf.coro_nr);
    col_test_nr.push_back(conf.task_nr);
    col_test_ns.push_back(total_ns);
}

std::unordered_map<std::string, std::vector<uint64_t>> lat_data_;
void reg_latency(const std::string &name, OnePassBucketMonitor<uint64_t> &m)
{
    if (unlikely(col_lat_idx.empty()))
    {
        col_lat_idx.push_back("lat_min");
        col_lat_idx.push_back("lat_p5");
        col_lat_idx.push_back("lat_p9");
        col_lat_idx.push_back("lat_p99");
    }

    lat_data_[name].push_back(m.min());
    lat_data_[name].push_back(m.percentile(0.5));
    lat_data_[name].push_back(m.percentile(0.9));
    lat_data_[name].push_back(m.percentile(0.99));
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

    DLOG_IF(WARNING, test_times % 1000 != 0)
        << "test_times % 1000 != 0. Will introduce 1/1000 performance error "
           "per thread";

    ssize_t task_per_sync = test_times / 1000;
    DLOG_IF(WARNING, task_per_sync <= (ssize_t) coro_nr)
        << "test_times < coro_nr. will result in variance in reported "
           "performance";

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

void bench_alloc_thread_coro_worker(Patronus::pointer p,
                                    size_t coro_id,
                                    CoroYield &yield,
                                    CoroCommunication &coro_comm,
                                    OnePassBucketMonitor<uint64_t> &lat_m,
                                    bool is_master,
                                    const BenchConfig &conf)
{
    [[maybe_unused]] auto tid = p->get_thread_id();
    [[maybe_unused]] auto dir_id = tid % kServerThreadNr;
    [[maybe_unused]] auto server_nid = ::config::get_server_nids().front();
    std::ignore = conf;
    std::ignore = is_master;

    CoroContext ctx(tid, &yield, &coro_comm.master, coro_id);

    size_t fail_nr = 0;
    size_t succ_nr = 0;

    ChronoTimer timer;
    ChronoTimer op_timer;

    auto sz = conf.link_entry_size;
    auto rdma_buf = p->get_rdma_buffer(sz);
    size_t actual_test_nr = 0;
    while (coro_comm.thread_remain_task > 0)
    {
        // VLOG(4) << "[coro] tid " << tid << " get_rlease. coro: " << ctx;
        actual_test_nr++;
        if (is_master && coro_id == 0)
        {
            op_timer.pin();
        }

        if (conf.use_mn)
        {
            // call MN to help us to copy
            p->remote_memcpy(conf.link_entry_nr,
                             conf.link_entry_size,
                             server_nid,
                             dir_id,
                             &ctx)
                .expect(RC::kOk);
        }
        else
        {
            for (size_t i = 0; i < conf.link_entry_nr; ++i)
            {
                // read
                {
                    uint64_t remote_addr =
                        sz * fast_pseudo_rand_int(0, 8_GB / sz);
                    auto ac_flag = (flag_t) AcquireRequestFlag::kNoRpc |
                                   (flag_t) AcquireRequestFlag::kNoGc;
                    auto lease = p->get_rlease(server_nid,
                                               dir_id,
                                               GlobalAddress(0, remote_addr),
                                               0 /* alloc_hint */,
                                               sz,
                                               0ns,
                                               ac_flag,
                                               &ctx);
                    CHECK(lease.success());
                    auto rw_flag = (flag_t) RWFlag::kUseUniversalRkey;
                    p->read(lease,
                            rdma_buf.buffer,
                            sz,
                            0 /* offset */,
                            rw_flag,
                            &ctx)
                        .expect(RC::kOk);

                    auto rel_flag = (flag_t) LeaseModifyFlag::kNoRpc;
                    p->relinquish(lease, 0 /* alloc_hint */, rel_flag, &ctx);
                }

                // then write
                {
                    uint64_t remote_addr =
                        sz * fast_pseudo_rand_int(0, 8_GB / sz);
                    auto ac_flag = (flag_t) AcquireRequestFlag::kNoRpc |
                                   (flag_t) AcquireRequestFlag::kNoGc;
                    auto lease = p->get_wlease(server_nid,
                                               dir_id,
                                               GlobalAddress(0, remote_addr),
                                               0 /* alloc_hint */,
                                               sz,
                                               0ns,
                                               ac_flag,
                                               &ctx);
                    auto rw_flag = (flag_t) RWFlag::kUseUniversalRkey;
                    p->write(lease,
                             rdma_buf.buffer,
                             sz,
                             0 /* offset */,
                             rw_flag,
                             &ctx)
                        .expect(RC::kOk);
                    auto rel_flag = (flag_t) LeaseModifyFlag::kNoRpc;
                    p->relinquish(lease, 0 /* hint */, rel_flag);
                }
            }
        }

        if (is_master && coro_id == 0)
        {
            auto ns = op_timer.pin();
            lat_m.collect(ns);
        }
        coro_comm.thread_remain_task--;
    }
    auto total_ns = timer.pin();

    p->put_rdma_buffer(std::move(rdma_buf));

    coro_comm.finish_all[coro_id] = true;

    VLOG(1) << "[bench] tid " << tid << " got " << fail_nr
            << " failed lease. succ lease: " << succ_nr << " within "
            << total_ns << " ns. coro: " << ctx;
    LOG_IF(INFO, is_master && coro_id == 0)
        << "[bench] self run " << actual_test_nr << ".";
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

void bench_alloc_thread_coro(Patronus::pointer patronus,
                             OnePassBucketMonitor<uint64_t> &lat_m,
                             bool is_master,
                             std::atomic<ssize_t> &work_nr,
                             const BenchConfig &conf)
{
    auto coro_nr = conf.coro_nr;
    auto test_times = conf.task_nr;

    auto tid = patronus->get_thread_id();
    auto dir_id = tid % kServerThreadNr;

    VLOG(1) << "[coro] client tid " << tid << " bind to core " << tid
            << ", using dir_id " << dir_id;
    CoroCommunication coro_comm;
    coro_comm.finish_all.resize(coro_nr);

    for (size_t i = 0; i < coro_nr; ++i)
    {
        coro_comm.workers[i] = CoroCall([patronus,
                                         &lat_m,
                                         coro_id = i,
                                         &coro_comm,
                                         is_master,
                                         &conf](CoroYield &yield) {
            bench_alloc_thread_coro_worker(
                patronus, coro_id, yield, coro_comm, lat_m, is_master, conf);
        });
    }

    coro_comm.master =
        CoroCall([patronus, test_times, &work_nr, &coro_comm, coro_nr](
                     CoroYield &yield) {
            bench_alloc_thread_coro_master(
                patronus, yield, test_times, work_nr, coro_comm, coro_nr);
        });

    coro_comm.master();
    return;
}

void bench_template(const std::string &name,
                    Patronus::pointer patronus,
                    boost::barrier &bar,
                    std::atomic<ssize_t> &work_nr,
                    bool is_master,
                    const BenchConfig &conf)

{
    auto test_times = conf.task_nr;
    auto thread_nr = conf.thread_nr;
    auto link_entry_nr = conf.link_entry_nr;
    auto link_entry_size = conf.link_entry_size;
    auto coro_nr = conf.coro_nr;
    bool report = conf.report;

    if (is_master)
    {
        work_nr = test_times;
        // LOG(INFO) << "debug !! work_nr: " << work_nr;
    }

    auto min = util::time::to_ns(0ns);
    auto max = util::time::to_ns(10ms);
    auto range = util::time::to_ns(1us);
    OnePassBucketMonitor lat_m(min, max, range);

    bar.wait();
    ChronoTimer timer;

    auto tid = patronus->get_thread_id();

    if (tid < thread_nr)
    {
        bench_alloc_thread_coro(patronus, lat_m, is_master, work_nr, conf);
    }

    bar.wait();
    auto total_ns = timer.pin();
    if (is_master && report)
    {
        reg_result(total_ns, conf);
        reg_latency(name, lat_m);
    }

    LOG_IF(INFO, is_master)
        << "[bench] BENCH: " << name << ", thread_nr: " << thread_nr
        << ", coro_nr: " << coro_nr << ", entry_nr: " << link_entry_nr
        << ", entry_size: " << link_entry_size << ", use_mn: " << conf.use_mn
        << ". Takes: " << util::pre_ns(total_ns) << " with op " << test_times;
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

    bench_template(conf.name, patronus, bar, shared_task_nr, is_master, conf);
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
               [[maybe_unused]] bool is_client,
               [[maybe_unused]] bool is_master)
{
    bar.wait();

    [[maybe_unused]] size_t key = 0;
    std::ignore = patronus;

    size_t thread_nr = 32;
    CHECK_LE(thread_nr, kMaxAppThread);
    // std::vector<BenchConfig> configs;

    for (bool use_mn : {true, false})
    {
        // for (size_t coro_nr : {1, 16})
        // for (size_t coro_nr : {1, 8})
        for (size_t coro_nr : {1, 8})
        {
            for (size_t linked_list_length : {1_K, 10_K, 50_K})
            {
                // for (size_t entry_size : {8_B, 4_KB, 2_MB})
                for (size_t entry_size : {8_B})
                // for (size_t entry_size : {2_MB})
                {
                    // auto test_times = kTestTimePerThread * thread_nr / 1000;
                    auto test_times = 1_M / linked_list_length;
                    auto config = BenchConfig::get_conf("link",
                                                        thread_nr,
                                                        coro_nr,
                                                        linked_list_length,
                                                        entry_size,
                                                        use_mn,
                                                        test_times);
                    run_benchmark(
                        patronus, {config}, bar, is_client, is_master, key);
                    key++;
                }
            }
        }
    }

    // for (bool use_mn : {true, true, true, false, false, false})
    // {
    //     for (size_t coro_nr : {1})
    //     {
    //         for (size_t linked_list_length : {1})
    //         {
    //             for (size_t entry_size : {128_MB})
    //             {
    //                 auto test_times =
    //                     2 * kTestTimePerThread * thread_nr / 500000;
    //                 auto config = BenchConfig::get_conf("link",
    //                                                     thread_nr,
    //                                                     coro_nr,
    //                                                     linked_list_length,
    //                                                     entry_size,
    //                                                     use_mn,
    //                                                     test_times);
    //                 run_benchmark(
    //                     patronus, {config}, bar, is_client, is_master, key);
    //                 key++;
    //             }
    //         }
    //     }
    // }

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
    config.client_rdma_buffer = {
        {8_B, config::patronus::kClientRdmaBufferSize, 2_MB},
        {0.01, 0.49, 0.5}};
    // config.client_rdma_buffer = {{128_MB}, {1}};

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

    {
        StrDataFrame df;
        df.load_index(std::move(col_idx));
        df.load_column<size_t>("x_thread_nr", std::move(col_x_thread_nr));
        df.load_column<size_t>("x_coro_nr", std::move(col_x_coro_nr));
        df.load_column<size_t>("x_use_mn", std::move(col_x_use_mn));
        df.load_column<size_t>("x_entry_nr", std::move(col_x_link_entry_nr));
        df.load_column<size_t>("x_entry_size",
                               std::move(col_x_link_entry_size));
        df.load_column<size_t>("test_nr(total)", std::move(col_test_nr));
        df.load_column<size_t>("test_ns(total)", std::move(col_test_ns));

        auto div_f = gen_F_div<size_t, size_t, double>();
        auto div_f2 = gen_F_div<double, size_t, double>();
        auto ops_f = gen_F_ops<size_t, size_t, double>();
        auto mul_f = gen_F_mul<double, size_t, double>();
        auto mul_f2 = gen_F_mul<size_t, size_t, size_t>();

        // calculate cluster numbers
        auto client_nr = ::config::get_client_nids().size();
        auto replace_mul_f = gen_replace_F_mul<double>(client_nr);
        auto replace_mul_f_size = gen_replace_F_mul<size_t>(client_nr);

        df.consolidate<size_t, size_t, size_t>(
            "x_thread_nr", "x_coro_nr", "x_effective_client_nr", mul_f2, false);
        df.replace<size_t>("x_effective_client_nr", replace_mul_f_size);

        // for effective cpy bandwidth
        {
            df.consolidate<size_t, size_t, size_t>(
                "x_entry_nr", "x_entry_size", "byte_per_test", mul_f2, false);
            df.consolidate<size_t, size_t, size_t>("byte_per_test",
                                                   "test_nr(total)",
                                                   "byte(total)",
                                                   mul_f2,
                                                   false);
            // it is byte / ns, should scale to byte / s
            df.consolidate<size_t, size_t, double>("byte(total)",
                                                   "test_ns(total)",
                                                   "bandwidth(MB/s)",
                                                   div_f,
                                                   false);
            // 1e9 to convert ns to s. /1024 / 1024 to convert B to MB.
            auto replace_mul_bw = gen_replace_F_mul<double>(1e9 / 1024 / 1024);
            df.replace<double>("bandwidth(MB/s)", replace_mul_bw);
        }

        df.consolidate<size_t, size_t, double>(
            "test_ns(total)", "test_nr(total)", "test lat", div_f, false);
        df.consolidate<size_t, size_t, double>("test_nr(total)",
                                               "test_ns(total)",
                                               "test ops(total)",
                                               ops_f,
                                               false);
        df.consolidate<double, size_t, double>("test ops(total)",
                                               "x_thread_nr",
                                               "test ops(thread)",
                                               div_f2,
                                               false);

        df.load_column<double>("test ops(cluster)",
                               df.get_column<double>("test ops(total)"));
        df.replace<double>("test ops(cluster)", replace_mul_f);

        std::map<std::string, std::string> info;
        info.emplace("core", std::to_string(kServerThreadNr));
        auto filename = binary_to_csv_filename(argv[0], FLAGS_exec_meta, info);
        df.write<std::ostream, std::string, size_t, double>(std::cout,
                                                            io_format::csv2);
        df.write<std::string, size_t, double>(filename.c_str(),
                                              io_format::csv2);
    }

    // {
    //     StrDataFrame df;
    //     df.load_index(std::move(col_lat_idx));
    //     for (auto &[name, vec] : lat_data_)
    //     {
    //         df.load_column<uint64_t>(name.c_str(), std::move(vec));
    //     }

    //     std::map<std::string, std::string> info;
    //     info.emplace("kind", "lat");
    //     auto filename = binary_to_csv_filename(argv[0], FLAGS_exec_meta,
    //     info); df.write<std::ostream, std::string, size_t, double>(std::cout,
    //                                                         io_format::csv2);
    //     df.write<std::string, size_t, double>(filename.c_str(),
    //                                           io_format::csv2);
    // }

    patronus->keeper_barrier("finished", 100ms);
    LOG(INFO) << "Exiting...";
}
