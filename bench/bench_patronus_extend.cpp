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

using namespace define::literals;
using namespace patronus;
using namespace std::chrono_literals;

constexpr static size_t kClientThreadNr = kMaxAppThread;
constexpr static size_t kServerThreadNr = NR_DIRECTORY;

constexpr static size_t kTestTimePerThread = 100_K;
// constexpr static size_t kTestTimePerThread = 100;

std::vector<std::string> col_idx;
std::vector<size_t> col_x_alloc_size;
std::vector<size_t> col_x_thread_nr;
std::vector<size_t> col_x_coro_nr;
std::vector<size_t> col_x_extend_nr;
std::vector<size_t> col_alloc_nr;
std::vector<size_t> col_alloc_ns;

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
    size_t block_size;
    size_t task_nr;
    std::chrono::nanoseconds acquire_ns;
    size_t extend_nr;
    std::chrono::nanoseconds extend_ns;
    bool extend_use_rpc;
    flag_t extend_flag;
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

std::ostream &operator<<(std::ostream &os, const BenchConfig &conf)
{
    auto acq_ns = util::time::to_ns(conf.acquire_ns);
    auto ext_ns = util::time::to_ns(conf.extend_ns);
    os << "`" << conf.name << "`: {Conf: thread_nr " << conf.thread_nr
       << ", coro_nr: " << conf.coro_nr << ", block_size: " << conf.block_size
       << ", task_nr: " << conf.task_nr << ", acquire_ns: " << acq_ns
       << ", extend_ns: " << ext_ns << ", extend_nr: " << conf.extend_nr
       << ", use_rpc: " << conf.extend_use_rpc
       << ", extend_flag: " << LeaseModifyFlagOut(conf.extend_flag)
       << ", report: " << conf.report << "}";
    return os;
}

class BenchConfigFactory
{
public:
    static std::vector<BenchConfig> get_extend(const std::string &name,
                                               size_t thread_nr,
                                               size_t coro_nr,
                                               size_t block_size,
                                               size_t task_nr,
                                               size_t extend_nr)
    {
        auto conf = BenchConfig::get_empty_conf(
            name, thread_nr, coro_nr, block_size, task_nr);
        conf.acquire_ns = 1ms;
        conf.extend_nr = extend_nr;
        conf.extend_ns = 1ms;
        conf.extend_use_rpc = false;
        conf.extend_flag = (flag_t) 0;
        return pipeline({conf, conf});
    }
    static std::vector<BenchConfig> get_rpc_extend(const std::string &name,
                                                   size_t thread_nr,
                                                   size_t coro_nr,
                                                   size_t block_size,
                                                   size_t task_nr,
                                                   size_t extend_nr)
    {
        auto confs = get_extend(
            name, thread_nr, coro_nr, block_size, task_nr, extend_nr);
        for (auto &conf : confs)
        {
            conf.extend_use_rpc = true;
        }
        return confs;
    }
    static std::vector<BenchConfig> get_rpc(const std::string &name,
                                            size_t thread_nr,
                                            size_t coro_nr,
                                            size_t block_size,
                                            size_t task_nr,
                                            size_t extend_nr)
    {
        auto confs = get_rpc_extend(
            name, thread_nr, coro_nr, block_size, task_nr, extend_nr);
        for (auto &conf : confs)
        {
            conf.extend_flag = (flag_t) LeaseModifyFlag::kDebugExtendDoNothing;
        }
        return confs;
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

void reg_result(const BenchConfig &conf, uint64_t total_ns)
{
    col_idx.push_back(conf.name);
    col_x_alloc_size.push_back(conf.block_size);
    col_x_thread_nr.push_back(conf.thread_nr);
    col_x_coro_nr.push_back(conf.coro_nr);
    col_x_extend_nr.push_back(conf.extend_nr);
    col_alloc_nr.push_back(conf.task_nr);
    col_alloc_ns.push_back(total_ns);
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
                                    OnePassBucketMonitor<uint64_t> &lat_m,
                                    bool is_master,
                                    const BenchConfig &conf)
{
    // TODO: latency
    std::ignore = lat_m;

    auto tid = patronus->get_thread_id();
    auto dir_id = tid % kServerThreadNr;
    auto server_nid = ::config::get_server_nids().front();

    auto alloc_size = conf.block_size;
    auto acquire_ns = conf.acquire_ns;

    CoroContext ctx(tid, &yield, &coro_comm.master, coro_id);

    size_t fail_nr = 0;
    size_t succ_nr = 0;

    ChronoTimer timer;
    ChronoTimer op_timer;
    while (coro_comm.thread_remain_task > 0)
    {
        // bool succ = true;
        if (is_master && coro_id == 0)
        {
            op_timer.pin();
        }
        auto acquire_flag = (flag_t) AcquireRequestFlag::kNoGc;
        auto lease = patronus->get_rlease(server_nid,
                                          dir_id,
                                          nullgaddr,
                                          0 /* alloc_hint */,
                                          alloc_size,
                                          acquire_ns,
                                          acquire_flag,
                                          &ctx);
        if (unlikely(!lease.success()))
        {
            CHECK_EQ(lease.ec(), AcquireRequestStatus::kMagicMwErr);
            continue;
        }

        for (size_t i = 0; i < conf.extend_nr; ++i)
        {
            if (conf.extend_use_rpc)
            {
                auto rc = patronus->rpc_extend(
                    lease, conf.extend_ns, conf.extend_flag, &ctx);
                CHECK_EQ(rc, RC::kOk);
            }
            else
            {
                auto rc = patronus->extend(
                    lease, conf.extend_ns, conf.extend_flag, &ctx);
                CHECK_EQ(rc, RC::kOk)
                    << "** extended failed at " << i << " attempt.";
            }
        }

        auto rel_flag = (flag_t) 0;
        patronus->relinquish(lease, 0 /* hint */, rel_flag, &ctx);

        coro_comm.thread_remain_task--;
    }

    auto total_ns = timer.pin();

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

void bench_alloc_thread_coro(Patronus::pointer patronus,
                             OnePassBucketMonitor<uint64_t> &lat_m,
                             bool is_master,
                             std::atomic<ssize_t> &work_nr,
                             const BenchConfig &conf)
{
    auto tid = patronus->get_thread_id();
    auto dir_id = tid % kServerThreadNr;
    auto test_times = conf.task_nr;
    auto coro_nr = conf.coro_nr;

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
                                         &conf,
                                         is_master](CoroYield &yield) {
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

void bench_template(Patronus::pointer patronus,
                    boost::barrier &bar,
                    std::atomic<ssize_t> &work_nr,
                    bool is_master,
                    const BenchConfig &conf)

{
    auto thread_nr = conf.thread_nr;
    bool report = conf.report;
    if (is_master)
    {
        LOG(INFO) << "[bench] BENCH: " << conf;
    }

    bar.wait();
    ChronoTimer timer;

    auto tid = patronus->get_thread_id();

    auto min = util::time::to_ns(0ns);
    auto max = util::time::to_ns(10ms);
    auto range = util::time::to_ns(1us);
    OnePassBucketMonitor lat_m(min, max, range);

    if (tid < thread_nr)
    {
        bench_alloc_thread_coro(patronus, lat_m, is_master, work_nr, conf);
    }

    bar.wait();
    auto total_ns = timer.pin();
    if (is_master && report)
    {
        // reg_result(conf.name,
        //            conf.task_nr,
        //            total_ns,
        //            conf.block_size,
        //            conf.thread_nr,
        //            conf.coro_nr,
        //            conf.acquire_ns,
        //            conf.extend_nr);
        reg_result(conf, total_ns);
        reg_latency(conf.name, lat_m);
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

    bench_template(patronus, bar, shared_task_nr, is_master, conf);
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

constexpr static size_t kExtendNr = 8;
void benchmark(Patronus::pointer patronus,
               boost::barrier &bar,
               bool is_client,
               bool is_master)
{
    bar.wait();

    constexpr static size_t kBlockSize = 64;

    size_t key = 0;
    // for (size_t thread_nr : {1})
    for (size_t thread_nr : {1, 2, 4, 8, 16, 32})
    // for (size_t thread_nr : {32})
    {
        CHECK_LE(thread_nr, kMaxAppThread);
        // for (size_t extend_nr : {2, 4, 8, 6, 10, 12, 16})
        for (size_t extend_nr : {kExtendNr})
        {
            for (size_t coro_nr : {1})
            {
                auto total_test_times = kTestTimePerThread * thread_nr;
                {
                    auto configs =
                        BenchConfigFactory::get_extend("ext",
                                                       thread_nr,
                                                       coro_nr,
                                                       kBlockSize,
                                                       total_test_times,
                                                       extend_nr);
                    run_benchmark(
                        patronus, configs, bar, is_client, is_master, key);
                }
                {
                    auto configs =
                        BenchConfigFactory::get_rpc_extend("rpc-ext",
                                                           thread_nr,
                                                           coro_nr,
                                                           kBlockSize,
                                                           total_test_times,
                                                           extend_nr);
                    run_benchmark(
                        patronus, configs, bar, is_client, is_master, key);
                }
                {
                    auto configs = BenchConfigFactory::get_rpc("rpc",
                                                               thread_nr,
                                                               coro_nr,
                                                               kBlockSize,
                                                               total_test_times,
                                                               extend_nr);
                    run_benchmark(
                        patronus, configs, bar, is_client, is_master, key);
                }
            }
        }
    }

    for (size_t thread_nr : {32})
    {
        CHECK_LE(thread_nr, kMaxAppThread);
        for (size_t extend_nr : {kExtendNr})
        {
            // for (size_t coro_nr : {2, 4, 8, 16, 32})
            for (size_t coro_nr : {2, 4, 8, 16})
            // for (size_t coro_nr : {16})
            {
                auto total_test_times = kTestTimePerThread * 4;
                {
                    auto configs =
                        BenchConfigFactory::get_extend("ext",
                                                       thread_nr,
                                                       coro_nr,
                                                       kBlockSize,
                                                       total_test_times,
                                                       extend_nr);
                    run_benchmark(
                        patronus, configs, bar, is_client, is_master, key);
                }
                {
                    auto configs =
                        BenchConfigFactory::get_rpc_extend("rpc-ext",
                                                           thread_nr,
                                                           coro_nr,
                                                           kBlockSize,
                                                           total_test_times,
                                                           extend_nr);
                    run_benchmark(
                        patronus, configs, bar, is_client, is_master, key);
                }
                {
                    auto configs = BenchConfigFactory::get_rpc("rpc",
                                                               thread_nr,
                                                               coro_nr,
                                                               kBlockSize,
                                                               total_test_times,
                                                               extend_nr);
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

    {
        StrDataFrame df;
        df.load_index(std::move(col_idx));
        df.load_column<size_t>("x_alloc_size", std::move(col_x_alloc_size));
        df.load_column<size_t>("x_thread_nr", std::move(col_x_thread_nr));
        df.load_column<size_t>("x_coro_nr", std::move(col_x_coro_nr));
        df.load_column<size_t>("x_extend_nr", std::move(col_x_extend_nr));
        df.load_column<size_t>("alloc_nr(total)", std::move(col_alloc_nr));
        df.load_column<size_t>("alloc_ns(total)", std::move(col_alloc_ns));

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

        df.consolidate<size_t, size_t, double>(
            "alloc_ns(total)", "alloc_nr(total)", "alloc lat", div_f, false);
        df.consolidate<size_t, size_t, double>("alloc_nr(total)",
                                               "alloc_ns(total)",
                                               "alloc ops(total)",
                                               ops_f,
                                               false);
        df.consolidate<double, size_t, double>("alloc ops(total)",
                                               "x_thread_nr",
                                               "alloc ops(thread)",
                                               div_f2,
                                               false);

        df.load_column<double>("alloc ops(cluster)",
                               df.get_column<double>("alloc ops(total)"));
        df.replace<double>("alloc ops(cluster)", replace_mul_f);

        auto filename = binary_to_csv_filename(argv[0], FLAGS_exec_meta);
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
    //     info); df.write<std::ostream, std::string, size_t,
    //     double>(std::cout,
    //                                                         io_format::csv2);
    //     df.write<std::string, size_t, double>(filename.c_str(),
    //                                           io_format::csv2);
    // }

    patronus->keeper_barrier("finished", 100ms);
    LOG(INFO) << "Exiting...";
}
