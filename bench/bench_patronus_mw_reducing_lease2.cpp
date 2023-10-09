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
#include "patronus/bench_manager.h"
#include "util/DataFrameF.h"
#include "util/PerformanceReporter.h"
#include "util/Rand.h"
#include "util/TimeConv.h"

using namespace util::literals;
using namespace patronus;
using namespace std::chrono_literals;

constexpr static size_t kClientThreadNr = kMaxAppThread;
constexpr static size_t kServerThreadNr = NR_DIRECTORY;

constexpr static size_t kTestTimePerThread = 500_K;
// constexpr static size_t kTestTimePerThread = 200_K;

std::vector<std::string> col_idx;
std::vector<size_t> col_x_alloc_size;
std::vector<size_t> col_x_thread_nr;
std::vector<size_t> col_x_coro_nr;
std::vector<size_t> col_x_lease_time_ns;
std::vector<size_t> col_x_with_reuse_mw_opt;
std::vector<size_t> col_x_with_mw_locality_opt;
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
    ssize_t thread_remain_task;
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
    bool reuse_mw_opt{true};
    bool mw_locality_opt{true};

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
    bool report_latency() const
    {
        return report && thread_nr == 32 && coro_nr == 1;
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
        flag_t relinquish_flag =
            (flag_t) LeaseModifyFlag::kNoRelinquishUnbindAny;
        auto acquire_ns = 0ns;

        auto conf = BenchConfig::get_empty_conf(
            name, thread_nr, coro_nr, block_size, task_nr);
        conf.acquire_flag = acquire_flag;
        conf.relinquish_flag = relinquish_flag;
        conf.acquire_ns = acquire_ns;
        conf.report = true;
        return {conf};
    }
    static std::vector<BenchConfig> get_rlease_nothing_no_rel(
        const std::string &name,
        size_t thread_nr,
        size_t coro_nr,
        size_t block_size,
        size_t task_nr)
    {
        flag_t acquire_flag =
            (flag_t) AcquireRequestFlag::kDebugServerDoNothing;
        flag_t relinquish_flag = (flag_t) 0;  // do_not_call_relinquish is true
        auto acquire_ns = 0ns;

        auto conf = BenchConfig::get_empty_conf(
            name, thread_nr, coro_nr, block_size, task_nr);
        conf.acquire_flag = acquire_flag;
        conf.relinquish_flag = relinquish_flag;
        conf.acquire_ns = acquire_ns;
        conf.do_not_call_relinquish = true;
        conf.report = true;
        return {conf};
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
        flag_t relinquish_flag =
            (flag_t) LeaseModifyFlag::kNoRelinquishUnbindAny |
            (flag_t) LeaseModifyFlag::kWithDeallocation;
        auto acquire_ns = 0ns;

        auto conf = BenchConfig::get_empty_conf(
            name, thread_nr, coro_nr, block_size, task_nr);
        conf.acquire_flag = acquire_flag;
        conf.relinquish_flag = relinquish_flag;
        conf.acquire_ns = acquire_ns;
        conf.report = true;
        return {conf};
    }
    static std::vector<BenchConfig> get_rlease_one_bind(const std::string &name,
                                                        size_t thread_nr,
                                                        size_t coro_nr,
                                                        size_t block_size,
                                                        size_t task_nr)
    {
        flag_t acquire_flag = (flag_t) AcquireRequestFlag::kNoGc |
                              (flag_t) AcquireRequestFlag::kNoBindPR;
        flag_t relinquish_flag =
            (flag_t) LeaseModifyFlag::kNoRelinquishUnbindAny;
        auto acquire_ns = 0ns;
        auto conf = BenchConfig::get_empty_conf(
            name, thread_nr, coro_nr, block_size, task_nr);
        conf.acquire_flag = acquire_flag;
        conf.relinquish_flag = relinquish_flag;
        conf.acquire_ns = acquire_ns;
        conf.reuse_mw_opt = false;
        conf.report = true;
        return {conf};
    }
    // 1 + 1
    static std::vector<BenchConfig> get_rlease_one_bind_one_unbind(
        const std::string &name,
        size_t thread_nr,
        size_t coro_nr,
        size_t block_size,
        size_t task_nr,
        std::chrono::nanoseconds acquire_ns)
    {
        flag_t acquire_flag = (flag_t) AcquireRequestFlag::kNoBindPR;
        flag_t relinquish_flag = (flag_t) 0;
        auto conf = BenchConfig::get_empty_conf(
            name, thread_nr, coro_nr, block_size, task_nr);
        conf.acquire_flag = acquire_flag;
        conf.relinquish_flag = relinquish_flag;
        conf.do_not_call_relinquish = true;
        conf.acquire_ns = acquire_ns;
        conf.reuse_mw_opt = false;
        conf.report = true;
        return {conf};
    }
    // 1 + 0 (at expetation), no locality
    static std::vector<BenchConfig>
    get_rlease_one_bind_one_unbind_reuse_mw_opt_no_locality(
        const std::string &name,
        size_t thread_nr,
        size_t coro_nr,
        size_t block_size,
        size_t task_nr,
        std::chrono::nanoseconds acquire_ns)
    {
        auto ret = get_rlease_one_bind_one_unbind(
            name, thread_nr, coro_nr, block_size, task_nr, acquire_ns);
        for (auto &conf : ret)
        {
            conf.reuse_mw_opt = true;
            conf.mw_locality_opt = false;
        }
        return ret;
    }
    // 1 + 0 (at expetation)
    static std::vector<BenchConfig> get_rlease_one_bind_one_unbind_reuse_mw_opt(
        const std::string &name,
        size_t thread_nr,
        size_t coro_nr,
        size_t block_size,
        size_t task_nr,
        std::chrono::nanoseconds acquire_ns)
    {
        auto ret = get_rlease_one_bind_one_unbind(
            name, thread_nr, coro_nr, block_size, task_nr, acquire_ns);
        for (auto &conf : ret)
        {
            conf.reuse_mw_opt = true;
        }
        return ret;
    }
    // 1 + 0 (at expetation) + gc
    static std::vector<BenchConfig>
    get_rlease_one_bind_one_unbind_reuse_mw_opt_gc(
        const std::string &name,
        size_t thread_nr,
        size_t coro_nr,
        size_t block_size,
        size_t task_nr,
        std::chrono::nanoseconds acquire_ns)
    {
        flag_t acquire_flag = (flag_t) AcquireRequestFlag::kNoBindPR;
        flag_t relinquish_flag = (flag_t) 0;
        auto conf = BenchConfig::get_empty_conf(
            name, thread_nr, coro_nr, block_size, task_nr);
        conf.acquire_flag = acquire_flag;
        conf.relinquish_flag = relinquish_flag;
        conf.acquire_ns = acquire_ns;
        conf.do_not_call_relinquish = true;
        conf.reuse_mw_opt = true;
        conf.report = true;
        return {conf};
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
        conf.report = true;
        return {conf};
    }
    static std::vector<BenchConfig> get_rlease_one_bind_one_unbind_autogc(
        const std::string &name,
        size_t thread_nr,
        size_t coro_nr,
        size_t block_size,
        size_t task_nr)
    {
        flag_t acquire_flag = (flag_t) AcquireRequestFlag::kNoBindPR;
        flag_t relinquish_flag =
            (flag_t) LeaseModifyFlag::kNoRpc;  // definitely no rpc
        auto acquire_ns = 1ns;
        auto conf = BenchConfig::get_empty_conf(
            name, thread_nr, coro_nr, block_size, task_nr);
        conf.acquire_flag = acquire_flag;
        conf.relinquish_flag = relinquish_flag;
        conf.acquire_ns = acquire_ns;
        conf.do_not_call_relinquish = true;
        conf.reuse_mw_opt = false;
        conf.report = true;
        return {conf};
    }
    static std::vector<BenchConfig> get_rlease_one_bind_one_unbind_autogc_dbg(
        const std::string &name,
        size_t thread_nr,
        size_t coro_nr,
        size_t block_size,
        size_t task_nr)
    {
        auto ret = get_rlease_one_bind_one_unbind_autogc(
            name, thread_nr, coro_nr, block_size, task_nr);
        for (auto &conf : ret)
        {
            conf.acquire_flag |= (flag_t) AcquireRequestFlag::kDebugFlag_1;
        }
        return ret;
    }

    // 2 + 2 w/o MW locality
    static std::vector<BenchConfig> get_rlease_full_no_locality(
        const std::string &name,
        size_t thread_nr,
        size_t coro_nr,
        size_t block_size,
        size_t task_nr,
        std::chrono::nanoseconds acquire_ns)
    {
        flag_t acquire_flag = 0;
        flag_t relinquish_flag = 0;
        auto conf = BenchConfig::get_empty_conf(
            name, thread_nr, coro_nr, block_size, task_nr);
        conf.acquire_flag = acquire_flag;
        conf.relinquish_flag = relinquish_flag;
        conf.acquire_ns = acquire_ns;
        conf.do_not_call_relinquish = true;
        conf.reuse_mw_opt = false;
        conf.mw_locality_opt = false;
        conf.report = true;
        return {conf};
    }

    // 2 + 2
    static std::vector<BenchConfig> get_rlease_full(
        const std::string &name,
        size_t thread_nr,
        size_t coro_nr,
        size_t block_size,
        size_t task_nr,
        std::chrono::nanoseconds acquire_ns)
    {
        flag_t acquire_flag = 0;
        flag_t relinquish_flag = 0;
        auto conf = BenchConfig::get_empty_conf(
            name, thread_nr, coro_nr, block_size, task_nr);
        conf.acquire_flag = acquire_flag;
        conf.relinquish_flag = relinquish_flag;
        conf.acquire_ns = acquire_ns;
        conf.do_not_call_relinquish = true;
        conf.reuse_mw_opt = false;
        conf.report = true;
        return {conf};
    }
    // 2 + 1
    static std::vector<BenchConfig> get_rlease_full_wo_unbind_pr(
        const std::string &name,
        size_t thread_nr,
        size_t coro_nr,
        size_t block_size,
        size_t task_nr,
        std::chrono::nanoseconds acquire_ns)
    {
        flag_t acquire_flag = 0;
        flag_t relinquish_flag =
            (flag_t) LeaseModifyFlag::kNoRelinquishUnbindPr;
        auto conf = BenchConfig::get_empty_conf(
            name, thread_nr, coro_nr, block_size, task_nr);
        conf.acquire_flag = acquire_flag;
        conf.relinquish_flag = relinquish_flag;
        conf.do_not_call_relinquish = true;
        conf.acquire_ns = acquire_ns;
        conf.reuse_mw_opt = false;
        conf.report = true;
        return {conf};
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
        conf.report = true;
        return {conf};
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
        conf.reuse_mw_opt = false;
        conf.report = true;
        return {conf};
    }

private:
    // static std::vector<BenchConfig> pipeline(std::vector<BenchConfig>
    // &&confs)
    // {
    //     for (auto &conf : confs)
    //     {
    //         conf.report = false;
    //     }
    //     confs.back().report = true;
    //     return confs;
    // }
};

using CoroComm = CoroCommunication;
using Config = BenchConfig;

void reg_result(size_t total_ns, const BenchConfig &conf)
{
    col_idx.push_back(conf.name);
    col_x_alloc_size.push_back(conf.block_size);
    col_x_thread_nr.push_back(conf.thread_nr);
    col_x_coro_nr.push_back(conf.coro_nr);
    auto ns = util::time::to_ns(conf.acquire_ns);
    col_x_lease_time_ns.push_back(ns);
    col_x_with_reuse_mw_opt.push_back(conf.reuse_mw_opt);
    col_x_with_mw_locality_opt.push_back(conf.mw_locality_opt);
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
struct Context
{
    std::unique_ptr<OnePassBucketMonitor<uint64_t>> lat_m;
    ChronoTimer timer;
};

void do_bench(Patronus::pointer p,
              Context &context,
              CoroComm &coro_comm,
              const Config &conf,
              CoroContext &ctx,
              bool is_master)
{
    auto tid = p->get_thread_id();
    if (unlikely(tid >= conf.thread_nr))
    {
        return;
    }

    auto alloc_size = conf.block_size;
    auto acquire_ns = conf.acquire_ns;
    auto acquire_flag = conf.acquire_flag;
    auto relinquish_flag = conf.relinquish_flag;
    auto do_not_call_relinquish = conf.do_not_call_relinquish;

    auto coro_id = ctx.coro_id();

    auto dir_id = tid % kServerThreadNr;
    auto server_nid = ::config::get_server_nids().front();

    size_t fail_nr = 0;
    size_t succ_nr = 0;

    ChronoTimer timer;
    ChronoTimer op_timer;
    while (coro_comm.thread_remain_task > 0)
    {
        bool succ = true;
        if (is_master && coro_id == 0)
        {
            op_timer.pin();
        }
        auto lease = p->get_rlease(server_nid,
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
                p->relinquish(lease, 0 /* hint */, relinquish_flag, &ctx);
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
            context.lat_m->collect(ns);
        }
    }
    auto total_ns = timer.pin();

    VLOG(1) << "[bench] tid " << tid << " got " << fail_nr
            << " failed lease. succ lease: " << succ_nr << " within "
            << total_ns << " ns. coro: " << ctx;
}

struct BenchResult
{
    double lat_min;
    double lat_p5;
    double lat_p9;
    double lat_p99;
};

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    PatronusConfig config;
    config.machine_nr = ::config::kMachineNr;
    config.block_class = {2_MB};
    config.block_ratio = {1};

    LOG(ERROR) << "TODO: since auto-expiration is turn on, the rolling header "
                  "is not work. Try to add a flag in acquisition";
    LOG(ERROR)
        << "TODO: launcher still not work: the coro_comm.thread_remain_task is "
           "not updated. It is because the maintainance of "
           "std::atomic<uint64_t> task_nr and the coro_comm.thread_remain_task "
           "needs both effort from master and worker coro. Still not figure "
           "out how to tidy up the codes.";

    auto patronus = Patronus::ins(config);

    auto nid = patronus->get_node_id();
    bool is_client = ::config::is_client(nid);

    patronus::bench::PatronusManager<Context, Config, CoroComm> manager(
        patronus, is_client ? kClientThreadNr : kServerThreadNr, kCoroCnt);

    if (is_client)
    {
        manager.register_task([](Patronus::pointer p,
                                 Context &context,
                                 CoroComm &coro_comm,
                                 const Config &config,
                                 CoroContext &ctx,
                                 bool is_master) {
            do_bench(p, context, coro_comm, config, ctx, is_master);
        });
        manager.register_start_bench([](Context &context, const Config &) {
            context.timer.pin();
            LOG(INFO) << "[bench] called timer.pin";
        });
        manager.register_end_bench([](Context &context, const Config &conf) {
            auto ns = context.timer.pin();
            reg_result(ns, conf);
            LOG(INFO) << "[bench] called timer.pin on end. takes: "
                      << util::pre_ns(ns);
        });
        manager.register_init([](Context &context) {
            auto min = util::time::to_ns(0ns);
            auto max = util::time::to_ns(10ms);
            auto range = util::time::to_ns(1us);
            context.lat_m = std::make_unique<OnePassBucketMonitor<uint64_t>>(
                min, max, range);
        });
    }

    std::vector<Config> bench_configs;
    for (size_t thread_nr : {32})
    {
        CHECK_LE(thread_nr, kMaxAppThread);
        for (size_t block_size : {64ul})
        {
            for (size_t coro_nr : {2, 4, 8, 16, 32})
            // for (size_t coro_nr : {32})
            {
                auto total_test_times = kTestTimePerThread * 4;
                {
                    auto configs = BenchConfigFactory::get_rlease_nothing(
                        "RPC",
                        thread_nr,
                        coro_nr,
                        block_size,
                        total_test_times);
                    bench_configs.insert(
                        bench_configs.end(), configs.begin(), configs.end());
                }
            }
        }
    }
    manager.bench(bench_configs);

    {
        StrDataFrame df;
        df.load_index(std::move(col_idx));
        df.load_column<size_t>("x_alloc_size", std::move(col_x_alloc_size));
        df.load_column<size_t>("x_thread_nr", std::move(col_x_thread_nr));
        df.load_column<size_t>("x_coro_nr", std::move(col_x_coro_nr));
        df.load_column<size_t>("x_lease_time(ns)",
                               std::move(col_x_lease_time_ns));
        df.load_column<size_t>("x_with_reuse_mw_opt",
                               std::move(col_x_with_reuse_mw_opt));
        df.load_column<size_t>("x_with_mw_locality_opt",
                               std::move(col_x_with_mw_locality_opt));
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

    {
        StrDataFrame df;
        df.load_index(std::move(col_lat_idx));
        for (auto &[name, vec] : lat_data_)
        {
            df.load_column<uint64_t>(name.c_str(), std::move(vec));
        }

        std::map<std::string, std::string> info;
        info.emplace("kind", "lat");
        auto filename = binary_to_csv_filename(argv[0], FLAGS_exec_meta, info);
        df.write<std::ostream, std::string, size_t, double>(std::cout,
                                                            io_format::csv2);
        df.write<std::string, size_t, double>(filename.c_str(),
                                              io_format::csv2);
    }

    patronus->keeper_barrier("finished", 100ms);
    LOG(INFO) << "Exiting...";
}
