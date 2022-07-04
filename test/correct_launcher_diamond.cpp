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
#include "thirdparty/serverless/serverless.h"
#include "util/DataFrameF.h"
#include "util/PerformanceReporter.h"
#include "util/Rand.h"
#include "util/TimeConv.h"

using namespace util::literals;
using namespace patronus;
using namespace std::chrono_literals;

// constexpr static size_t kClientThreadNr = kMaxAppThread;
constexpr static size_t kClientThreadNr = 1;
constexpr static size_t kServerThreadNr = NR_DIRECTORY;

// constexpr static size_t kTestTimePerThread = 5_K;
constexpr static size_t kTestTimePerThread = 100;

constexpr static size_t kChainNr = 1;

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

inline size_t gen_coro_key(size_t node_id, size_t thread_id, size_t coro_id)
{
    CHECK_LT(coro_id, kCoroCnt);
    return node_id * kMaxAppThread * kCoroCnt + thread_id * kCoroCnt + coro_id;
}
uint64_t bench_locator(uint64_t key)
{
    // the 4 is not needed
    return key * 4 * sizeof(uint64_t);
}

struct BenchConfig
{
    size_t thread_nr;
    size_t coro_nr;
    size_t task_nr;
    std::string name;
    bool report;

    static BenchConfig get_empty_conf(const std::string &name,
                                      size_t thread_nr,
                                      size_t coro_nr,
                                      size_t task_nr)
    {
        BenchConfig ret;
        ret.name = name;
        ret.thread_nr = thread_nr;
        ret.coro_nr = coro_nr;
        ret.task_nr = task_nr;
        return ret;
    }
};

class BenchConfigFactory
{
public:
    static std::vector<BenchConfig> get_basic(const std::string &name,
                                              size_t thread_nr,
                                              size_t coro_nr,
                                              size_t task_nr)
    {
        auto conf =
            BenchConfig::get_empty_conf(name, thread_nr, coro_nr, task_nr);
        return pipeline({conf});
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

struct BenchResult
{
    double lat_min;
    double lat_p5;
    double lat_p9;
    double lat_p99;
};

using Parameters = serverless::Parameters;
using ParameterMap = serverless::Parameters::ParameterMap;
using lambda_t = serverless::CoroLauncher::lambda_t;
struct Comm
{
    uint64_t magic;
};

size_t worker_do_nr_{0};

RetCode worker_do(Parameters &parameters,
                  bool root,
                  bool tail,
                  size_t expect_faa_nr,
                  CoroContext *ctx,
                  util::TraceView trace)
{
    worker_do_nr_++;
    Comm *c = nullptr;
    if (root)
    {
        c = new Comm();
        parameters.prv() = c;
        c->magic = fast_pseudo_rand_int();
    }
    else
    {
        c = (Comm *) parameters.prv();
    }

    if (root)
    {
        auto buffer = parameters.get_buffer(sizeof(uint64_t));
        memcpy(buffer.buffer, &(c->magic), sizeof(uint64_t));
        parameters.write("addr", std::move(buffer), ctx, trace).expect(RC::kOk);
    }
    else if (!tail)
    {
        auto buffer = parameters.get_buffer(sizeof(int64_t));
        parameters.faa("addr", 1, buffer, ctx, trace).expect(RC::kOk);
        parameters.put_rdma_buffer(std::move(buffer));
    }
    else
    {
        CHECK(tail);
        auto got_buffer = parameters.read("addr", ctx, trace);
        int64_t got = *(int64_t *) got_buffer.buffer;
        CHECK_EQ(got, c->magic + expect_faa_nr);
        parameters.put_rdma_buffer(std::move(got_buffer));
    }

    if (tail)
    {
        delete (Comm *) parameters.prv();
        parameters.prv() = nullptr;
    }
    return RC::kOk;
}

void register_lambdas(serverless::CoroLauncher &launcher,
                      Patronus::pointer patronus,
                      size_t total_lambda_nr,
                      size_t chain_nr)
{
    size_t chain_length = total_lambda_nr / chain_nr;
    CHECK_GT(chain_length, 2);
    size_t chain_middle_length = chain_length - 2;  // sub root and tail

    auto root = [chain_middle_length](Parameters &parameters,
                                      CoroContext *ctx,
                                      util::TraceView trace) -> RetCode {
        return worker_do(parameters,
                         true /* is root */,
                         false /* is tail */,
                         chain_middle_length,
                         ctx,
                         trace);
    };
    auto tail = [chain_middle_length](Parameters &parameters,
                                      CoroContext *ctx,
                                      util::TraceView trace) -> RetCode {
        return worker_do(parameters,
                         false /* is root */,
                         true /* is tail */,
                         chain_middle_length,
                         ctx,
                         trace);
    };
    auto middle = [chain_middle_length](Parameters &parameters,
                                        CoroContext *ctx,
                                        util::TraceView trace) -> RetCode {
        return worker_do(parameters,
                         false /* is root */,
                         false /* is tail */,
                         chain_middle_length,
                         ctx,
                         trace);
    };
    auto tid = patronus->get_thread_id();
    auto nid = patronus->get_node_id();
    for (size_t i = 0; i < chain_nr; ++i)
    {
        // one chain
        ParameterMap init_param;
        auto &addr = init_param["addr"];
        CHECK_LT(i, kCoroCnt);
        auto key = gen_coro_key(nid, tid, i);
        auto offset = bench_locator(key);
        addr.gaddr = GlobalAddress(0, offset);
        addr.size = sizeof(uint64_t);

        lambda_t root_id = launcher.add_lambda(root,
                                               init_param,
                                               {} /* recv param from */,
                                               {} /* depend on */,
                                               {} /* reloop to */);
        std::vector<lambda_t> middle_lambdas;
        for (size_t k = 0; k < chain_middle_length; ++k)
        {
            auto id = launcher.add_lambda(middle,
                                          {} /* init param */,
                                          root_id /* recv param from */,
                                          {root_id} /* depend on */,
                                          {});
            middle_lambdas.push_back(id);
        }

        launcher.add_lambda(tail,
                            {} /* init param */,
                            root_id /* recv param from */,
                            middle_lambdas /* depend on */,
                            root_id /* reloop to */);
    }
}

void bench_alloc_thread_coro(
    Patronus::pointer patronus,
    [[maybe_unused]] OnePassBucketMonitor<uint64_t> &lat_m,
    [[maybe_unused]] bool is_master,
    std::atomic<ssize_t> &work_nr,
    const serverless::Config &serverless_config,
    const BenchConfig &conf)
{
    auto test_times = conf.task_nr;
    auto server_nid = ::config::get_server_nids().front();
    auto tid = patronus->get_thread_id();
    auto dir_id = tid % kServerThreadNr;

    serverless::CoroLauncher launcher(
        patronus, server_nid, dir_id, serverless_config, test_times, work_nr);
    register_lambdas(launcher, patronus, kCoroCnt, kChainNr);

    launcher.launch();

    return;
}

void bench_template(const std::string &name,
                    Patronus::pointer patronus,
                    boost::barrier &bar,
                    std::atomic<ssize_t> &work_nr,
                    bool is_master,
                    const serverless::Config &serverless_config,
                    const BenchConfig &conf)

{
    auto test_times = conf.task_nr;
    auto thread_nr = conf.thread_nr;
    auto coro_nr = conf.coro_nr;
    auto report = conf.report;

    if (is_master)
    {
        work_nr = test_times;
        LOG(INFO) << "[bench] BENCH: " << name << ", thread_nr: " << thread_nr
                  << ", coro_nr: " << coro_nr << ", report: " << report;
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
        bench_alloc_thread_coro(
            patronus, lat_m, is_master, work_nr, serverless_config, conf);
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
              << ", coro_nr: " << conf.coro_nr << ", report: " << conf.report;

    patronus->server_serve(key);
}
std::atomic<ssize_t> shared_task_nr;

void run_benchmark_client(Patronus::pointer patronus,
                          const serverless::Config &serverless_config,
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
                   is_master,
                   serverless_config,
                   conf);
    bar.wait();
    if (is_master)
    {
        patronus->finished(key);
    }
}

void run_benchmark(Patronus::pointer patronus,
                   const serverless::Config &serverless_config,
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
            run_benchmark_client(
                patronus, serverless_config, conf, bar, is_master, key);
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

    std::vector<serverless::Config> serverless_configs;
    // serverless_configs.emplace_back(
    //     serverless::Config::get_mw("mw[step]", true));
    serverless_configs.emplace_back(
        serverless::Config::get_mw("mw[nested]", false));

    // for (size_t thread_nr : {32})
    // for (size_t thread_nr : {32})
    for (size_t thread_nr : {1})
    {
        CHECK_LE(thread_nr, kMaxAppThread);
        // for (size_t coro_nr : {2, 4, 8, 16, 32})
        // for (size_t coro_nr : {1, 32})
        for (size_t coro_nr : {32})
        {
            auto total_test_times = kTestTimePerThread * 4;
            for (const auto &serverless_config : serverless_configs)
            {
                LOG_IF(INFO, is_master) << "[config] " << serverless_config;
                auto configs = BenchConfigFactory::get_basic(
                    "diamond", thread_nr, coro_nr, total_test_times);
                run_benchmark(patronus,
                              serverless_config,
                              configs,
                              bar,
                              is_client,
                              is_master,
                              key);
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

    patronus->keeper_barrier("finished", 100ms);
    LOG(INFO) << "Finished.";
}
