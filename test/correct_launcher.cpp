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

constexpr static size_t kClientThreadNr = kMaxAppThread;
// constexpr static size_t kClientThreadNr = 1;
constexpr static size_t kServerThreadNr = NR_DIRECTORY;

constexpr static size_t kChainNr = 4;
// constexpr static size_t kChainNr = 1;

constexpr static size_t kTestTimePerThread = 10_K;
// constexpr static size_t kTestTimePerThread = 100;

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
using lambda_t = serverless::CoroLauncher::lambda_t;
using ParameterMap = serverless::Parameters::ParameterMap;
struct Comm
{
    uint64_t magic;
};

size_t worker_do_nr_{0};
RetCode worker_do(Parameters &parameters,
                  bool root,
                  bool tail,
                  CoroContext *ctx,
                  util::TraceView trace = util::nulltrace)
{
    worker_do_nr_++;

    Comm *c = nullptr;
    if (root)
    {
        auto magic = fast_pseudo_rand_int();
        auto buffer = parameters.get_buffer(sizeof(uint64_t));
        memcpy(buffer.buffer, &magic, sizeof(uint64_t));
        parameters.write("addr", std::move(buffer), ctx, trace).expect(RC::kOk);
        trace.pin("init magic");
        CHECK_EQ(parameters.prv(), nullptr)
            << "** parameters at " << (void *) &parameters << " with prv "
            << parameters.prv() << " not null";
        c = new Comm();
        c->magic = magic;
        parameters.prv() = (void *) c;
        CHECK_EQ(parameters.prv(), c);
        CHECK_NE(parameters.prv(), nullptr);
    }
    else
    {
        c = (Comm *) parameters.prv();
        CHECK_NE(parameters.prv(), nullptr);
    }

    // read, validate same
    {
        auto [buffer_read, _] = parameters.read("addr", ctx, trace);
        std::ignore = _;
        uint64_t got = *(uint64_t *) buffer_read.buffer;
        CHECK_EQ(got, c->magic);
        parameters.put_rdma_buffer(std::move(buffer_read));
    }

    // write to magic + 1
    {
        auto buffer_write = parameters.get_buffer(sizeof(uint64_t));
        c->magic = c->magic + 1;
        memcpy(buffer_write.buffer, &(c->magic), sizeof(uint64_t));
        parameters.write("addr", std::move(buffer_write), ctx, trace)
            .expect(RC::kOk);
    }

    // debug: reread, make sure we really write it successfully
    {
        auto [buffer_read, _] = parameters.read("addr", ctx, trace);
        std::ignore = _;
        auto got = *(uint64_t *) buffer_read.buffer;
        CHECK_EQ(got, c->magic);
        parameters.put_rdma_buffer(std::move(buffer_read));
    }

    if (tail)
    {
        delete c;
        // LOG(INFO) << "[debug] !! bench! setting prv to nullptr";
        parameters.prv() = nullptr;
    }

    return RC::kOk;
}

void register_lambda(Patronus::pointer patronus,
                     serverless::CoroLauncher &launcher,
                     size_t lambda_nr,
                     size_t chain_nr)
{
    auto root = [](Parameters &parameters,
                   CoroContext *ctx,
                   TraceView trace) -> RetCode {
        return worker_do(
            parameters, true /* is root */, false /* is tail */, ctx, trace);
    };
    auto tail = [](Parameters &parameters,
                   CoroContext *ctx,
                   TraceView trace) -> RetCode {
        return worker_do(
            parameters, false /* is root */, true /* is tail */, ctx, trace);
    };
    auto middle = [](Parameters &parameters,
                     CoroContext *ctx,
                     TraceView trace) -> RetCode {
        return worker_do(
            parameters, false /* is root */, false /* is tail */, ctx, trace);
    };

    size_t lambda_nr_per_chain = lambda_nr / chain_nr;
    for (size_t chain_id = 0; chain_id < chain_nr; ++chain_id)
    {
        auto tid = patronus->get_thread_id();
        auto nid = patronus->get_node_id();
        CHECK_LT(chain_id, kCoroCnt);
        auto key = gen_coro_key(nid, tid, chain_id);
        auto offset = bench_locator(key);

        ParameterMap init_param;
        init_param["addr"].gaddr = GlobalAddress(0, offset);
        init_param["addr"].size = sizeof(uint64_t);

        lambda_t last_id{};
        lambda_t root_id{};
        for (size_t i = 0; i < lambda_nr_per_chain; ++i)
        {
            if (i == 0)
            {
                last_id = launcher.add_lambda(root,
                                              init_param,
                                              {} /* recv para from */,
                                              {} /* depend_on */,
                                              {} /* reloop to lambda */);
                root_id = last_id;
            }
            else
            {
                bool is_last = i + 1 == lambda_nr_per_chain;
                if (is_last)
                {
                    auto id = launcher.add_lambda(tail,
                                                  {} /* init param */,
                                                  last_id /* recv para from*/,
                                                  {last_id} /* depend_on */,
                                                  root_id /* reloop to */);
                    last_id = id;
                }
                else
                {
                    auto id = launcher.add_lambda(middle,
                                                  {} /* init param */,
                                                  last_id /* recv param from */,
                                                  {last_id} /* depend on */,
                                                  {} /* reloop to */);
                    last_id = id;
                }
            }
        }
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

    auto tid = patronus->get_thread_id();
    auto dir_id = tid % kServerThreadNr;
    // auto nid = patronus->get_node_id();
    auto server_nid = ::config::get_server_nids().front();

    serverless::CoroLauncher launcher(patronus,
                                      server_nid,
                                      dir_id,
                                      serverless_config,
                                      test_times,
                                      work_nr,
                                      0);

    register_lambda(patronus, launcher, kCoroCnt, kChainNr);

    launcher.launch();

    LOG(INFO) << "[detail] worker_do_nr: " << worker_do_nr_ << " from tid "
              << tid;
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
    serverless_configs.emplace_back(
        serverless::Config::get_mw("mw[nested]", false));
    serverless_configs.emplace_back(
        serverless::Config::get_mw("mw[step]", true));

    for (size_t thread_nr : {kMaxAppThread})
    {
        CHECK_LE(thread_nr, kMaxAppThread);

        for (size_t coro_nr : {32})
        {
            auto total_test_times = kTestTimePerThread * 4;
            for (const auto &serverless_config : serverless_configs)
            {
                auto configs = BenchConfigFactory::get_basic(
                    "chains", thread_nr, coro_nr, total_test_times);
                LOG_IF(INFO, is_master)
                    << "[config] serverless_config: " << serverless_config;
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
