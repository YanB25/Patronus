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
constexpr static size_t kServerThreadNr = NR_DIRECTORY;

constexpr static size_t kTestTimePerThread = 1_K;

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
struct Comm
{
    uint64_t magic;
};

size_t worker_do_nr_{0};
RetCode worker_do(Patronus::pointer patronus,
                  Parameters &parameters,
                  bool root,
                  bool tail,
                  CoroContext *ctx)
{
    // worker_do_nr_++;
    // auto tid = patronus->get_thread_id();
    // auto dir_id = tid % kServerThreadNr;
    // auto server_nid = ::config::get_server_nids().front();
    std::ignore = patronus;
    std::ignore = parameters;
    std::ignore = root;
    std::ignore = tail;
    std::ignore = ctx;

    // Comm *comm;
    // if (root)
    // {
    //     comm = new Comm;
    // }
    // else
    // {
    //     comm = (Comm *) r.prv();
    // }

    if (root)
    {
        // auto ac_flag = (flag_t) AcquireRequestFlag::kNoGc;
        // auto wlease = patronus->get_wlease(server_nid,
        //                                    dir_id,
        //                                    r.gaddr,
        //                                    0 /* alloc hint */,
        //                                    r.size,
        //                                    0ns,
        //                                    ac_flag,
        //                                    ctx);
        // CHECK(wlease.success());
        // auto rdma_buf = patronus->get_rdma_buffer(sizeof(uint64_t));
        // auto magic = fast_pseudo_rand_int();
        // // LOG(INFO) << "[debug] !!! init magic to " << (void *) magic << "
        // at "
        // //           << r.gaddr;
        // *(uint64_t *) rdma_buf.buffer = magic;
        // patronus
        //     ->write(wlease,
        //             rdma_buf.buffer,
        //             sizeof(uint64_t),
        //             0 /* offset */,
        //             0 /* flag */,
        //             ctx)
        //     .expect(RC::kOk);
        // comm->magic = magic;

        // patronus->put_rdma_buffer(rdma_buf);
        // patronus->relinquish(wlease, 0 /* alloc hint */, 0 /* flag */, ctx);

        // parameters.write("init", )
    }

    // read, validate same
    {
        // auto ac_flag = (flag_t) AcquireRequestFlag::kNoGc;
        // auto rlease = patronus->get_rlease(server_nid,
        //                                    dir_id,
        //                                    r.gaddr,
        //                                    0 /* alloc hint */,
        //                                    r.size,
        //                                    0ns,
        //                                    ac_flag,
        //                                    ctx);
        // CHECK(rlease.success());
        // auto rdma_buf = patronus->get_rdma_buffer(sizeof(uint64_t));
        // memset(rdma_buf.buffer, 0, sizeof(uint64_t));
        // patronus
        //     ->read(rlease,
        //            rdma_buf.buffer,
        //            sizeof(uint64_t),
        //            0 /* offset */,
        //            0 /* flag */,
        //            ctx)
        //     .expect(RC::kOk);
        // std::atomic<uint64_t> *atomic_got =
        //     (std::atomic<uint64_t> *) rdma_buf.buffer;
        // uint64_t got = atomic_got->load();
        // CHECK_EQ(got, comm->magic);
        // patronus->put_rdma_buffer(rdma_buf);
        // patronus->relinquish(rlease, 0 /* hint */, 0 /* flag */, ctx);
    }

    // write to magic + 1
    {
        // auto ac_flag = (flag_t) AcquireRequestFlag::kNoGc;
        // auto wlease = patronus->get_wlease(server_nid,
        //                                    dir_id,
        //                                    r.gaddr,
        //                                    0 /* hint */,
        //                                    r.size,
        //                                    0ns,
        //                                    ac_flag,
        //                                    ctx);
        // CHECK(wlease.success());
        // auto rdma_buf = patronus->get_rdma_buffer(sizeof(uint64_t));
        // comm->magic = comm->magic + 1;
        // *(uint64_t *) rdma_buf.buffer = comm->magic;
        // patronus
        //     ->write(wlease,
        //             rdma_buf.buffer,
        //             sizeof(uint64_t),
        //             0 /* offset */,
        //             0 /* flag */,
        //             ctx)
        //     .expect(RC::kOk);
        // patronus->put_rdma_buffer(rdma_buf);
        // patronus->relinquish(wlease, 0 /* hint */, 0 /* flag */, ctx);
        // LOG(INFO) << "[debug] !!! updating magic to " << (void *)
        // (comm->magic)
        //           << " at " << r.gaddr;
    }

    // debug: reread, make sure we really write it successfully
    {
        // auto ac_flag = (flag_t) AcquireRequestFlag::kNoGc;
        // auto rlease = patronus->get_rlease(server_nid,
        //                                    dir_id,
        //                                    r.gaddr,
        //                                    0 /* alloc hint */,
        //                                    r.size,
        //                                    0ns,
        //                                    ac_flag,
        //                                    ctx);
        // CHECK(rlease.success());
        // auto rdma_buf = patronus->get_rdma_buffer(sizeof(uint64_t));
        // memset(rdma_buf.buffer, 0, sizeof(uint64_t));
        // patronus
        //     ->read(rlease,
        //            rdma_buf.buffer,
        //            sizeof(uint64_t),
        //            0 /* offset */,
        //            0 /* flag */,
        //            ctx)
        //     .expect(RC::kOk);
        // std::atomic<uint64_t> *atomic_got =
        //     (std::atomic<uint64_t> *) rdma_buf.buffer;
        // uint64_t got = atomic_got->load();
        // CHECK_EQ(got, comm->magic);
        // patronus->put_rdma_buffer(rdma_buf);
        // patronus->relinquish(rlease, 0 /* hint */, 0 /* flag */, ctx);
    }

    // tell the next lambda the magic
    {
        // output["addr"] = input.find("addr")->second;
        // output["addr"].prv = comm;
    }

    // if (tail)
    // {
    //     delete (Comm *) r.prv;
    // }
    return RC::kOk;
}

void bench_alloc_thread_coro(
    Patronus::pointer patronus,
    [[maybe_unused]] OnePassBucketMonitor<uint64_t> &lat_m,
    [[maybe_unused]] bool is_master,
    std::atomic<ssize_t> &work_nr,
    const BenchConfig &conf)
{
    auto test_times = conf.task_nr;

    auto tid = patronus->get_thread_id();
    auto dir_id = tid % kServerThreadNr;
    // auto nid = patronus->get_node_id();
    auto server_nid = ::config::get_server_nids().front();
    LOG(WARNING) << "TODO: use real config";
    serverless::Config config;

    serverless::CoroLauncher launcher(
        patronus, server_nid, dir_id, config, test_times, work_nr);

    auto root = [patronus](Parameters &parameters,
                           CoroContext *ctx) -> RetCode {
        return worker_do(
            patronus, parameters, true /* is root */, false /* is tail */, ctx);
    };
    auto tail = [patronus](Parameters &parameters,
                           CoroContext *ctx) -> RetCode {
        return worker_do(
            patronus, parameters, false /* is root */, true /* is tail */, ctx);
    };
    auto middle = [patronus](Parameters &parameters,
                             CoroContext *ctx) -> RetCode {
        return worker_do(patronus,
                         parameters,
                         false /* is root */,
                         false /* is tail */,
                         ctx);
    };

    {
        CHECK(false) << "TODO:";
        // one chain
        // Parameters init_para;
        // auto &addr = init_para["addr"];
        // auto key = gen_coro_key(nid, tid, 0);
        // auto offset = bench_locator(key);
        // addr.gaddr = GlobalAddress(0, offset);
        // addr.size = sizeof(uint64_t);

        // LOG(INFO) << "[debug] !! init_para: " << init_para;

        // lambda_t last_id{};
        // lambda_t root_id{};
        // size_t expect_lambda_nr = kCoroCnt / 2;
        // for (size_t i = 0; i < expect_lambda_nr; ++i)
        // {
        //     if (i == 0)
        //     {
        //         last_id = launcher.add_lambda(root,
        //                                       init_para,
        //                                       {} /* recv para from */,
        //                                       {} /* depend_on */,
        //                                       {} /* reloop to lambda */);
        //         root_id = last_id;
        //     }
        //     else
        //     {
        //         bool is_last = i + 1 == expect_lambda_nr;
        //         if (is_last)
        //         {
        //             auto id = launcher.add_lambda(tail,
        //                                           {} /* init param */,
        //                                           last_id /* recv para from
        //                                           */, {last_id} /* depend_on
        //                                           */, root_id /* reloop to
        //                                           */);
        //             last_id = id;
        //         }
        //         else
        //         {
        //             auto id = launcher.add_lambda(middle,
        //                                           {} /* init param */,
        //                                           last_id /* recv param from
        //                                           */, {last_id} /* depend on
        //                                           */,
        //                                           {} /* reloop to */);
        //             last_id = id;
        //         }
        //     }
        // }
    }

    {
        // another chain
        CHECK(false) << "TODO:";

        // Parameters init_para;
        // auto &addr = init_para["addr"];
        // auto key = gen_coro_key(nid, tid, 1);
        // auto offset = bench_locator(key);
        // addr.gaddr = GlobalAddress(0, offset);
        // addr.size = sizeof(uint64_t);

        // lambda_t last_id{};
        // lambda_t root_id{};
        // size_t expect_lambda_nr = kCoroCnt / 2;
        // for (size_t i = 0; i < expect_lambda_nr; ++i)
        // {
        //     if (i == 0)
        //     {
        //         last_id = launcher.add_lambda(root,
        //                                       init_para,
        //                                       {} /* recv para from */,
        //                                       {} /* depend_on */,
        //                                       {} /* reloop to lambda */);
        //         root_id = last_id;
        //     }
        //     else
        //     {
        //         bool is_last = i + 1 == expect_lambda_nr;
        //         if (is_last)
        //         {
        //             auto id = launcher.add_lambda(tail,
        //                                           {} /* init param */,
        //                                           last_id /* recv para from
        //                                           */, {last_id} /* depend_on
        //                                           */, root_id /* reloop to
        //                                           */);
        //             last_id = id;
        //         }
        //         else
        //         {
        //             auto id = launcher.add_lambda(middle,
        //                                           {} /* init param */,
        //                                           last_id /* recv param from
        //                                           */, {last_id} /* depend on
        //                                           */,
        //                                           {} /* reloop to */);
        //             last_id = id;
        //         }
        //     }
        // }
    }

    launcher.launch();

    LOG(INFO) << "[debug] !! worker_do_nr: " << worker_do_nr_ << " from tid "
              << tid;
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
        bench_alloc_thread_coro(patronus, lat_m, is_master, work_nr, conf);
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
               bool is_client,
               bool is_master)
{
    bar.wait();

    size_t key = 0;

    // for (size_t thread_nr : {32})
    for (size_t thread_nr : {1})
    {
        CHECK_LE(thread_nr, kMaxAppThread);
        // for (size_t coro_nr : {2, 4, 8, 16, 32})
        // for (size_t coro_nr : {1, 32})
        for (size_t coro_nr : {32})
        {
            auto total_test_times = kTestTimePerThread * 4;
            {
                auto configs = BenchConfigFactory::get_basic(
                    "two-chain", thread_nr, coro_nr, total_test_times);
                run_benchmark(
                    patronus, configs, bar, is_client, is_master, key);
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
