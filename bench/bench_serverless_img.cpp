#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <queue>
#include <set>

#include "Common.h"
#include "Magick++.h"
#include "PerThread.h"
#include "Rdma.h"
#include "boost/thread/barrier.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "patronus/All.h"
#include "thirdparty/serverless/serverless.h"
#include "util/DataFrameF.h"
#include "util/Hexdump.hpp"
#include "util/IRdmaAdaptor.h"
#include "util/PerformanceReporter.h"
#include "util/Rand.h"
#include "util/TimeConv.h"
#include "util/Util.h"

using namespace util::literals;
using namespace patronus;
using namespace std::chrono_literals;
using namespace hmdf;

constexpr static size_t kClientThreadNr = kMaxAppThread;
constexpr static size_t kServerThreadNr = NR_DIRECTORY;
constexpr static size_t kTestTimePerThread = 5_K;

DEFINE_string(exec_meta, "", "The meta data of this execution");

constexpr static size_t kCoroCnt = 32;  // max

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

struct Meta
{
    GlobalAddress image_blob_gaddr;
    size_t image_blob_size;
    uint64_t checksum;
    // size_t
};

struct ImageProcessingPrv
{
    void *meta_buffer;
    void *image_blob;
};

void register_lambdas(serverless::CoroLauncher &launcher,
                      Patronus::pointer patronus,
                      GlobalAddress meta_gaddr)
{
    auto extract_meta_data = [patronus](const Parameters &input,
                                        Parameters &output,
                                        CoroContext *ctx) -> RetCode {
        auto server_nid = ::config::get_server_nids().front();
        auto tid = patronus->get_thread_id();
        auto dir_id = tid % kServerThreadNr;
        const auto &meta_param = input.find("meta")->second;
        auto ac_flag = (flag_t) AcquireRequestFlag::kNoGc |
                       (flag_t) AcquireRequestFlag::kNoBindPR;
        auto rlease = patronus->get_rlease(server_nid,
                                           dir_id,
                                           meta_param.gaddr,
                                           0 /* alloc */,
                                           meta_param.size,
                                           0ns,
                                           ac_flag,
                                           ctx);
        CHECK(rlease.success());

        output["meta"] = input.find("meta")->second;
        output["meta"].prv =
            (void *) patronus->get_rdma_buffer(sizeof(Meta)).buffer;
    };

    auto transfer_meta = [patronus](const Parameters &input,
                                    Parameters &output,
                                    CoroContext *ctx) -> RetCode {

    };
    auto handler = [patronus](const Parameters &input,
                              Parameters &output,
                              CoroContext *ctx) -> RetCode {

    };
    auto thumbnail = [patronus](const Parameters &input,
                                Parameters &output,
                                CoroContext *ctx) -> RetCode {

    };
    auto response_meta = [patronus](const Parameters &input,
                                    Parameters &output,
                                    CoroContext *ctx) -> RetCode {

    };

    Parameters init_param;
    init_param["meta"].gaddr = meta_gaddr;
    init_param["meta"].size = sizeof(Meta);
    auto extract_meta_id = launcher.add_lambda(extract_meta_data,
                                               init_param,
                                               {} /* recv from */,
                                               {} /* depend on */,
                                               {} /* reloop to */);
    auto transfer_meta_id = launcher.add_lambda(transfer_meta,
                                                {} /* init param */,
                                                extract_meta_id,
                                                {extract_meta_id},
                                                {} /* reloop */);
    auto handler_id = launcher.add_lambda(handler,
                                          {} /* init param */,
                                          transfer_meta_id,
                                          {transfer_meta_id},
                                          {} /* reloop */);
    auto thumbnail_id = launcher.add_lambda(thumbnail,
                                            {} /* init param */,
                                            handler_id,
                                            {handler_id},
                                            {} /* reloop */);
    auto response_meta_id = launcher.add_lambda(response_meta,
                                                {} /* init param */,
                                                thumbnail_id,
                                                {thumbnail_id},
                                                extract_meta_id);
    launcher.launch();
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

    serverless::CoroLauncher launcher(patronus, test_times, work_nr);
    register_lambdas(launcher, patronus, kCoroCnt, 4);

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

constexpr static const char *img_name = "view.thumbnail.jpeg";
void init_meta(Patronus::pointer patronus)
{
    auto img_path = artifacts_directory() / img_name;
    Magick::Image image(img_path);
    Magick::Blob blob;
    image.write(&blob);

    auto *meta_addr = patronus->patronus_alloc(sizeof(Meta), 0 /* hint */);
    auto *blob_addr = patronus->patronus_alloc(blob.length(), 0 /* hint */);
    memcpy(blob_addr, blob.data(), blob.length());
    auto &meta = *(Meta *) meta_addr;
    meta.image_blob_gaddr = patronus->to_exposed_gaddr(blob_addr);
    meta.image_blob_size = blob.length();
    meta.checksum = 0;
    meta.checksum = util::djb2_digest(&meta, sizeof(Meta));

    auto meta_gaddr = patronus->to_exposed_gaddr(meta_addr);
    patronus->put("serverless:meta_gaddr", meta_gaddr, 0ns);
    LOG(INFO) << "Puting to serverless:meta_gaddr: " << meta_gaddr
              << ", content: " << util::Hexdump(&meta, sizeof(meta));
}

void run_benchmark_server(Patronus::pointer patronus,
                          const BenchConfig &conf,
                          bool is_master,
                          uint64_t key)
{
    std::ignore = conf;

    init_meta(patronus);

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
                    "diamond", thread_nr, coro_nr, total_test_times);
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
