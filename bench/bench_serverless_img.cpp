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

[[maybe_unused]] constexpr static size_t kCoroCnt = 32;  // max

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

// struct Meta
// {
//     GlobalAddress image_blob_gaddr;
//     size_t image_blob_size;
//     uint64_t checksum;
//     // size_t
// };
struct ImageData
{
    size_t size;
    char buffer[];
};

struct ImageMeta
{
    size_t columns;
    size_t rows;
    size_t scaled_columns;
    size_t scaled_rows;
    size_t file_size;
};

void register_lambdas(serverless::CoroLauncher &launcher,
                      GlobalAddress image_gaddr,
                      size_t image_size)
{
    auto extract_meta_data = [](Parameters &parameters,
                                CoroContext *ctx,
                                util::TraceView trace) -> RetCode {
        auto img_buffer = parameters.read("img_data", ctx, trace);
        const auto &img_data = *(ImageData *) img_buffer.buffer;
        Magick::Blob blob(img_data.buffer, img_data.size);
        Magick::Image image(blob);

        auto img_meta_buffer = parameters.get_buffer(sizeof(ImageMeta));
        auto &img_meta = *(ImageMeta *) img_meta_buffer.buffer;
        img_meta.columns = image.columns();
        img_meta.rows = image.rows();
        img_meta.scaled_columns = 0;
        img_meta.scaled_rows = 0;
        img_meta.file_size = image.fileSize();
        parameters.set_param("img_meta",
                             std::move(img_meta_buffer),
                             sizeof(ImageMeta),
                             ctx,
                             trace);

        parameters.put_rdma_buffer(std::move(img_buffer));
        trace.pin("done: extrace meta");
        return RC::kOk;
    };

    auto transfer_meta = [](Parameters &parameters,
                            CoroContext *ctx,
                            util::TraceView trace) -> RetCode {
        const auto &img_meta_buffer =
            parameters.get_param("img_meta", ctx, trace);
        auto &img_meta = *(ImageMeta *) img_meta_buffer.buffer;
        img_meta.scaled_columns = img_meta.columns / 10;
        img_meta.scaled_rows = img_meta.rows / 10;

        auto update_img_meta_buffer = parameters.get_buffer(sizeof(ImageMeta));
        memcpy(update_img_meta_buffer.buffer,
               img_meta_buffer.buffer,
               sizeof(ImageMeta));
        parameters.set_param("img_meta",
                             std::move(update_img_meta_buffer),
                             sizeof(ImageMeta),
                             ctx,
                             trace);
        trace.pin("done: transfer meta");
        return RC::kOk;
    };
    auto handler = [](Parameters &parameters,
                      CoroContext *ctx,
                      util::TraceView trace) -> RetCode {
        const auto &img_meta_buffer =
            parameters.get_param("img_meta", ctx, trace);
        const auto &img_meta = *(ImageMeta *) img_meta_buffer.buffer;
        DCHECK_GT(img_meta.file_size, 0);
        parameters.alloc("thumbnail", img_meta.file_size, ctx, trace);

        trace.pin("done: handler");
        return RC::kOk;
    };
    auto thumbnail = [](Parameters &parameters,
                        CoroContext *ctx,
                        util::TraceView trace) -> RetCode {
        const auto &img_meta_buffer = parameters.read("img_meta", ctx, trace);
        const auto &img_meta = *(ImageMeta *) img_meta_buffer.buffer;

        auto img_buffer = parameters.read("img_data", ctx, trace);
        const auto &img_data = *(ImageData *) img_buffer.buffer;
        Magick::Blob blob(img_data.buffer, img_data.size);
        Magick::Image image(blob);

        image.zoom({img_meta.scaled_columns, img_meta.scaled_rows});
        image.write(&blob);
        auto img_write_buffer = parameters.get_buffer(blob.length());
        memcpy(img_write_buffer.buffer, blob.data(), blob.length());
        parameters.write("thumbnail", std::move(img_write_buffer), ctx, trace)
            .expect(RC::kOk);

        parameters.put_rdma_buffer(std::move(img_buffer));

        trace.pin("done: thumbnail");
        return RC::kOk;
    };
    auto response_meta = [](Parameters &parameters,
                            CoroContext *ctx,
                            util::TraceView trace) -> RetCode {
        const auto &img_meta_buffer =
            parameters.get_param("img_meta", ctx, trace);
        auto response_buffer = parameters.get_buffer(sizeof(ImageMeta));
        memcpy(
            response_buffer.buffer, img_meta_buffer.buffer, sizeof(ImageMeta));
        parameters.set_param("response",
                             std::move(response_buffer),
                             sizeof(ImageMeta),
                             ctx,
                             trace);
        trace.pin("done: response");
        return RC::kOk;
    };

    ParameterMap init_param;
    init_param["img_data"].gaddr = image_gaddr;
    init_param["img_data"].size = image_size;

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
    [[maybe_unused]] auto response_meta_id =
        launcher.add_lambda(response_meta,
                            {} /* init param */,
                            thumbnail_id,
                            {thumbnail_id},
                            extract_meta_id);

    launcher.launch();
}

void bench_alloc_thread_coro(
    Patronus::pointer patronus,
    GlobalAddress img_gaddr,
    size_t img_size,
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

    register_lambdas(launcher, img_gaddr, img_size);

    launcher.launch();

    return;
}

void bench_template(const std::string &name,
                    Patronus::pointer patronus,
                    GlobalAddress img_gaddr,
                    size_t img_size,
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
        bench_alloc_thread_coro(patronus,
                                img_gaddr,
                                img_size,
                                lat_m,
                                is_master,
                                work_nr,
                                serverless_config,
                                conf);
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

    uint64_t blob_size = blob.length();
    auto *blob_addr = patronus->patronus_alloc(blob.length(), 0 /* hint */);
    memcpy(blob_addr, blob.data(), blob_size);
    auto blob_gaddr = patronus->to_exposed_gaddr(blob_addr);

    patronus->put("serverless:img_gaddr", blob_gaddr, 0ns);
    patronus->put("serverless:img_size", blob_size, 0ns);
    LOG(INFO) << "Puting to serverless:meta_gaddr: " << blob_gaddr
              << ", size: " << blob_size;
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

GlobalAddress g_img_gaddr;
uint64_t g_img_size;
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
        g_img_gaddr =
            patronus->get_object<GlobalAddress>("serverless:img_gaddr", 100ms);
        g_img_size =
            patronus->get_object<uint64_t>("serverless:img_size", 100ms);
    }
    bar.wait();

    bench_template(conf.name,
                   patronus,
                   g_img_gaddr,
                   g_img_size,
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
        serverless::Config::get_mw("mw[step]", true));
    serverless_configs.emplace_back(
        serverless::Config::get_mw("mw[nested]", false));

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
                auto configs = BenchConfigFactory::get_basic(
                    "diamond", thread_nr, coro_nr, total_test_times);
                LOG_IF(INFO, is_master) << "[config] " << serverless_config;
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
