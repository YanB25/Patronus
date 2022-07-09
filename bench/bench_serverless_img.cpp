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
using namespace util::pre;

constexpr static size_t kClientThreadNr = kMaxAppThread;
constexpr static size_t kServerThreadNr = NR_DIRECTORY;
constexpr static size_t kTestTimePerThread = 5_K;

constexpr static size_t kExpectCompressedImgSize = 69922;

[[maybe_unused]] constexpr static size_t V1 =
    ::config::verbose::kBenchReserve_1;
[[maybe_unused]] constexpr static size_t V2 =
    ::config::verbose::kBenchReserve_2;

std::vector<std::string> col_idx;
std::vector<size_t> col_x_thread_nr;
std::vector<size_t> col_x_coro_nr;
std::vector<size_t> col_test_op_nr;
std::vector<size_t> col_ns;

std::vector<std::string> col_lat_idx;
std::map<std::string, std::vector<uint64_t>> lat_data;

DEFINE_string(exec_meta, "", "The meta data of this execution");

[[maybe_unused]] constexpr static size_t kCoroCnt = 32;  // max

struct BenchConfig
{
    size_t thread_nr;
    size_t coro_nr;
    size_t task_nr;
    std::string name;
    bool report;

    bool should_report_lat() const
    {
        return thread_nr == kMaxAppThread && coro_nr == 1;
    }

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

struct ImageMeta
{
    size_t columns;
    size_t rows;
    size_t scaled_columns;
    size_t scaled_rows;
    size_t file_size;
};
inline std::ostream &operator<<(std::ostream &os, const ImageMeta &meta)
{
    os << "{ImageMeta columns: " << meta.columns << ", rows: " << meta.rows
       << ", scaled_columns: " << meta.scaled_columns
       << ", scaled_rows: " << meta.scaled_rows
       << ", file_size: " << meta.file_size << "}";
    return os;
}

void init_allocator(Patronus::pointer p,
                    size_t dir_id,
                    size_t thread_nr,
                    size_t expect_size)
{
    auto rh_buffer = p->get_user_reserved_buffer();
    auto thread_pool_size = rh_buffer.size / thread_nr;
    void *thread_pool_addr = rh_buffer.buffer + dir_id * thread_pool_size;

    mem::SlabAllocatorConfig slab_config;
    slab_config.block_class = {expect_size};
    slab_config.block_ratio = {1.0};
    slab_config.enable_recycle = true;
    auto allocator = mem::SlabAllocator::new_instance(
        thread_pool_addr, thread_pool_size, slab_config);
    p->reg_allocator(config::serverless::kAllocHint, allocator);
}

void register_lambdas(serverless::CoroLauncher &launcher,
                      size_t coro_nr,
                      GlobalAddress image_gaddr,
                      size_t image_size,
                      Magick::Image image)
{
    auto extract_meta_data = [image](Parameters &parameters,
                                     CoroContext *ctx,
                                     util::TraceView trace) -> RetCode {
        auto [img_buffer, img_size] = parameters.read("img_data", ctx, trace);
        LOG_IF(WARNING, img_size >= 32_MB)
            << "** Too large. Is it really correct? got: " << img_size;
        DCHECK_GE(img_buffer.size, img_size);

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
        DVLOG(V2) << "l1 img_meta: " << img_meta;

        parameters.put_rdma_buffer(std::move(img_buffer));
        trace.pin("done: extrace meta");
        return RC::kOk;
    };

    auto transfer_meta = [](Parameters &parameters,
                            CoroContext *ctx,
                            util::TraceView trace) -> RetCode {
        auto [img_meta_buffer, img_meta_size] =
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

        DVLOG(V2) << "l2 img_meta: " << img_meta;
        trace.pin("done: transfer meta");
        return RC::kOk;
    };
    auto handler = [](Parameters &parameters,
                      CoroContext *ctx,
                      util::TraceView trace) -> RetCode {
        auto [img_meta_buffer, img_meta_size] =
            parameters.get_param("img_meta", ctx, trace);
        const auto &img_meta = *(ImageMeta *) img_meta_buffer.buffer;
        DCHECK_GT(img_meta.file_size, 0);
        parameters.alloc("thumbnail", img_meta.file_size, ctx, trace);

        DVLOG(V2) << "l3 allocated with size " << img_meta.file_size;
        trace.pin("done: handler");
        return RC::kOk;
    };
    auto thumbnail = [image](Parameters &parameters,
                             CoroContext *ctx,
                             util::TraceView trace) mutable -> RetCode {
        auto [img_meta_buffer, img_meta_size] =
            parameters.get_param("img_meta", ctx, trace);
        const auto &img_meta = *(ImageMeta *) img_meta_buffer.buffer;

        std::ignore = img_meta;
        std::ignore = image;

        auto [img_buffer, img_size] = parameters.read("img_data", ctx, trace);
        DCHECK_GE(img_buffer.size, img_size);

        trace.pin("img cpy");
        CHECK_GT(img_meta.scaled_columns, 0);
        CHECK_GT(img_meta.scaled_rows, 0);
        DVLOG(V2) << "l4 zooming to " << img_meta.scaled_columns << ", "
                  << img_meta.scaled_rows;
        image.zoom({img_meta.scaled_columns, img_meta.scaled_rows});
        trace.pin("img zoom");
        Magick::Blob blob;
        image.write(&blob);
        trace.pin("img dump");

        auto img_write_buffer = parameters.get_buffer(blob.length());
        memcpy(img_write_buffer.buffer, blob.data(), blob.length());
        parameters
            .write("thumbnail",
                   std::move(img_write_buffer),
                   blob.length(),
                   ctx,
                   trace)
            .expect(RC::kOk);

        parameters.put_rdma_buffer(std::move(img_buffer));

        DVLOG(V2) << "l4 write thumbnail with size: " << image.fileSize();

        trace.pin("done: thumbnail");
        return RC::kOk;
    };
    auto response_meta = [](Parameters &parameters,
                            CoroContext *ctx,
                            util::TraceView trace) -> RetCode {
        auto [img_meta_buffer, img_meta_size] =
            parameters.get_param("img_meta", ctx, trace);
        auto response_buffer = parameters.get_buffer(sizeof(ImageMeta));
        memcpy(
            response_buffer.buffer, img_meta_buffer.buffer, sizeof(ImageMeta));
        parameters.set_param("response",
                             std::move(response_buffer),
                             sizeof(ImageMeta),
                             ctx,
                             trace);
        DVLOG(V2) << "l5 response.";
        trace.pin("done: response");
        return RC::kOk;
    };

    ParameterMap init_param;
    init_param["img_data"].gaddr = image_gaddr;
    init_param["img_data"].size = image_size;

    size_t lambda_nr = 0;
    constexpr static size_t kLambdaNrPerChain = 5;
    while (lambda_nr + kLambdaNrPerChain <= coro_nr)
    {
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
        lambda_nr += kLambdaNrPerChain;
    }
}

void reg_result(const std::string &name,
                const BenchConfig &bench_conf,
                uint64_t ns)
{
    col_idx.push_back(name);
    col_x_thread_nr.push_back(bench_conf.thread_nr);
    col_x_coro_nr.push_back(bench_conf.coro_nr);
    col_test_op_nr.push_back(bench_conf.task_nr);
    col_ns.push_back(ns);
}

void reg_lat_result(const std::string &name,
                    const OnePassBucketMonitor<uint64_t> &lat_m,
                    [[maybe_unused]] const BenchConfig &bench_conf)
{
    if (col_lat_idx.empty())
    {
        col_lat_idx.push_back("lat_min(avg)");
        col_lat_idx.push_back("lat_p5(avg)");
        col_lat_idx.push_back("lat_p9(avg)");
        col_lat_idx.push_back("lat_p99(avg)");
    }
    if (likely(lat_data[name].empty()))
    {
        lat_data[name].push_back(lat_m.min());
        lat_data[name].push_back(lat_m.percentile(0.5));
        lat_data[name].push_back(lat_m.percentile(0.9));
        lat_data[name].push_back(lat_m.percentile(0.99));
    }
    else
    {
        LOG(WARNING) << "** Recording latency for `" << name
                     << "` duplicated. Skip.";
    }
}

void bench_alloc_thread_coro(
    Patronus::pointer patronus,
    boost::barrier &bar,
    GlobalAddress img_gaddr,
    size_t img_size,
    Magick::Image image,
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
    size_t thread_nr = conf.thread_nr;

    std::unique_ptr<serverless::CoroLauncher> launcher;
    if (tid < thread_nr)
    {
        launcher = std::make_unique<serverless::CoroLauncher>(patronus,
                                                              server_nid,
                                                              dir_id,
                                                              serverless_config,
                                                              test_times,
                                                              work_nr,
                                                              0);
        // 20 * 1.0 / kTestTimePerThread);
        register_lambdas(*launcher, conf.coro_nr, img_gaddr, img_size, image);
    }

    bar.wait();
    ChronoTimer timer;
    if (launcher)
    {
        launcher->launch();
    }
    bar.wait();
    auto ns = timer.pin();

    if (is_master)
    {
        auto report_name = conf.name + "-" + serverless_config.name;
        reg_result(report_name, conf, ns);
        if (conf.should_report_lat())
        {
            reg_lat_result(report_name, lat_m, conf);
        }
    }

    return;
}

void bench_template(const std::string &name,
                    Patronus::pointer patronus,
                    GlobalAddress img_gaddr,
                    size_t img_size,
                    Magick::Image img_data,
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

    // auto tid = patronus->get_thread_id();

    auto min = util::time::to_ns(0ns);
    auto max = util::time::to_ns(10ms);
    auto range = util::time::to_ns(1us);
    OnePassBucketMonitor lat_m(min, max, range);

    bench_alloc_thread_coro(patronus,
                            bar,
                            img_gaddr,
                            img_size,
                            img_data,
                            lat_m,
                            is_master,
                            work_nr,
                            serverless_config,
                            conf);

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
    LOG(ERROR) << "[debug] !! blob_size: " << blob_size << ", checksum: "
               << (void *) util::djb2_digest(blob.data(), blob.length());
    auto blob_gaddr = patronus->to_exposed_gaddr(blob_addr);

    patronus->put("serverless:img_gaddr", blob_gaddr, 0ns);
    patronus->put("serverless:img_size", blob_size, 0ns);
    auto img_blob = std::string((const char *) blob.data(), blob.length());
    patronus->put("serverless:img_blob", img_blob, 1ms);
    LOG(INFO) << "Puting to serverless:meta_gaddr: " << blob_gaddr
              << ", size: " << blob_size << ", blob checksum: "
              << util::djb2_digest(img_blob.data(), img_blob.length());
}
void run_benchmark_server(Patronus::pointer patronus,
                          const BenchConfig &conf,
                          bool is_master,
                          boost::barrier &bar,
                          uint64_t key)
{
    auto tid = patronus->get_thread_id();
    auto dir_id = tid;
    std::ignore = conf;

    init_meta(patronus);

    // Make sure kExpectCompressedImgSize large enough
    init_allocator(patronus, dir_id, kServerThreadNr, kExpectCompressedImgSize);

    if (is_master)
    {
        patronus->finished(key);
    }

    bar.wait();
    if (is_master)
    {
        patronus->keeper_barrier("server_ready-" + std::to_string(key), 100ms);
    }

    LOG(INFO) << "[coro] server thread tid " << tid;

    LOG(INFO) << "[bench] BENCH: " << conf.name
              << ", thread_nr: " << conf.thread_nr
              << ", coro_nr: " << conf.coro_nr << ", report: " << conf.report;

    patronus->server_serve(key);
}
std::atomic<ssize_t> shared_task_nr;

GlobalAddress g_img_gaddr;
uint64_t g_img_size;
Magick::Image g_img_data;
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
        auto img_data = patronus->get("serverless:img_blob", 100ms);
        Magick::Blob blob(img_data.data(), img_data.size());
        g_img_data.read(blob);
        LOG(INFO) << "Getting serverless:img_gaddr " << g_img_gaddr
                  << ", serverless:img_size: " << g_img_size << ", checksum: "
                  << util::djb2_digest(img_data.data(), img_data.length());

        patronus->keeper_barrier("server_ready-" + std::to_string(key), 100ms);
    }

    bar.wait();

    // LOG(INFO) << "It is " << g_img_gaddr << ", " << g_img_size;
    bench_template(conf.name,
                   patronus,
                   g_img_gaddr,
                   g_img_size,
                   g_img_data,
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
            run_benchmark_server(patronus, conf, is_master, bar, key);
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
        serverless::Config::get_unprot("unprot[step]", true));
    serverless_configs.emplace_back(
        serverless::Config::get_unprot("unprot[nested]", false));
    serverless_configs.emplace_back(
        serverless::Config::get_mw("mw[step]", true));
    serverless_configs.emplace_back(
        serverless::Config::get_mw("mw[nested]", false));
    // serverless_configs.emplace_back(
    //     serverless::Config::get_mr("mr[step]", true));
    // serverless_configs.emplace_back(
    //     serverless::Config::get_mr("mr[nested]", false));
    // serverless_configs.emplace_back(
    //     serverless::Config::get_rpc("rpc[step]", true));
    // serverless_configs.emplace_back(
    //     serverless::Config::get_rpc("rpc[nested]", false));

    for (size_t thread_nr : {1, 4, 8, 32})
    // for (size_t thread_nr : {32})
    {
        CHECK_LE(thread_nr, kMaxAppThread);
        // for (size_t coro_nr : {2, 4, 8, 16, 32})
        // for (size_t coro_nr : {1, 32})
        for (size_t coro_nr : {5})
        {
            auto total_test_times =
                kTestTimePerThread * std::min(size_t(4), thread_nr);
            for (const auto &serverless_config : serverless_configs)
            {
                auto configs = BenchConfigFactory::get_basic(
                    "img",
                    thread_nr,
                    coro_nr,
                    total_test_times * serverless_config.scale_factor);
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
    config.reserved_buffer_size = 1_GB;
    config.lease_buffer_size = (kDSMCacheSize - 1_GB) / 2;
    config.alloc_buffer_size = (kDSMCacheSize - 1_GB) / 2;
    config.client_rdma_buffer.block_class = {70_KB, 8_KB, 8_B};
    config.client_rdma_buffer.block_ratio = {0.1, 1 - 0.1 - 0.001, 0.001};

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
        df.load_column<size_t>("test_nr(total)", std::move(col_test_op_nr));
        df.load_column<size_t>("test_ns(total)", std::move(col_ns));

        auto div_f = gen_F_div<size_t, size_t, double>();
        auto div_f2 = gen_F_div<double, size_t, double>();
        auto ops_f = gen_F_ops<size_t, size_t, double>();
        auto mul_f = gen_F_mul<double, size_t, double>();
        auto mul_f2 = gen_F_mul<size_t, size_t, size_t>();
        df.consolidate<size_t, size_t, size_t>(
            "x_thread_nr", "x_coro_nr", "client_nr(effective)", mul_f2, false);
        df.consolidate<size_t, size_t, size_t>("test_ns(total)",
                                               "client_nr(effective)",
                                               "test_ns(effective)",
                                               mul_f2,
                                               false);
        df.consolidate<size_t, size_t, double>(
            "test_ns(total)", "test_nr(total)", "lat(div)", div_f, false);
        df.consolidate<size_t, size_t, double>(
            "test_ns(effective)", "test_nr(total)", "lat(avg)", div_f, false);
        df.consolidate<size_t, size_t, double>(
            "test_nr(total)", "test_ns(total)", "ops(total)", ops_f, false);

        auto client_nr = ::config::get_client_nids().size();
        auto replace_mul_f = gen_replace_F_mul<double>(client_nr);
        df.load_column<double>("ops(cluster)",
                               df.get_column<double>("ops(total)"));
        df.replace<double>("ops(cluster)", replace_mul_f);

        df.consolidate<double, size_t, double>(
            "ops(total)", "x_thread_nr", "ops(thread)", div_f2, false);
        df.consolidate<double, size_t, double>(
            "ops(thread)", "x_coro_nr", "ops(coro)", div_f2, false);

        auto filename = binary_to_csv_filename(argv[0], FLAGS_exec_meta);
        df.write<std::ostream, std::string, size_t, double>(std::cout,
                                                            io_format::csv2);
        df.write<std::string, size_t, double>(filename.c_str(),
                                              io_format::csv2);
    }

    {
        StrDataFrame df;
        df.load_index(std::move(col_lat_idx));
        for (auto &[name, vec] : lat_data)
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
    LOG(INFO) << "Finished.";
}
