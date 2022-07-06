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

// constexpr static size_t kClientThreadNr = kMaxAppThread;
constexpr static size_t kClientThreadNr = 1;
constexpr static size_t kServerThreadNr = NR_DIRECTORY;
constexpr static size_t kTestTimePerThread = 100;

constexpr static const char *img_name = "view.thumbnail.jpeg";

DEFINE_string(exec_meta, "", "The meta data of this execution");

// [[maybe_unused]] constexpr static size_t kCoroCnt = 32;  // max
[[maybe_unused]] constexpr static size_t kCoroCnt = 1;  // max

struct Prv
{
    GlobalAddress img_gaddr;
    size_t img_size;
    ssize_t thread_remain_task;
    std::unique_ptr<Magick::Image> image;
    std::unique_ptr<Magick::Blob> blob;
};

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

struct ImageMeta
{
    size_t columns;
    size_t rows;
    size_t scaled_columns;
    size_t scaled_rows;
    size_t file_size;
};

void bench_alloc_thread_coro_worker(
    Patronus::pointer patronus,
    size_t coro_id,
    CoroYield &yield,
    CoroExecutionContextWith<kCoroCnt, Prv> &coro_comm,
    OnePassBucketMonitor<uint64_t> &lat_m,
    bool is_master,
    [[maybe_unused]] const BenchConfig &conf)
{
    auto tid = patronus->get_thread_id();
    auto dir_id = tid % kServerThreadNr;
    auto server_nid = ::config::get_server_nids().front();

    CoroContext ctx(tid, &yield, &coro_comm.master(), coro_id);

    size_t fail_nr = 0;
    size_t succ_nr = 0;

    ChronoTimer timer;
    ChronoTimer op_timer;
    auto &prv = coro_comm.get_private_data();
    while (prv.thread_remain_task > 0)
    {
        // bool succ = true;
        // VLOG(4) << "[coro] tid " << tid << " get_rlease. coro: " << ctx;
        if (is_master && coro_id == 0)
        {
            op_timer.pin();
        }
        auto ac_flag = (flag_t) AcquireRequestFlag::kNoGc |
                       (flag_t) AcquireRequestFlag::kNoBindPR;
        auto lease = patronus->get_rlease(server_nid,
                                          dir_id,
                                          prv.img_gaddr,
                                          0 /* alloc_hint */,
                                          prv.img_size,
                                          0ns,
                                          ac_flag,
                                          &ctx);
        CHECK(lease.success());
        auto img_rdma_buf = patronus->get_rdma_buffer(prv.img_size);
        patronus
            ->read(lease,
                   img_rdma_buf.buffer,
                   prv.img_size,
                   0 /* offset */,
                   0 /* flag */,
                   &ctx)
            .expect(RC::kOk);

        // char *buf = (char *) malloc(coro_comm.img_size);
        // memcpy(buf, img_rdma_buf.buffer, coro_comm.img_size);
        // LOG(ERROR) << "!!! buf: size: " << coro_comm.img_size << ", checksum:
        // "
        //            << (void *) util::djb2_digest(buf, coro_comm.img_size);
        // Magick::Blob blob(buf, coro_comm.img_size);

        // for (size_t i = 0; i < 10; ++i)
        // {
        // Magick::Image img(blob);
        // LOG(INFO) << "img: " << img.fileSize();
        // }

        // free(buf);

        patronus->put_rdma_buffer(std::move(img_rdma_buf));
        patronus->relinquish(lease, 0 /* hint */, 0 /* flag */, &ctx);

        if (is_master && coro_id == 0)
        {
            auto ns = op_timer.pin();
            lat_m.collect(ns);
        }
        prv.thread_remain_task--;
    }
    auto total_ns = timer.pin();

    coro_comm.worker_finished(coro_id);

    VLOG(1) << "[bench] tid " << tid << " got " << fail_nr
            << " failed lease. succ lease: " << succ_nr << " within "
            << total_ns << " ns. coro: " << ctx;
    ctx.yield_to_master();
    CHECK(false) << "yield back to me.";
}

void bench_alloc_thread_coro_master(
    Patronus::pointer patronus,
    CoroYield &yield,
    size_t test_times,
    std::atomic<ssize_t> &work_nr,
    CoroExecutionContextWith<kCoroCnt, Prv> &coro_comm,
    size_t coro_nr)
{
    auto tid = patronus->get_thread_id();

    CoroContext mctx(tid, &yield, coro_comm.workers());
    CHECK(mctx.is_master());

    LOG_IF(WARNING, test_times % 1000 != 0)
        << "test_times % 1000 != 0. Will introduce 1/1000 performance error "
           "per thread";

    ssize_t task_per_sync = test_times / 1000;
    LOG_IF(WARNING, task_per_sync <= (ssize_t) coro_nr)
        << "test_times < coro_nr.";

    task_per_sync =
        std::max(task_per_sync, ssize_t(coro_nr));  // at least coro_nr

    coro_comm.get_private_data().thread_remain_task = task_per_sync;
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
        if (coro_comm.get_private_data().thread_remain_task <=
            2 * ssize_t(coro_nr))
        {
            auto cur_task_nr = std::min(remain, task_per_sync);

            if (cur_task_nr >= 0)
            {
                remain =
                    work_nr.fetch_sub(cur_task_nr, std::memory_order_relaxed) -
                    cur_task_nr;
                if (remain >= 0)
                {
                    coro_comm.get_private_data().thread_remain_task +=
                        cur_task_nr;
                }
            }
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
            // bool finish_all = std::all_of(std::begin(coro_comm.finish_all),
            //                               std::end(coro_comm.finish_all),
            //                               [](bool i) { return i; });
            if (coro_comm.is_finished_all())
            {
                CHECK_LE(remain, 0);
                break;
            }
        }
    }
}

void bench_alloc_thread_coro(
    Patronus::pointer patronus,
    GlobalAddress img_gaddr,
    size_t img_size,
    const std::string &img_data,
    OnePassBucketMonitor<uint64_t> &lat_m,
    bool is_master,
    std::atomic<ssize_t> &work_nr,
    [[maybe_unused]] const serverless::Config &serverless_config,
    [[maybe_unused]] const BenchConfig &conf)
{
    auto coro_nr = conf.coro_nr;
    auto test_times = conf.task_nr;

    // auto server_nid = ::config::get_server_nids().front();
    auto tid = patronus->get_thread_id();
    auto dir_id = tid % kServerThreadNr;

    VLOG(1) << "[coro] client tid " << tid << " bind to core " << tid
            << ", using dir_id " << dir_id;

    CoroExecutionContextWith<kCoroCnt, Prv> comm;
    auto &prv = comm.get_private_data();
    prv.img_gaddr = img_gaddr;
    prv.img_size = img_size;
    prv.blob =
        std::make_unique<Magick::Blob>(img_data.data(), img_data.length());
    prv.image = std::make_unique<Magick::Image>(*prv.blob);

    for (size_t i = 0; i < coro_nr; ++i)
    {
        auto &worker = comm.worker(i);
        worker =
            CoroCall([patronus, &lat_m, coro_id = i, &comm, is_master, &conf](
                         CoroYield &yield) {
                bench_alloc_thread_coro_worker(
                    patronus, coro_id, yield, comm, lat_m, is_master, conf);
            });
    }

    auto &master = comm.master();
    master = CoroCall(
        [patronus, test_times, &work_nr, &comm, coro_nr](CoroYield &yield) {
            bench_alloc_thread_coro_master(
                patronus, yield, test_times, work_nr, comm, coro_nr);
        });

    master();
    return;
}

void bench_template(const std::string &name,
                    Patronus::pointer patronus,
                    GlobalAddress img_gaddr,
                    size_t img_size,
                    const std::string &img_data,
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
                                img_data,
                                lat_m,
                                is_master,
                                work_nr,
                                serverless_config,
                                conf);
    }

    bar.wait();
}

void init_meta(Patronus::pointer patronus)
{
    auto img_path = artifacts_directory() / img_name;
    Magick::Image image(img_path);
    Magick::Blob blob;
    image.write(&blob);

    uint64_t blob_size = blob.length();
    auto *blob_addr = patronus->patronus_alloc(blob.length(), 0 /* hint */);
    memcpy(blob_addr, blob.data(), blob_size);
    // LOG(ERROR) << "[debug] !! blob_size: " << blob_size << ", checksum: "
    //            << (void *) util::djb2_digest(blob.data(), blob.length());
    auto blob_gaddr = patronus->to_exposed_gaddr(blob_addr);

    patronus->put("serverless:img_gaddr", blob_gaddr, 0ns);
    patronus->put("serverless:img_size", blob_size, 0ns);
    LOG(INFO) << "Puting to serverless:meta_gaddr: " << blob_gaddr
              << ", size: " << blob_size;

    auto to_put = std::string((const char *) blob.data(), blob.length());
    LOG(INFO) << "[debug] !! putting img_blob with size " << to_put.size()
              << ". checksum: "
              << (void *) util::djb2_digest(to_put.data(), to_put.length());

    patronus->put("serverless:img_blob", to_put, 1ms);

    auto get = patronus->get("serverless:img_blob", 100ms);
    LOG(INFO) << "[debug] !! getting back img_blob with size " << get.size()
              << ". checksum: "
              << (void *) util::djb2_digest(get.data(), get.length());
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
std::string g_img_data;
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
        g_img_data = patronus->get("serverless:img_blob", 100ms);
        LOG(INFO) << "Getting serverless:img_gaddr " << g_img_gaddr
                  << ", serverless:img_size: " << g_img_size;
        LOG(INFO) << "[debug] !! getting img_blob with size: "
                  << g_img_data.length() << ", checksum: "
                  << (void *) util::djb2_digest(g_img_data.data(),
                                                g_img_data.length());
    }
    bar.wait();

    LOG(INFO) << "It is " << g_img_gaddr << ", " << g_img_size;
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
        // for (size_t coro_nr : {32})
        for (size_t coro_nr : {1})
        {
            CHECK_LE(coro_nr, kCoroCnt);
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
    config.client_rdma_buffer.block_class = {70_KB, 8_KB, 8_B};
    config.client_rdma_buffer.block_ratio = {0.2, 1 - 0.2 - 0.001, 0.001};

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
