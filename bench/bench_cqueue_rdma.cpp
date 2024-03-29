#include <algorithm>
#include <random>
#include <set>

#include "Common.h"
#include "boost/thread/barrier.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "patronus/Patronus.h"
#include "patronus/RdmaAdaptor.h"
#include "patronus/memory/direct_allocator.h"
#include "patronus/memory/mr_allocator.h"
#include "thirdparty/concurrent-queue/conf.h"
#include "thirdparty/concurrent-queue/queue.h"
#include "thirdparty/concurrent-queue/queue_handle.h"
#include "util/BenchRand.h"
#include "util/DataFrameF.h"
#include "util/Rand.h"
#include "util/TimeConv.h"

using namespace patronus::cqueue;
using namespace util::literals;
using namespace patronus;
using namespace hmdf;

constexpr static size_t kServerThreadNr = NR_DIRECTORY;
constexpr static size_t kClientThreadNr = 32;
constexpr static size_t kMaxCoroNr = 16;

constexpr static size_t kEntryNrPerBlock = 1024;
// constexpr static size_t kEntryNrPerBlock = 64;

DEFINE_string(exec_meta, "", "The meta data of this execution");

std::vector<std::string> col_idx;
std::vector<size_t> col_x_queue_nr;
std::vector<size_t> col_x_entry_per_block;
std::vector<size_t> col_x_thread_nr;
std::vector<size_t> col_x_coro_nr;
std::vector<size_t> col_test_nr;
std::vector<size_t> col_ns;

std::vector<std::string> col_lat_idx;
std::vector<uint64_t> col_lat_min;
std::vector<uint64_t> col_lat_p5;
std::vector<uint64_t> col_lat_p9;
std::vector<uint64_t> col_lat_p99;
std::unordered_map<std::string, std::vector<uint64_t>> lat_data;

uint64_t calculate_client_id(uint64_t nid, uint64_t tid, uint64_t coro_id)
{
    return nid * kMaxAppThread * kMaxCoroNr + tid * kMaxCoroNr + coro_id;
}

struct Object
{
    uint64_t nid;
    uint64_t tid;
    uint64_t coro_id;
    uint64_t magic;
};

struct BenchConfig
{
    std::string name;
    size_t queue_nr{1};
    size_t entry_per_block{0};
    size_t thread_nr{1};
    size_t coro_nr{1};
    size_t test_nr{0};
    bool should_report{false};

    bool should_report_lat() const
    {
        return should_report && thread_nr == kClientThreadNr && coro_nr == 1;
    }

    void validate() const
    {
        CHECK_GT(queue_nr, 0) << "** should have at least one queue";
    }
    size_t cluster_client_nr() const
    {
        return local_client_nr() * ::config::get_client_nids().size();
    }
    size_t local_client_nr() const
    {
        return thread_nr * coro_nr;
    }
    std::string conf_name() const
    {
        return name;
    }
    BenchConfig clone() const
    {
        BenchConfig ret;
        ret.name = name;
        ret.thread_nr = thread_nr;
        ret.coro_nr = coro_nr;
        ret.test_nr = test_nr;
        ret.should_report = should_report;
        ret.queue_nr = queue_nr;
        return ret;
    }
    static BenchConfig get_conf(const std::string &name,
                                size_t test_nr,
                                size_t thread_nr,
                                size_t coro_nr,
                                size_t queue_nr,
                                size_t entry_per_block)
    {
        BenchConfig conf;
        conf.name = name;
        conf.test_nr = test_nr;
        conf.thread_nr = thread_nr;
        conf.coro_nr = coro_nr;
        conf.test_nr = test_nr;
        conf.queue_nr = queue_nr;
        conf.should_report = true;
        conf.entry_per_block = entry_per_block;
        conf.validate();
        return conf;
    }
};
std::ostream &operator<<(std::ostream &os, const BenchConfig &conf)
{
    os << "{conf: name: " << conf.name << ", queue_nr: " << conf.queue_nr
       << ", entry_per_block: " << conf.entry_per_block
       << ", thread_nr: " << conf.thread_nr << ", coro_nr: " << conf.coro_nr
       << "(local_client_nr: " << conf.local_client_nr()
       << ", cluster: " << conf.cluster_client_nr()
       << "), test_nr: " << conf.test_nr
       << ", should_report: " << conf.should_report << "}";
    return os;
}

using QueueHandleT = QueueHandle<Object, kEntryNrPerBlock>;
using QueueT = Queue<Object, kEntryNrPerBlock>;

typename QueueHandleT::pointer gen_handle(Patronus::pointer p,
                                          size_t dir_id,
                                          const QueueHandleConfig &conf,
                                          GlobalAddress meta_gaddr,
                                          CoroContext *ctx)
{
    auto server_nid = ::config::get_server_nids().front();

    DVLOG(1) << "Generating queue handle with gaddr " << meta_gaddr;

    auto rdma_adpt = patronus::RdmaAdaptor::new_instance(
        server_nid, dir_id, p, conf.bypass_prot, conf.use_two_sided, ctx);

    auto handle =
        QueueHandleT::new_instance(server_nid, meta_gaddr, rdma_adpt, conf);

    return handle;
}

struct AdditionalCoroCtx
{
    ssize_t thread_remain_task{0};
    size_t put_nr{0};
    size_t put_succ_nr{0};
    size_t get_nr{0};
    size_t get_succ_nr{0};
    size_t rdma_protection_nr{0};
};

void reg_result(const std::string &name,
                const BenchConfig &bench_conf,
                [[maybe_unused]] const AdditionalCoroCtx &prv,
                uint64_t ns)
{
    col_idx.push_back(name);
    col_x_thread_nr.push_back(bench_conf.thread_nr);
    col_x_coro_nr.push_back(bench_conf.coro_nr);
    col_x_queue_nr.push_back(bench_conf.queue_nr);
    col_x_entry_per_block.push_back(bench_conf.entry_per_block);
    col_ns.push_back(ns);
    col_test_nr.push_back(bench_conf.test_nr);
}

void reg_latency(const std::string &name,
                 const OnePassBucketMonitor<uint64_t> &lat_m)
{
    if (col_lat_idx.empty())
    {
        col_lat_idx.push_back("lat_min");
        col_lat_idx.push_back("lat_p5");
        col_lat_idx.push_back("lat_p9");
        col_lat_idx.push_back("lat_p99");
    }
    if (unlikely(!lat_data[name].empty()))
    {
        LOG(WARNING) << "[bench] Skip recording latency because duplicated.";
        return;
    }
    lat_data[name].push_back(lat_m.min());
    lat_data[name].push_back(lat_m.percentile(0.5));
    lat_data[name].push_back(lat_m.percentile(0.9));
    lat_data[name].push_back(lat_m.percentile(0.99));

    CHECK_EQ(lat_data[name].size(), col_lat_idx.size())
        << "** duplicated test detected.";
}

typename QueueT::pointer gen_queue(Patronus::pointer p,
                                   size_t id,  // base 0
                                   const QueueConfig &config)
{
    auto allocator = p->get_allocator(0 /* default hint */);

    auto server_rdma_adpt = patronus::RdmaAdaptor::new_instance(p);
    auto queue = QueueT::new_instance(server_rdma_adpt, allocator, config);

    auto meta_gaddr = queue->meta_gaddr();
    p->put("race:meta_gaddr[" + std::to_string(id) + "]", meta_gaddr, 0ns);
    LOG(INFO) << "Puting to race:meta_gaddr with " << meta_gaddr;

    return queue;
}

void test_basic_client_worker(
    Patronus::pointer p,
    size_t coro_id,
    CoroYield &yield,
    [[maybe_unused]] const BenchConfig &bench_conf,
    const QueueHandleConfig &handle_conf,
    std::vector<GlobalAddress> meta_gaddrs,
    CoroExecutionContextWith<kMaxCoroNr, AdditionalCoroCtx> &ex,
    OnePassBucketMonitor<uint64_t> &lat_m)
{
    auto tid = p->get_thread_id();
    auto dir_id = tid % kServerThreadNr;
    auto use_queue_id = tid % bench_conf.queue_nr;
    CoroContext ctx(tid, &yield, &ex.master(), coro_id);

    CHECK_EQ(meta_gaddrs.size(), bench_conf.queue_nr);
    CHECK_LT(use_queue_id, meta_gaddrs.size());
    auto cur_meta_gaddr = meta_gaddrs[use_queue_id];
    auto handle = gen_handle(p, dir_id, handle_conf, cur_meta_gaddr, &ctx);

    size_t put_succ_nr = 0;
    size_t put_retry_nr = 0;
    size_t get_succ_nr = 0;
    size_t get_nomem_nr = 0;
    size_t rdma_protection_nr = 0;
    size_t executed_nr = 0;

    ChronoTimer timer;

    ChronoTimer op_timer;
    bool should_report_latency = (tid == 0 && coro_id == 0);

    auto value = Object{};
    // util::TraceManager tm(tid == 0 && coro_id == 0 ? 1.0 / 10_K : 0);
    while (ex.get_private_data().thread_remain_task > 0)
    {
        // auto trace = tm.trace("push");
        if (should_report_latency)
        {
            op_timer.pin();
        }
        // auto rc = handle->lf_push_back(value, trace);
        auto rc = handle->lf_push_back(value);
        if (rc == kOk)
        {
            put_succ_nr++;
        }
        else
        {
            CHECK_EQ(rc, RC::kRetry);
            put_retry_nr++;
        }

        if (should_report_latency)
        {
            lat_m.collect(timer.pin());
        }
        executed_nr++;
        ex.get_private_data().thread_remain_task--;
        // LOG_IF(INFO, trace.enabled()) << trace;
    }
    auto ns = timer.pin();

    auto &comm = ex.get_private_data();
    comm.put_succ_nr += put_succ_nr;
    comm.put_nr += put_succ_nr + put_retry_nr;
    comm.get_succ_nr += get_succ_nr;
    comm.get_nr += get_succ_nr + get_nomem_nr;
    comm.rdma_protection_nr += rdma_protection_nr;

    LOG(INFO) << "[bench] put: succ: " << put_succ_nr
              << ", retry: " << put_retry_nr << ", get: succ: " << get_succ_nr
              << ", not found: " << get_nomem_nr
              << ", rdma_protection_nr: " << rdma_protection_nr
              << ". executed: " << executed_nr << ", take: " << ns
              << " ns. ctx: " << ctx;

    handle.reset();

    ex.worker_finished(coro_id);
    ctx.yield_to_master();
}

void test_basic_client_master(
    Patronus::pointer p,
    CoroYield &yield,
    size_t test_nr,
    std::atomic<ssize_t> &atm_task_nr,
    size_t coro_nr,
    CoroExecutionContextWith<kMaxCoroNr, AdditionalCoroCtx> &ex)
{
    auto tid = p->get_thread_id();

    CoroContext mctx(tid, &yield, ex.workers());
    CHECK(mctx.is_master());

    ssize_t task_per_sync = test_nr / 100;
    LOG_IF(WARNING, task_per_sync <= (ssize_t) coro_nr);
    task_per_sync = std::max(task_per_sync, ssize_t(coro_nr));
    ssize_t remain =
        atm_task_nr.fetch_sub(task_per_sync, std::memory_order_relaxed) -
        task_per_sync;
    ex.get_private_data().thread_remain_task = task_per_sync;

    for (size_t i = coro_nr; i < kMaxCoroNr; ++i)
    {
        ex.worker_finished(i);
    }

    for (size_t i = 0; i < coro_nr; ++i)
    {
        mctx.yield_to_worker(i);
    }

    coro_t coro_buf[2 * kMaxCoroNr];
    while (true)
    {
        if ((ssize_t) ex.get_private_data().thread_remain_task <=
            2 * ssize_t(coro_nr))
        {
            // refill the thread_remain_task
            auto cur_task_nr = std::min(remain, task_per_sync);
            if (cur_task_nr >= 0)
            {
                remain = atm_task_nr.fetch_sub(cur_task_nr,
                                               std::memory_order_relaxed) -
                         cur_task_nr;
                if (remain >= 0)
                {
                    ex.get_private_data().thread_remain_task += cur_task_nr;
                }
            }
        }
        auto nr = p->try_get_client_continue_coros(coro_buf, 2 * kMaxCoroNr);
        for (size_t i = 0; i < nr; ++i)
        {
            auto coro_id = coro_buf[i];
            // DVLOG(1) << "[bench] yielding due to CQE: " << (int) coro_id;
            mctx.yield_to_worker(coro_id);
        }

        if (remain <= 0)
        {
            if (ex.is_finished_all())
            {
                CHECK_LE(remain, 0);
                break;
            }
        }
    }
}

std::atomic<ssize_t> g_total_test_nr;
std::vector<GlobalAddress> g_meta_gaddrs;

void benchmark_client(Patronus::pointer p,
                      boost::barrier &bar,
                      bool is_master,
                      const BenchConfig &bench_conf,
                      const QueueHandleConfig &handle_conf,
                      uint64_t key)
{
    auto coro_nr = bench_conf.coro_nr;
    auto thread_nr = bench_conf.thread_nr;
    bool first_enter = true;
    bool server_should_leave = true;
    CHECK_LE(coro_nr, kMaxCoroNr);
    size_t actual_test_nr = bench_conf.test_nr;

    auto tid = p->get_thread_id();
    // auto nid = p->get_node_id();
    if (is_master)
    {
        // init here by master
        g_total_test_nr = actual_test_nr;
        if (first_enter)
        {
            p->keeper_barrier("server_ready-" + std::to_string(key), 100ms);
            // fetch meta_gaddr here by master thread
            // because it may be slow

            g_meta_gaddrs.clear();
            g_meta_gaddrs.resize(bench_conf.queue_nr);
            for (size_t i = 0; i < bench_conf.queue_nr; ++i)
            {
                g_meta_gaddrs[i] = p->get_object<GlobalAddress>(
                    "race:meta_gaddr[" + std::to_string(i) + "]", 1ms);
            }
        }
    }
    bar.wait();

    auto min = util::time::to_ns(0ns);
    auto max = util::time::to_ns(1ms);
    auto rng = util::time::to_ns(1us);
    OnePassBucketMonitor lat_m(min, max, rng);

    ChronoTimer timer;
    CoroExecutionContextWith<kMaxCoroNr, AdditionalCoroCtx> ex;
    bool should_enter = tid < thread_nr;

    if (should_enter)
    {
        ex.get_private_data().thread_remain_task = 0;
        for (size_t i = coro_nr; i < kMaxCoroNr; ++i)
        {
            // no that coro, so directly finished.
            ex.worker_finished(i);
        }
        for (size_t i = 0; i < coro_nr; ++i)
        {
            ex.worker(i) = CoroCall(
                [p, coro_id = i, &bench_conf, &handle_conf, &ex, &lat_m](
                    CoroYield &yield) {
                    test_basic_client_worker(p,
                                             coro_id,
                                             yield,
                                             bench_conf,
                                             handle_conf,
                                             g_meta_gaddrs,
                                             ex,
                                             lat_m);
                });
        }
        auto &master = ex.master();
        master = CoroCall([p, &ex, actual_test_nr = actual_test_nr, coro_nr](
                              CoroYield &yield) {
            test_basic_client_master(
                p, yield, actual_test_nr, g_total_test_nr, coro_nr, ex);
        });

        master();
    }

    bar.wait();
    auto ns = timer.pin();

    double ops = 1e9 * actual_test_nr / ns;
    double avg_ns = 1.0 * ns / actual_test_nr;
    LOG_IF(INFO, is_master)
        << "[bench] total op: " << actual_test_nr << ", ns: " << ns
        << ", ops: " << ops << ", avg " << avg_ns << " ns";

    if (is_master && server_should_leave)
    {
        LOG(INFO) << "p->finished(" << key << ")";
        p->finished(key);
    }

    auto report_name =
        bench_conf.conf_name() + "[" + handle_conf.conf_name() + "]";
    if (is_master && bench_conf.should_report)
    {
        reg_result(report_name, bench_conf, ex.get_private_data(), ns);

        if (bench_conf.should_report_lat())
        {
            reg_latency(report_name, lat_m);
        }
    }
}

void benchmark_server(Patronus::pointer p,
                      boost::barrier &bar,
                      bool is_master,
                      const std::vector<BenchConfig> &confs,
                      const QueueConfig &queue_config,
                      uint64_t key)
{
    auto thread_nr = confs[0].thread_nr;
    auto queue_nr = confs.front().queue_nr;
    for (const auto &conf : confs)
    {
        thread_nr = std::max(thread_nr, conf.thread_nr);
        CHECK_EQ(queue_nr, conf.queue_nr);
    }

    typename QueueT::pointer queue;
    std::vector<QueueT::pointer> queues;

    if (is_master)
    {
        queues.resize(queue_nr);
        for (size_t i = 0; i < queue_nr; ++i)
        {
            queues[i] = gen_queue(p, i, queue_config);
        }
        p->finished(key);
    }

    // wait for everybody to finish preparing
    bar.wait();
    if (is_master)
    {
        p->keeper_barrier("server_ready-" + std::to_string(key), 100ms);
    }

    p->server_serve(key);
    bar.wait();
}
template <size_t kSize>
struct Dummy
{
    char buf[kSize];
};

void benchmark(Patronus::pointer p, boost::barrier &bar, bool is_client)
{
    uint64_t key = 0;
    bool is_master = p->get_thread_id() == 0;
    bar.wait();

    std::vector<QueueHandleConfig> handle_configs;
    handle_configs.emplace_back(
        QueueHandleConfig::get_mw("mw", kEntryNrPerBlock));
    handle_configs.emplace_back(
        QueueHandleConfig::get_unprotected("unprot", kEntryNrPerBlock));
    handle_configs.emplace_back(
        QueueHandleConfig::get_mr("mr", kEntryNrPerBlock));
    handle_configs.emplace_back(
        QueueHandleConfig::get_rpc("rpc", kEntryNrPerBlock));

    for (const auto &handle_conf : handle_configs)
    {
        // for (size_t thread_nr : {1, 4, 8, 16, 32})
        for (size_t thread_nr : {1, 2, 4, 8, 16, 32})
        // for (size_t thread_nr : {1, 4, 8, 32})
        // for (size_t thread_nr : {1, 8, 32})
        {
            for (size_t queue_nr : {4})
            // for (size_t queue_nr : {1})
            {
                constexpr static size_t kCoroNr = 1;
                LOG_IF(INFO, is_master)
                    << "[bench] benching multiple threads for " << handle_conf;
                key++;
                auto conf = BenchConfig::get_conf("lf_push",
                                                  1_M * handle_conf.task_scale,
                                                  thread_nr,
                                                  kCoroNr,
                                                  queue_nr,
                                                  kEntryNrPerBlock);
                if (is_client)
                {
                    conf.validate();
                    LOG_IF(INFO, is_master)
                        << "[sub-conf] running conf: " << conf;
                    benchmark_client(p, bar, is_master, conf, handle_conf, key);
                }
                else
                {
                    benchmark_server(
                        p, bar, is_master, {conf}, QueueConfig{}, key);
                }
            }
        }
    }

    // for (const auto &handle_conf : handle_configs)
    // {
    //     LOG(WARNING)
    //         << "** In this experiments, not recommend to bench with
    //         coroutine.";
    //     LOG(WARNING) << "** cqueue is a lock free algorithm. Using coroutine
    //     "
    //                     "brings extra latency, which degrades performance.";
    //     constexpr static size_t kThreadNr = 32;
    //     for (size_t coro_nr : {2, 4, 8, 16})
    //     {
    //         for (size_t queue_nr : {4})
    //         {
    //             LOG_IF(INFO, is_master)
    //                 << "[bench] benching multiple threads for " <<
    //                 handle_conf;
    //             key++;
    //             auto conf = BenchConfig::get_conf("lf_push",
    //                                               1_M *
    //                                               handle_conf.task_scale,
    //                                               kThreadNr,
    //                                               coro_nr,
    //                                               queue_nr,
    //                                               kEntryNrPerBlock);
    //             if (is_client)
    //             {
    //                 conf.validate();
    //                 LOG_IF(INFO, is_master)
    //                     << "[sub-conf] running conf: " << conf;
    //                 benchmark_client(p, bar, is_master, conf, handle_conf,
    //                 key);
    //             }
    //             else
    //             {
    //                 benchmark_server(
    //                     p, bar, is_master, {conf}, QueueConfig{}, key);
    //             }
    //         }
    //     }
    // }
}

constexpr static size_t kExpectAllocationSize =
    QueueHandleT::alloc_block_size();

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    PatronusConfig pconfig;
    pconfig.machine_nr = ::config::kMachineNr;
    pconfig.block_class = {kExpectAllocationSize};
    pconfig.block_ratio = {1};
    pconfig.reserved_buffer_size = 2_GB;
    pconfig.lease_buffer_size = (::config::kDefaultDSMSize - 2_GB) / 2;
    pconfig.alloc_buffer_size = (::config::kDefaultDSMSize - 2_GB) / 2;

    auto patronus = Patronus::ins(pconfig);

    std::vector<std::thread> threads;
    // boost::barrier bar(kThreadNr);
    auto nid = patronus->get_node_id();

    bool is_client = ::config::is_client(nid);

    if (is_client)
    {
        patronus->registerClientThread();
    }
    else
    {
        patronus->registerServerThread();
    }

    LOG(INFO) << "[bench] " << pre_patronus_explain(*patronus);

    if (is_client)
    {
        boost::barrier bar(kClientThreadNr);
        for (size_t i = 0; i < kClientThreadNr - 1; ++i)
        {
            threads.emplace_back([patronus, &bar]() {
                patronus->registerClientThread();
                bar.wait();
                benchmark(patronus, bar, true);
            });
        }
        bar.wait();
        patronus->keeper_barrier("begin", 10ms);
        benchmark(patronus, bar, true);

        for (auto &t : threads)
        {
            t.join();
        }
    }
    else
    {
        boost::barrier bar(kServerThreadNr);
        for (size_t i = 0; i < kServerThreadNr - 1; ++i)
        {
            threads.emplace_back([patronus, &bar]() {
                patronus->registerServerThread();
                bar.wait();
                benchmark(patronus, bar, false);
            });
        }
        bar.wait();
        patronus->keeper_barrier("begin", 100ms);
        benchmark(patronus, bar, false);

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
        df.load_column<size_t>("x_queue_nr", std::move(col_x_queue_nr));
        df.load_column<size_t>("x_entry_per_block",
                               std::move(col_x_entry_per_block));
        df.load_column<size_t>("test_nr(total)", std::move(col_test_nr));
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

    LOG(INFO) << "finished. ctrl+C to quit.";
}