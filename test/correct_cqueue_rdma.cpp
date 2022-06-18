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
#include "util/Pre.h"
#include "util/Rand.h"
#include "util/TimeConv.h"

using namespace patronus::cqueue;
using namespace define::literals;
using namespace patronus;
using namespace hmdf;

constexpr static size_t kServerThreadNr = NR_DIRECTORY;
constexpr static size_t kClientThreadNr = 32;
constexpr static size_t kMaxCoroNr = 16;
constexpr static size_t kEntryNrPerBlock = 64;

DEFINE_string(exec_meta, "", "The meta data of this execution");

uint64_t calculate_client_id(uint64_t nid, uint64_t tid, uint64_t coro_id)
{
    return nid * kMaxAppThread * kMaxCoroNr + tid * kMaxCoroNr + coro_id;
}

struct QueueItem
{
    uint64_t nid;
    uint64_t tid;
    uint64_t cid;
    uint64_t magic_number;
    bool valid{true};
};
std::ostream &operator<<(std::ostream &os, const QueueItem &item)
{
    os << "{QueueItem nid: " << item.nid << ", tid: " << item.tid
       << ", cid: " << item.cid
       << ", magic_number: " << (void *) item.magic_number
       << ", valid: " << item.valid << "}";
    return os;
}

struct BenchConfig
{
    std::string name;
    double producer_nr{0};
    double consumer_nr{0};
    size_t thread_nr{1};
    size_t coro_nr{1};
    size_t test_nr{0};
    bool should_report{false};

    bool should_report_lat() const
    {
        return should_report && thread_nr == 32 && coro_nr == 1;
    }

    void validate() const
    {
        double sum = producer_nr + consumer_nr;
        CHECK_DOUBLE_EQ(sum, local_client_nr());
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
    bool is_consumer(uint64_t tid) const
    {
        return tid < consumer_nr;
    }
    bool is_producer(uint64_t tid) const
    {
        return !is_consumer(tid);
    }

    BenchConfig clone() const
    {
        BenchConfig ret;
        ret.name = name;
        ret.producer_nr = producer_nr;
        ret.consumer_nr = consumer_nr;
        ret.thread_nr = thread_nr;
        ret.coro_nr = coro_nr;
        ret.test_nr = test_nr;
        ret.should_report = should_report;
        return ret;
    }
    static BenchConfig get_conf(const std::string &name,
                                size_t test_nr,
                                size_t producer_nr,
                                size_t consumer_nr,
                                size_t coro_nr)
    {
        BenchConfig conf;
        conf.name = name;
        conf.test_nr = test_nr;
        conf.producer_nr = producer_nr;
        conf.consumer_nr = consumer_nr;
        conf.thread_nr = producer_nr + consumer_nr;
        conf.coro_nr = coro_nr;
        conf.test_nr = test_nr;
        conf.should_report = true;

        conf.validate();
        return conf;
    }
};
std::ostream &operator<<(std::ostream &os, const BenchConfig &conf)
{
    os << "{conf: name: " << conf.name << ", producer: " << conf.producer_nr
       << ", consumer: " << conf.consumer_nr
       << ", thread_nr: " << conf.thread_nr << ", coro_nr: " << conf.coro_nr
       << "(local_client_nr: " << conf.local_client_nr()
       << ", cluster: " << conf.cluster_client_nr()
       << "), test_nr: " << conf.test_nr
       << ", should_report: " << conf.should_report << "}";
    return os;
}

using QueueHandleT = QueueHandle<QueueItem, kEntryNrPerBlock>;
using QueueT = Queue<QueueItem, kEntryNrPerBlock>;

typename QueueHandleT::pointer gen_handle(Patronus::pointer p,
                                          size_t dir_id,
                                          const QueueHandleConfig &conf,
                                          GlobalAddress meta_gaddr,
                                          CoroContext *ctx)
{
    auto server_nid = ::config::get_server_nids().front();

    DVLOG(1) << "Getting from race:meta_gaddr got " << meta_gaddr;

    auto rdma_adpt = patronus::RdmaAdaptor::new_instance(
        server_nid, dir_id, p, conf.bypass_prot, false /* two sided */, ctx);

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

void validate_helper(const std::list<QueueItem> &list)
{
    LOG(INFO) << "Validating lists: " << util::pre_iter(list, 10);
    uint64_t book[MAX_MACHINE][kMaxAppThread][kMaxCoroNr]{};
    size_t ith = 0;
    for (const auto &item : list)
    {
        if (unlikely(!item.valid))
        {
            LOG(WARNING) << "Witness not valid item: " << item;
        }
        else
        {
            if (unlikely(!item.valid))
            {
                LOG(WARNING) << "Ignoring " << ith << "-th item: " << item
                             << " (not valid)";
            }
            auto expect = book[item.nid][item.tid][item.cid];
            CHECK_GT(item.magic_number, expect)
                << "nid: " << item.nid << ", tid: " << item.tid
                << ", cid: " << item.cid << ", expect > " << expect
                << ", got: " << item.magic_number;
            book[item.nid][item.tid][item.cid] = expect;
        }
        ith++;
    }
}

typename QueueT::pointer gen_queue(Patronus::pointer p,
                                   const QueueConfig &config)
{
    auto allocator = p->get_allocator(0 /* default hint */);

    auto server_rdma_adpt = patronus::RdmaAdaptor::new_instance(p);
    auto queue = QueueT::new_instance(server_rdma_adpt, allocator, config);

    auto meta_gaddr = queue->meta_gaddr();
    p->put("race:meta_gaddr", meta_gaddr, 0ns);
    LOG(INFO) << "Puting to race:meta_gaddr with " << meta_gaddr;

    return queue;
}

void test_basic_client_worker(
    Patronus::pointer p,
    size_t coro_id,
    CoroYield &yield,
    const BenchConfig &bench_conf,
    const QueueHandleConfig &handle_conf,
    GlobalAddress meta_gaddr,
    CoroExecutionContextWith<kMaxCoroNr, AdditionalCoroCtx> &ex,
    OnePassBucketMonitor<uint64_t> &lat_m)
{
    auto nid = p->get_node_id();
    auto tid = p->get_thread_id();
    auto dir_id = tid % kServerThreadNr;
    CoroContext ctx(tid, &yield, &ex.master(), coro_id);

    auto handle = gen_handle(p, dir_id, handle_conf, meta_gaddr, &ctx);

    size_t put_succ_nr = 0;
    size_t put_retry_nr = 0;
    size_t get_succ_nr = 0;
    size_t get_nomem_nr = 0;
    size_t rdma_protection_nr = 0;
    size_t executed_nr = 0;

    ChronoTimer timer;

    ChronoTimer op_timer;
    bool should_report_latency = (tid == 0 && coro_id == 0);

    QueueItem pop_entries[kEntryNrPerBlock];

    util::TraceManager tm(1);
    uint64_t magic = 1;
    while (ex.get_private_data().thread_remain_task > 0)
    {
        auto trace = tm.trace("op");
        if (should_report_latency)
        {
            op_timer.pin();
        }
        if (bench_conf.is_consumer(tid))
        {
            CHECK(false) << "TODO: not supported under multithreads";
        }
        else
        {
            QueueItem item{nid, tid, coro_id, magic};
            // LOG(INFO) << "[bench] LF PUSH " << item;
            auto rc = handle->lf_push_back(item, trace);
            // LOG(INFO) << "[bench] finished LF PUSH " << item << ". rc: " <<
            // rc;
            CHECK_EQ(rc, kOk) << trace;
            if (rc == kOk)
            {
                put_succ_nr++;
                executed_nr++;
                magic++;
                ex.get_private_data().thread_remain_task--;
            }
            else
            {
                CHECK_EQ(rc, RC::kRetry);
                put_retry_nr++;
            }
        }

        if (should_report_latency)
        {
            lat_m.collect(timer.pin());
        }
    }
    auto lists = handle->debug_iterator();
    validate_helper(lists);
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
GlobalAddress g_meta_gaddr;
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
            g_meta_gaddr = p->get_object<GlobalAddress>("race:meta_gaddr", 1ms);
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
            ex.worker(i) =
                CoroCall([p,
                          coro_id = i,
                          &bench_conf,
                          &handle_conf,
                          &ex,
                          &lat_m,
                          meta_gaddr = g_meta_gaddr](CoroYield &yield) {
                    test_basic_client_worker(p,
                                             coro_id,
                                             yield,
                                             bench_conf,
                                             handle_conf,
                                             meta_gaddr,
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
}

void benchmark_server(Patronus::pointer p,
                      boost::barrier &bar,
                      bool is_master,
                      const std::vector<BenchConfig> &confs,
                      const QueueConfig &queue_config,
                      uint64_t key)
{
    auto thread_nr = confs[0].thread_nr;
    for (const auto &conf : confs)
    {
        thread_nr = std::max(thread_nr, conf.thread_nr);
    }

    typename QueueT::pointer queue;

    if (is_master)
    {
        p->finished(key);
        queue = gen_queue(p, queue_config);
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

    for (const auto &handle_conf : handle_configs)
    {
        for (size_t producer_nr : {kMaxAppThread})
        {
            for (size_t coro_nr : {1})
            {
                LOG_IF(INFO, is_master)
                    << "[bench] benching multiple threads for " << handle_conf;
                key++;
                // auto conf = BenchConfig::get_conf(
                //     "default", 1_M, producer_nr, 0 /* consumer_nr */,
                //     coro_nr);
                auto conf = BenchConfig::get_conf("default",
                                                  1_M * handle_conf.task_scale,
                                                  producer_nr,
                                                  0 /* consumer_nr */,
                                                  coro_nr);
                if (is_client)
                {
                    conf.validate();
                    LOG_IF(INFO, is_master)
                        << "[sub-conf] running conf: " << conf;
                    benchmark_client(p, bar, is_master, conf, handle_conf, key);
                }
                else
                {
                    // TODO: let queue config be real.
                    benchmark_server(
                        p, bar, is_master, {conf}, QueueConfig{}, key);
                }
            }
        }
    }
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    PatronusConfig pconfig;
    pconfig.machine_nr = ::config::kMachineNr;
    pconfig.block_class = {32_MB, 2_MB, 4_KB};
    pconfig.block_ratio = {0.1, 0.5, 0.4};
    pconfig.reserved_buffer_size = 2_GB;
    pconfig.lease_buffer_size = (kDSMCacheSize - 2_GB) / 2;
    pconfig.alloc_buffer_size = (kDSMCacheSize - 2_GB) / 2;

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

    patronus->keeper_barrier("finished", 100ms);

    LOG(INFO) << "finished. ctrl+C to quit.";
}