#include <algorithm>
#include <random>
#include <set>

#include "Common.h"
#include "boost/thread/barrier.hpp"
#include "glog/logging.h"
#include "patronus/Patronus.h"
#include "patronus/RdmaAdaptor.h"
#include "patronus/memory/direct_allocator.h"
#include "patronus/memory/mr_allocator.h"
#include "thirdparty/racehashing/hashtable.h"
#include "thirdparty/racehashing/hashtable_handle.h"
#include "thirdparty/racehashing/utils.h"
#include "util/BenchRand.h"
#include "util/DataFrameF.h"
#include "util/Debug.h"
#include "util/Rand.h"
#include "util/TimeConv.h"

using namespace patronus::hash;
using namespace define::literals;
using namespace patronus;
using namespace hmdf;

[[maybe_unused]] constexpr uint16_t kClientNodeId = 1;
[[maybe_unused]] constexpr uint16_t kServerNodeId = 0;
constexpr uint32_t kMachineNr = 2;
constexpr static uint64_t kMaxKey = 100_K;
constexpr static size_t kThreadNr = 24;
constexpr static size_t kMaxCoroNr = 1;

DEFINE_string(exec_meta, "", "The meta data of this execution");
DEFINE_bool(no_csv, false, "Do not generate result file. Not ready");

std::vector<std::string> col_idx;
std::vector<size_t> col_x_thread_nr;
std::vector<size_t> col_x_coro_nr;
std::vector<size_t> col_x_io_per_boot;
std::vector<size_t> col_test_op_nr;
std::vector<size_t> col_ns;
std::vector<size_t> col_rdma_protection_nr;

struct KVGenConf
{
    uint64_t max_key{1};
    bool use_zip{false};
    double zip_skewness{0};
    size_t kvblock_expect_size{64};
    std::string desc() const
    {
        if (use_zip)
        {
            return "zip(skewness:" + std::to_string(zip_skewness) +
                   ";max_key:" + std::to_string(max_key) +
                   ";blk_sz:" + std::to_string(kvblock_expect_size) + ")";
        }
        else
        {
            return "uni(max_key:" + std::to_string(max_key) +
                   ";blk_sz:" + std::to_string(kvblock_expect_size) + ")";
        }
    }
};
inline std::ostream &operator<<(std::ostream &os, const KVGenConf &c)
{
    os << "{kv_conf: max_key: " << c.max_key << ", use_zip: " << c.use_zip
       << ", skewness: " << c.zip_skewness
       << ", kvblock_expect_size: " << c.kvblock_expect_size << "}";
    return os;
}

struct BenchConfig
{
    std::string name;
    bool auto_extend{false};
    bool first_enter{true};
    bool server_should_leave{true};
    size_t thread_nr{1};
    size_t coro_nr{1};
    size_t test_nr{0};
    bool should_report{false};
    size_t initial_subtable_nr{1};
    bool subtable_use_mr{false};
    size_t io_nr_per_bootstrap{0};
    KVGenConf kv_conf;
    IKVRandGenerator::pointer kv_g;

    void validate() const
    {
    }
    std::string conf_name() const
    {
        return name;
    }
    BenchConfig clone() const
    {
        BenchConfig ret;
        ret.name = name;
        ret.auto_extend = auto_extend;
        ret.first_enter = first_enter;
        ret.server_should_leave = server_should_leave;
        ret.thread_nr = thread_nr;
        ret.coro_nr = coro_nr;
        ret.test_nr = test_nr;
        ret.should_report = should_report;
        ret.initial_subtable_nr = initial_subtable_nr;
        ret.subtable_use_mr = subtable_use_mr;
        ret.io_nr_per_bootstrap = io_nr_per_bootstrap;
        if (kv_g != nullptr)
        {
            ret.kv_g = kv_g->clone();
        }
        else
        {
            ret.kv_g = nullptr;
        }
        return ret;
    }
    static BenchConfig get_empty_conf(const std::string &name)
    {
        BenchConfig conf;
        conf.name = name;
        conf.initial_subtable_nr = 1;

        return conf;
    }
    static BenchConfig get_default_conf(const std::string &name,
                                        size_t initial_subtable_nr,
                                        size_t test_nr,
                                        size_t thread_nr,
                                        size_t coro_nr,
                                        bool auto_extend)
    {
        auto conf = get_empty_conf(name);
        conf.initial_subtable_nr = initial_subtable_nr;
        conf.auto_extend = auto_extend;
        conf.thread_nr = thread_nr;
        conf.coro_nr = coro_nr;
        conf.test_nr = test_nr;
        conf.kv_conf = KVGenConf{.use_zip = true,
                                 .max_key = kMaxKey,
                                 .zip_skewness = 0.99,
                                 .kvblock_expect_size = 64};

        if (conf.kv_conf.use_zip)
        {
            conf.kv_g = MehcachedZipfianRandGenerator::new_instance(
                0, conf.kv_conf.max_key, conf.kv_conf.zip_skewness);
        }
        else
        {
            conf.kv_g =
                UniformRandGenerator::new_instance(0, conf.kv_conf.max_key);
        }

        conf.validate();
        return conf;
    }
};
std::ostream &operator<<(std::ostream &os, const BenchConfig &conf)
{
    os << "{conf: name: " << conf.name << ", expand: " << conf.auto_extend
       << ", thread: " << conf.thread_nr << ", coro: " << conf.coro_nr
       << ", test: " << conf.test_nr
       << ", subtable_nr: " << conf.initial_subtable_nr
       << ", enter: " << conf.first_enter
       << ", leave: " << conf.server_should_leave
       << ", report: " << conf.should_report
       << ", io_nr_per_bootstrap: " << conf.io_nr_per_bootstrap << "}";
    return os;
}

class BenchConfigFactory
{
public:
    static std::vector<BenchConfig> get_bootstrap_config(
        const std::string &name,
        size_t initial_subtable_nr,
        size_t test_nr,
        size_t thread_nr,
        size_t coro_nr,
        size_t io_nr_per_bootstrap)
    {
        auto warm_conf = BenchConfig::get_default_conf(name,
                                                       initial_subtable_nr,
                                                       test_nr,
                                                       thread_nr,
                                                       coro_nr,
                                                       false /* extend */);
        warm_conf.io_nr_per_bootstrap = io_nr_per_bootstrap;
        auto eval_conf = warm_conf.clone();
        eval_conf.should_report = true;

        return pipeline({warm_conf, eval_conf});
    }

private:
    static std::vector<BenchConfig> pipeline(std::vector<BenchConfig> &&confs)
    {
        for (auto &conf : confs)
        {
            conf.first_enter = false;
            conf.server_should_leave = false;
        }
        if (confs.empty())
        {
            return confs;
        }
        confs.front().first_enter = true;
        confs.back().server_should_leave = true;
        return confs;
    }
};

RemoteMemHandle alloc_dcache_handle(IRdmaAdaptor::pointer rdma_adpt,
                                    GlobalAddress table_meta_gaddr)
{
    return rdma_adpt->acquire_perm(table_meta_gaddr,
                                   0 /* alloc hint */,
                                   64_MB,
                                   0ns,
                                   (uint8_t) AcquireRequestFlag::kNoGc);
}
void free_dcache_handle(IRdmaAdaptor::pointer rdma_adpt,
                        RemoteMemHandle &handle)
{
    rdma_adpt->relinquish_perm(handle, 0 /* alloc hint */, 0 /* flag */);
}

template <size_t kE, size_t kB, size_t kS>
typename RaceHashing<kE, kB, kS>::pointer gen_rdma_rh(Patronus::pointer p,
                                                      size_t initial_subtable)
{
    using RaceHashingT = RaceHashing<kE, kB, kS>;
    auto rh_buffer = p->get_user_reserved_buffer();

    RaceHashingConfig conf;
    conf.initial_subtable = initial_subtable;
    conf.g_kvblock_pool_size = rh_buffer.size;
    conf.g_kvblock_pool_addr = rh_buffer.buffer;
    auto server_rdma_ctx = patronus::RdmaAdaptor::new_instance(p);

    // borrow from the master's kAllocHintDirSubtable allocator
    auto rh_allocator = p->get_allocator(hash::config::kAllocHintDirSubtable);

    auto rh = RaceHashingT::new_instance(server_rdma_ctx, rh_allocator, conf);

    auto meta_gaddr = rh->meta_gaddr();
    p->put("race:meta_gaddr", meta_gaddr, 0ns);
    LOG(INFO) << "Puting to race:meta_gaddr with " << meta_gaddr;

    return rh;
}

constexpr uint64_t kLatencyMin = util::time::to_ns(0ns);
constexpr uint64_t kLatencyMax = util::time::to_ns(10ms);
constexpr uint64_t kLatencyRange = util::time::to_ns(500ns);

struct AdditionalCoroCtx
{
    ssize_t thread_remain_task{0};
    OnePassBucketMonitor<size_t> g{kLatencyMin, kLatencyMax, kLatencyRange};
    OnePassBucketMonitor<size_t> read_hit_lat{
        kLatencyMin, kLatencyMax, kLatencyRange};
    OnePassBucketMonitor<size_t> read_miss_lat{
        kLatencyMin, kLatencyMax, kLatencyRange};
    OnePassBucketMonitor<size_t> insert_succ_lat{
        kLatencyMin, kLatencyMax, kLatencyRange};
    OnePassBucketMonitor<size_t> insert_fail_lat{
        kLatencyMin, kLatencyMax, kLatencyRange};
    size_t get_nr{0};
    size_t get_succ_nr{0};
    size_t put_nr{0};
    size_t put_succ_nr{0};
    size_t del_nr{0};
    size_t del_succ_nr{0};
    size_t rdma_protection_nr{0};
};

template <size_t kE, size_t kB, size_t kS>
void test_basic_client_worker(
    Patronus::pointer p,
    size_t coro_id,
    CoroYield &yield,
    [[maybe_unused]] const BenchConfig &bench_conf,
    RaceHashingHandleConfig &rhh_conf,
    GlobalAddress meta_gaddr,
    CoroExecutionContextWith<kMaxCoroNr, AdditionalCoroCtx> &ex)
{
    auto tid = p->get_thread_id();
    auto dir_id = tid;
    CoroContext ctx(tid, &yield, &ex.master(), coro_id);

    size_t executed_nr = 0;
    ChronoTimer timer;

    auto rdma_adpt =
        patronus::RdmaAdaptor::new_instance(kServerNodeId, dir_id, p, &ctx);
    rhh_conf.init.dcache_handle = alloc_dcache_handle(rdma_adpt, meta_gaddr);

    std::string key;
    key.resize(sizeof(uint64_t));
    std::string got_value;
    while (ex.get_private_data().thread_remain_task > 0)
    {
        {
            using HandleT = typename RaceHashing<kE, kB, kS>::Handle;
            auto prhh = HandleT::new_instance(kServerNodeId,
                                              meta_gaddr,
                                              rhh_conf,
                                              false /* auto expand */,
                                              rdma_adpt);
            prhh->init();
            for (size_t i = 0; i < bench_conf.io_nr_per_bootstrap; ++i)
            {
                bench_conf.kv_g->gen_key(&key[0], sizeof(uint64_t));
                prhh->get(key, got_value);
            }
        }
        executed_nr++;
        ex.get_private_data().thread_remain_task--;
    }
    auto ns = timer.pin();

    LOG(INFO) << "[bench] op: " << executed_nr << ", ns: " << ns
              << ", ops: " << 1e9 * executed_nr / ns << ". " << ctx;

    free_dcache_handle(rdma_adpt, rhh_conf.init.dcache_handle.value());

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
    auto mid = tid;

    CoroContext mctx(tid, &yield, ex.workers());
    CHECK(mctx.is_master());

    ssize_t task_per_sync = test_nr / 100;
    LOG_IF(WARNING, task_per_sync <= (ssize_t) coro_nr);
    task_per_sync = std::max(task_per_sync, ssize_t(coro_nr));
    ssize_t remain =
        atm_task_nr.fetch_sub(task_per_sync, std::memory_order_relaxed) -
        task_per_sync;
    ex.get_private_data().thread_remain_task = task_per_sync;

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
        auto nr =
            p->try_get_client_continue_coros(mid, coro_buf, 2 * kMaxCoroNr);
        for (size_t i = 0; i < nr; ++i)
        {
            auto coro_id = coro_buf[i];
            DVLOG(1) << "[bench] yielding due to CQE: " << (int) coro_id;
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
template <size_t kE, size_t kB, size_t kS>
void benchmark_client(Patronus::pointer p,
                      boost::barrier &bar,
                      bool is_master,
                      const BenchConfig &bench_conf,
                      RaceHashingHandleConfig &rhh_conf,
                      uint64_t key)
{
    auto tid = p->get_thread_id();

    auto coro_nr = bench_conf.coro_nr;
    CHECK_EQ(coro_nr, 1);
    auto thread_nr = bench_conf.thread_nr;
    bool first_enter = bench_conf.first_enter;
    bool server_should_leave = bench_conf.server_should_leave;
    CHECK_LE(coro_nr, kMaxCoroNr);
    size_t actual_test_nr = bench_conf.test_nr * rhh_conf.test_nr_scale_factor;

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

    ChronoTimer timer;
    CoroExecutionContextWith<kMaxCoroNr, AdditionalCoroCtx> ex;
    if (tid < thread_nr)
    {
        ex.get_private_data().thread_remain_task = 0;
        for (size_t i = coro_nr; i < kMaxCoroNr; ++i)
        {
            // no that coro, so directly finished.
            ex.worker_finished(i);
        }
        for (size_t i = 0; i < coro_nr; ++i)
        {
            ex.worker(
                i) = CoroCall([p,
                               coro_id = i,
                               &bench_conf,
                               &rhh_conf,
                               &ex,
                               meta_gaddr = g_meta_gaddr](CoroYield &yield) {
                test_basic_client_worker<kE, kB, kS>(
                    p, coro_id, yield, bench_conf, rhh_conf, meta_gaddr, ex);
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
        << ", ops: " << ops << ", avg " << avg_ns
        << " ns. global lat: " << ex.get_private_data().g;

    if (is_master && server_should_leave)
    {
        LOG(INFO) << "p->finished(" << key << ")";
        p->finished(key);
    }

    if (is_master && bench_conf.should_report)
    {
        CHECK_EQ(coro_nr, 1)
            << "only work when coro_nr == 1. Possible RC to the bucket monitor";
        col_idx.push_back(bench_conf.conf_name() + "[" + rhh_conf.name + "]");
        col_x_thread_nr.push_back(bench_conf.thread_nr);
        col_x_coro_nr.push_back(bench_conf.coro_nr);
        col_test_op_nr.push_back(actual_test_nr);
        col_ns.push_back(ns);
        col_x_io_per_boot.push_back(bench_conf.io_nr_per_bootstrap);
    }
}

template <size_t kE, size_t kB, size_t kS>
void benchmark_server(Patronus::pointer p,
                      boost::barrier &bar,
                      bool is_master,
                      const std::vector<BenchConfig> &confs,
                      uint64_t key)
{
    auto thread_nr = confs[0].thread_nr;
    auto initial_subtable = confs[0].initial_subtable_nr;
    for (const auto &conf : confs)
    {
        thread_nr = std::max(thread_nr, conf.thread_nr);
        DCHECK_EQ(initial_subtable, conf.initial_subtable_nr);
    }

    using RaceHashingT = RaceHashing<kE, kB, kS>;
    auto tid = p->get_thread_id();
    auto mid = tid;

    typename RaceHashingT::pointer rh;
    if (tid < thread_nr)
    {
        // do nothing
    }
    else
    {
        CHECK(!is_master) << "** master not initing allocator";
    }
    if (is_master)
    {
        p->finished(key);
        rh = gen_rdma_rh<kE, kB, kS>(p, initial_subtable);
        auto max_capacity = rh->max_capacity();
        LOG(INFO) << "[bench] rh: " << pre_rh_explain(*rh)
                  << ". max_capacity: " << max_capacity;
    }
    // wait for everybody to finish preparing
    bar.wait();
    if (is_master)
    {
        p->keeper_barrier("server_ready-" + std::to_string(key), 100ms);
    }

    p->server_serve(mid, key);
    bar.wait();
}

void benchmark(Patronus::pointer p, boost::barrier &bar, bool is_client)
{
    uint64_t key = 0;
    bool is_master = p->get_thread_id() == 0;
    bar.wait();

    // set the rhh we like to test
    std::vector<RaceHashingHandleConfig> rhh_configs;
    rhh_configs.push_back(
        RaceHashingConfigFactory::get_unprotected_boostrap("unprot"));
    rhh_configs.push_back(RaceHashingConfigFactory::get_mw_protected_bootstrap(
        "patronus w/o(st)", false /* eager_subtable */));
    rhh_configs.push_back(RaceHashingConfigFactory::get_mr_protected_boostrap(
        "mr w/o(st)", false /* eager_subtable */));
    rhh_configs.push_back(RaceHashingConfigFactory::get_mw_protected_bootstrap(
        "patronus w/(st)", true /* eager_subtable */));
    rhh_configs.push_back(RaceHashingConfigFactory::get_mr_protected_boostrap(
        "mr w/(st)", true /* eager_subtable */));

    for (auto &rhh_conf : rhh_configs)
    {
        for (size_t io_nr : {0, 1, 8})
        {
            LOG_IF(INFO, is_master)
                << "[bench] benching single thread for " << rhh_conf;
            key++;
            auto basic_conf =
                BenchConfigFactory::get_bootstrap_config("boot",
                                                         8 /* subtable nr */,
                                                         100_K,
                                                         1 /* thread nr */,
                                                         1 /* coro nr */,
                                                         io_nr);
            if (is_client)
            {
                for (const auto &bench_conf : basic_conf)
                {
                    bench_conf.validate();
                    LOG_IF(INFO, is_master)
                        << "[sub-conf] running conf: " << bench_conf;
                    benchmark_client<8, 16, 16>(
                        p, bar, is_master, bench_conf, rhh_conf, key);
                }
            }
            else
            {
                benchmark_server<8, 16, 16>(p, bar, is_master, basic_conf, key);
            }
        }
    }
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    PatronusConfig pconfig;
    pconfig.machine_nr = kMachineNr;
    pconfig.block_class = {2_MB, 8_KB};
    pconfig.block_ratio = {0.5, 0.5};
    pconfig.reserved_buffer_size = 2_GB;
    pconfig.lease_buffer_size = (kDSMCacheSize - 2_GB) / 2;
    pconfig.alloc_buffer_size = (kDSMCacheSize - 2_GB) / 2;

    LOG(WARNING) << "[bench] The unprotected with io_nr > 0 is not accurate. "
                    "It will bind subtable.";

    auto patronus = Patronus::ins(pconfig);

    std::vector<std::thread> threads;
    boost::barrier bar(kThreadNr);
    auto nid = patronus->get_node_id();

    bool is_client = nid == kClientNodeId;

    if (is_client)
    {
        patronus->registerClientThread();
    }
    else
    {
        patronus->registerServerThread();
    }

    LOG(INFO) << "[bench] " << pre_patronus_explain(*patronus);

    for (size_t i = 0; i < kThreadNr - 1; ++i)
    {
        threads.emplace_back([patronus, &bar, is_client]() {
            if (is_client)
            {
                patronus->registerClientThread();
            }
            else
            {
                patronus->registerServerThread();
            }
            benchmark(patronus, bar, is_client);
        });
    }
    benchmark(patronus, bar, is_client);

    for (auto &t : threads)
    {
        t.join();
    }

    StrDataFrame df;
    df.load_index(std::move(col_idx));
    df.load_column<size_t>("x_thread_nr", std::move(col_x_thread_nr));
    df.load_column<size_t>("x_coro_nr", std::move(col_x_coro_nr));
    df.load_column<size_t>("test_nr(total)", std::move(col_test_op_nr));
    df.load_column<size_t>("test_ns(total)", std::move(col_ns));

    df.load_column<size_t>("rdma_protection_nr",
                           std::move(col_rdma_protection_nr));
    df.load_column<size_t>("io_nr_per_boot", std::move(col_x_io_per_boot));

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
    df.consolidate<double, size_t, double>(
        "ops(total)", "x_thread_nr", "ops(thread)", div_f2, false);
    df.consolidate<double, size_t, double>(
        "ops(thread)", "x_coro_nr", "ops(coro)", div_f2, false);

    auto filename = binary_to_csv_filename(argv[0], FLAGS_exec_meta);
    df.write<std::ostream, std::string, size_t, double>(std::cout,
                                                        io_format::csv2);
    if (likely(!FLAGS_no_csv))
    {
        df.write<std::string, size_t, double>(filename.c_str(),
                                              io_format::csv2);
    }

    patronus->keeper_barrier("finished", 100ms);

    LOG(INFO) << "finished. ctrl+C to quit.";
}