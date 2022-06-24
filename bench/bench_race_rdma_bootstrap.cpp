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
#include "thirdparty/racehashing/hashtable.h"
#include "thirdparty/racehashing/hashtable_handle.h"
#include "thirdparty/racehashing/utils.h"
#include "util/BenchRand.h"
#include "util/DataFrameF.h"
#include "util/Debug.h"
#include "util/Rand.h"
#include "util/TimeConv.h"

using namespace patronus::hash;
using namespace util::literals;
using namespace patronus;
using namespace hmdf;

constexpr static uint64_t kMaxKey = 3_K;

constexpr static size_t kServerThreadNr = NR_DIRECTORY;
constexpr static size_t kClientThreadNr = kMaxAppThread;

constexpr static size_t kLoadDataClientNodeId = 1;

constexpr static size_t kMaxCoroNr = 32;

DEFINE_string(exec_meta, "", "The meta data of this execution");
DEFINE_bool(no_csv, false, "Do not generate result file. Not ready");

std::vector<std::string> col_idx;
std::vector<size_t> col_x_thread_nr;
std::vector<size_t> col_x_coro_nr;
std::vector<size_t> col_x_io_per_boot;
std::vector<size_t> col_x_kvblock_size;
std::vector<size_t> col_test_op_nr;
std::vector<size_t> col_ns;
std::vector<size_t> col_rdma_protection_nr;
std::vector<double> col_succ_rate;

std::vector<std::string> col_lat_idx;
std::unordered_map<std::string, std::vector<uint64_t>> lat_data;

std::vector<double> col_put_succ_rate;
std::vector<double> col_get_succ_rate;

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
    bool load_data{false};
    size_t io_nr_per_bootstrap{0};
    KVGenConf kv_conf;
    IKVRandGenerator::pointer kv_g;
    std::optional<uint64_t> limit_nid{std::nullopt};

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
        conf.load_data = false;
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
        size_t load_nr,
        size_t test_nr,
        size_t thread_nr,
        size_t coro_nr,
        size_t io_nr_per_bootstrap)
    {
        auto load_conf = BenchConfig::get_default_conf(name,
                                                       initial_subtable_nr,
                                                       1 /* test_nr */,
                                                       1 /* thread_nr */,
                                                       1 /* coro_nr */,
                                                       false);
        load_conf.limit_nid = kLoadDataClientNodeId;
        load_conf.io_nr_per_bootstrap = load_nr;
        load_conf.test_nr = 1;
        load_conf.load_data = true;

        auto warm_conf = BenchConfig::get_default_conf(name,
                                                       initial_subtable_nr,
                                                       test_nr,
                                                       thread_nr,
                                                       coro_nr,
                                                       false /* extend */);
        warm_conf.io_nr_per_bootstrap = io_nr_per_bootstrap;
        auto eval_conf = warm_conf.clone();
        eval_conf.should_report = true;

        return pipeline({load_conf, warm_conf, eval_conf});
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
    size_t get_nr{0};
    size_t get_succ_nr{0};
    size_t put_nr{0};
    size_t put_succ_nr{0};
    size_t del_nr{0};
    size_t del_succ_nr{0};
    size_t rdma_protection_nr{0};
};

void init_allocator(Patronus::pointer p,
                    size_t dir_id,
                    size_t thread_nr,
                    size_t kvblock_expect_size)
{
    // for server to handle kv block allocation requests
    // give all to kv blocks
    auto tid = p->get_thread_id();
    CHECK_LT(tid, thread_nr);

    auto rh_buffer = p->get_user_reserved_buffer();

    auto thread_kvblock_pool_size = rh_buffer.size / thread_nr;
    void *thread_kvblock_pool_addr =
        rh_buffer.buffer + tid * thread_kvblock_pool_size;

    mem::SlabAllocatorConfig kvblock_slab_config;
    kvblock_slab_config.block_class = {kvblock_expect_size};
    kvblock_slab_config.block_ratio = {1.0};
    auto kvblock_allocator =
        mem::SlabAllocator::new_instance(thread_kvblock_pool_addr,
                                         thread_kvblock_pool_size,
                                         kvblock_slab_config);

    p->reg_allocator(hash::config::kAllocHintKVBlock, kvblock_allocator);

    LOG_FIRST_N(WARNING, 1)
        << "TODO: the performance for MR may be higher: MR over MR";

    // a little bit tricky here: MR allocator should use the internal ones
    // otherwise will fail to convert to the dsm offset.
    mem::MRAllocatorConfig dir_st_mr_config;
    dir_st_mr_config.allocator = p->get_allocator(Patronus::kDefaultHint);
    dir_st_mr_config.rdma_context = p->get_dsm()->get_rdma_context(dir_id);
    auto dir_st_mr_allocator = mem::MRAllocator::new_instance(dir_st_mr_config);
    p->reg_allocator(hash::config::kAllocHintDirSubtableOverMR,
                     dir_st_mr_allocator);

    // I believe sharing config is safe
    mem::MRAllocatorConfig kvblock_mr_config;
    kvblock_mr_config.allocator = kvblock_allocator;
    kvblock_mr_config.rdma_context = p->get_dsm()->get_rdma_context(dir_id);
    auto kvblock_mr_allocator =
        mem::MRAllocator::new_instance(kvblock_mr_config);
    p->reg_allocator(hash::config::kAllocHintKVBlockOverMR,
                     kvblock_mr_allocator);
}

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
    auto dir_id = tid % kServerThreadNr;
    auto server_nid = ::config::get_server_nids().front();
    CoroContext ctx(tid, &yield, &ex.master(), coro_id);

    size_t executed_nr = 0;
    size_t executed_op_nr = 0;
    ChronoTimer timer;

    auto rdma_adpt = patronus::RdmaAdaptor::new_instance(server_nid,
                                                         dir_id,
                                                         p,
                                                         rhh_conf.bypass_prot,
                                                         false /* two sided */,
                                                         &ctx);

    std::string key;
    key.resize(sizeof(uint64_t));
    std::string got_value;
    size_t succ_nr{0};
    size_t miss_nr{0};
    size_t retry_nr{0};

    OnePassBucketMonitor<uint64_t> lat_m(
        kLatencyMin, kLatencyMax, kLatencyRange);

    bool should_report_lat = tid == 0 && coro_id == 0;
    ChronoTimer op_timer;
    while (ex.get_private_data().thread_remain_task > 0)
    {
        if (should_report_lat)
        {
            op_timer.pin();
        }
        {
            using HandleT = typename RaceHashing<kE, kB, kS>::Handle;
            auto prhh = HandleT::new_instance(server_nid,
                                              meta_gaddr,
                                              rhh_conf,
                                              false /* auto expand */,
                                              rdma_adpt);
            prhh->init();
            for (size_t i = 0; i < bench_conf.io_nr_per_bootstrap; ++i)
            {
                bench_conf.kv_g->gen_key(&key[0], sizeof(uint64_t));
                RetCode rc;
                if (bench_conf.load_data)
                {
                    rc = prhh->put(key, got_value);
                }
                else
                {
                    rc = prhh->get(key, got_value);
                }

                if (rc == RC::kOk)
                {
                    succ_nr++;
                }
                else if (rc == RC::kNotFound)
                {
                    miss_nr++;
                }
                else if (rc == RC::kRetry)
                {
                    retry_nr++;
                }
                else
                {
                    LOG(FATAL) << "Unknown return code: " << rc;
                }
                executed_op_nr++;
            }
        }

        if (should_report_lat)
        {
            auto ns = timer.pin();
            lat_m.collect(ns);
        }

        executed_nr++;
        ex.get_private_data().thread_remain_task--;
        if (bench_conf.load_data)
        {
            ex.get_private_data().put_succ_nr += succ_nr;
            ex.get_private_data().put_nr += succ_nr + miss_nr + retry_nr;
        }
        else
        {
            ex.get_private_data().get_succ_nr += succ_nr;
            ex.get_private_data().get_nr += succ_nr + miss_nr + retry_nr;
        }
    }
    auto ns = timer.pin();

    LOG(INFO) << "[bench] op: " << executed_nr << ", ex op: " << executed_op_nr
              << " ns: " << ns << ", ops: " << 1e9 * executed_nr / ns
              << ". succ: " << succ_nr << ", miss: " << miss_nr
              << ", retry: " << retry_nr << ". " << ctx;

    auto report_name = bench_conf.conf_name() + "[" + rhh_conf.name + "]";
    if (should_report_lat && bench_conf.should_report)
    {
        if (col_lat_idx.empty())
        {
            col_lat_idx.push_back("lat_min");
            col_lat_idx.push_back("lat_p5");
            col_lat_idx.push_back("lat_p9");
            col_lat_idx.push_back("lat_p99");
            col_lat_idx.push_back("lat_max");
        }
        lat_data[report_name].push_back(lat_m.min());
        lat_data[report_name].push_back(lat_m.percentile(0.5));
        lat_data[report_name].push_back(lat_m.percentile(0.9));
        lat_data[report_name].push_back(lat_m.percentile(0.99));
        lat_data[report_name].push_back(lat_m.max());
    }

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
    auto nid = p->get_node_id();

    auto coro_nr = bench_conf.coro_nr;
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

    bool should_enter = tid < thread_nr;
    if (bench_conf.limit_nid.has_value() && bench_conf.limit_nid.value() != nid)
    {
        should_enter = false;
    }
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

    auto report_name = bench_conf.conf_name() + "[" + rhh_conf.name + "]";
    if (is_master && bench_conf.should_report)
    {
        col_idx.push_back(report_name);
        col_x_thread_nr.push_back(bench_conf.thread_nr);
        col_x_coro_nr.push_back(bench_conf.coro_nr);
        col_x_kvblock_size.push_back(rhh_conf.kvblock_expect_size);
        col_test_op_nr.push_back(actual_test_nr);
        col_ns.push_back(ns);
        col_x_io_per_boot.push_back(bench_conf.io_nr_per_bootstrap);

        // const auto &m = ex.get_private_data().g;
        // col_latency_min.push_back(m.min());
        // col_latency_p5.push_back(m.percentile(0.5));
        // col_latency_p9.push_back(m.percentile(0.9));
        // col_latency_p99.push_back(m.percentile(0.99));
        // col_latency_max.push_back(m.max());

        auto &prv = ex.get_private_data();
        double put_rate = 1.0 * prv.put_succ_nr / prv.put_nr;
        double get_rate = 1.0 * prv.get_succ_nr / prv.get_nr;
        col_put_succ_rate.push_back(put_rate);
        col_get_succ_rate.push_back(get_rate);
    }

    bar.wait();
}

template <size_t kE, size_t kB, size_t kS>
void benchmark_server(Patronus::pointer p,
                      boost::barrier &bar,
                      bool is_master,
                      const std::vector<BenchConfig> &confs,
                      size_t kvblock_expect_size,
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
    auto dir_id = tid;

    typename RaceHashingT::pointer rh;
    if (tid < thread_nr)
    {
        // do nothing
        init_allocator(p, dir_id, thread_nr, kvblock_expect_size);
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

    p->server_serve(key);
    bar.wait();
}

void benchmark(Patronus::pointer p, boost::barrier &bar, bool is_client)
{
    uint64_t key = 0;
    bool is_master = p->get_thread_id() == 0;
    bar.wait();

    // set the rhh we like to test
    std::vector<RaceHashingHandleConfig> rhh_configs;
    // for (size_t kvblock_size : {64_B, 512_B})
    // for (size_t kvblock_size : {512_B})
    // for (size_t kvblock_size : {1_KB, 4_KB})
    for (size_t kvblock_size : {4_KB})
    // for (size_t kvblock_size : {512_B, 4_KB})
    // for (size_t kvblock_size : {4_KB})
    {
        rhh_configs.push_back(RaceHashingConfigFactory::get_unprotected(
            "unprot",
            kvblock_size,
            1 /* batch */,
            true /* mock kvblock match */));
        rhh_configs.push_back(RaceHashingConfigFactory::get_mw_protected(
            "MW",
            kvblock_size,
            1 /* batch size */,
            true /* mock kvblock match */));
        rhh_configs.push_back(RaceHashingConfigFactory::get_mr_protected(
            "MR",
            kvblock_size,
            1 /* batch size */,
            true /* mock kvblock match */));
    }

    auto capacity = RaceHashing<4, 16, 16>::max_capacity();

    for (auto &rhh_conf : rhh_configs)
    {
        for (size_t thread_nr : {1, 2, 4, 8, 16, 32})
        // for (size_t thread_nr : {32})
        {
            for (size_t coro_nr : {1})
            // for (size_t coro_nr : {1})
            {
                // for (size_t io_nr : {8, 16, 32})
                for (size_t io_nr : {0})
                // for (size_t io_nr : {128})
                // for (size_t io_nr : {0, 1, 8, 16, 32})
                {
                    LOG_IF(INFO, is_master)
                        << "[bench] benching single thread for " << rhh_conf;
                    key++;
                    auto basic_conf = BenchConfigFactory::get_bootstrap_config(
                        "boot",
                        4 /* subtable nr */,
                        capacity,
                        // 1_M,
                        100_K,
                        // 10_K /* test_nr */,
                        thread_nr /* thread nr */,
                        coro_nr /* coro nr */,
                        io_nr);
                    if (is_client)
                    {
                        for (const auto &bench_conf : basic_conf)
                        {
                            bench_conf.validate();
                            LOG_IF(INFO, is_master)
                                << "[sub-conf] running conf: " << bench_conf;
                            benchmark_client<4, 16, 16>(
                                p, bar, is_master, bench_conf, rhh_conf, key);
                        }
                    }
                    else
                    {
                        benchmark_server<4, 16, 16>(
                            p,
                            bar,
                            is_master,
                            basic_conf,
                            rhh_conf.kvblock_expect_size,
                            key);
                    }
                }
            }
        }
    }

    for (auto &rhh_conf : rhh_configs)
    {
        for (size_t thread_nr : {32})
        {
            for (size_t coro_nr : {2, 4, 8, 16})
            // for (size_t coro_nr : {1})
            {
                // for (size_t io_nr : {8, 16, 32})
                for (size_t io_nr : {0})
                // for (size_t io_nr : {128})
                // for (size_t io_nr : {0, 1, 8, 16, 32})
                {
                    LOG_IF(INFO, is_master)
                        << "[bench] benching single thread for " << rhh_conf;
                    key++;
                    auto basic_conf = BenchConfigFactory::get_bootstrap_config(
                        "boot",
                        4 /* subtable nr */,
                        capacity,
                        // 1_M,
                        100_K,
                        // 10_K /* test_nr */,
                        thread_nr /* thread nr */,
                        coro_nr /* coro nr */,
                        io_nr);
                    if (is_client)
                    {
                        for (const auto &bench_conf : basic_conf)
                        {
                            bench_conf.validate();
                            LOG_IF(INFO, is_master)
                                << "[sub-conf] running conf: " << bench_conf;
                            benchmark_client<4, 16, 16>(
                                p, bar, is_master, bench_conf, rhh_conf, key);
                        }
                    }
                    else
                    {
                        benchmark_server<4, 16, 16>(
                            p,
                            bar,
                            is_master,
                            basic_conf,
                            rhh_conf.kvblock_expect_size,
                            key);
                    }
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
    pconfig.block_class = {2_MB, 8_KB};
    pconfig.block_ratio = {0.5, 0.5};
    pconfig.reserved_buffer_size = 2_GB;
    pconfig.lease_buffer_size = (kDSMCacheSize - 2_GB) / 2;
    pconfig.alloc_buffer_size = (kDSMCacheSize - 2_GB) / 2;

    LOG(WARNING) << "[bench] The unprotected with io_nr > 0 is not accurate. "
                    "It will bind subtable.";

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
        patronus->keeper_barrier("begin", 100ms);
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
        df.load_column<size_t>("x_kvblock_size", std::move(col_x_kvblock_size));
        df.load_column<size_t>("x_io_nr_per_boot",
                               std::move(col_x_io_per_boot));
        df.load_column<size_t>("test_nr(total)", std::move(col_test_op_nr));
        df.load_column<size_t>("test_ns(total)", std::move(col_ns));

        df.load_column<size_t>("rdma_protection_nr",
                               std::move(col_rdma_protection_nr));

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

        df.consolidate<double, size_t, double>("ops(cluster)",
                                               "x_io_nr_per_boot",
                                               "effective ops(cluster)",
                                               mul_f,
                                               false);

        df.consolidate<double, size_t, double>(
            "ops(total)", "x_thread_nr", "ops(thread)", div_f2, false);
        df.consolidate<double, size_t, double>(
            "ops(thread)", "x_coro_nr", "ops(coro)", div_f2, false);

        df.load_column<double>("get_rate", std::move(col_get_succ_rate));
        df.load_column<double>("put_rate", std::move(col_put_succ_rate));

        auto filename = binary_to_csv_filename(argv[0], FLAGS_exec_meta);
        df.write<std::ostream, std::string, size_t, double>(std::cout,
                                                            io_format::csv2);
        if (likely(!FLAGS_no_csv))
        {
            df.write<std::string, size_t, double>(filename.c_str(),
                                                  io_format::csv2);
        }
    }

    // {
    //     StrDataFrame df;
    //     df.load_index(std::move(col_lat_idx));
    //     for (auto &[key, vec] : lat_data)
    //     {
    //         df.load_column<uint64_t>(key.c_str(), std::move(vec));
    //     }
    //     std::map<std::string, std::string> info;
    //     info.emplace("kind", "lat");
    //     auto filename = binary_to_csv_filename(argv[0], FLAGS_exec_meta,
    //     info); df.write<std::ostream, std::string, size_t, double>(std::cout,
    //                                                         io_format::csv2);
    //     if (likely(!FLAGS_no_csv))
    //     {
    //         df.write<std::string, size_t, double>(filename.c_str(),
    //                                               io_format::csv2);
    //     }
    // }

    patronus->keeper_barrier("finished", 100ms);

    LOG(INFO) << "finished. ctrl+C to quit.";
}