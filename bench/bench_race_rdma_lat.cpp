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
constexpr static size_t kThreadNr = 24;
constexpr static size_t kMaxCoroNr = 1;
constexpr static uint64_t kMaxKey = 100_K;

DEFINE_string(exec_meta, "", "The meta data of this execution");
DEFINE_bool(no_csv, false, "Do not generate result file. Not ready");

std::vector<std::string> col_idx;
std::vector<std::string> col_x_kvdist;
std::vector<size_t> col_x_thread_nr;
std::vector<size_t> col_x_coro_nr;
std::vector<double> col_x_put_rate;
std::vector<double> col_x_del_rate;
std::vector<double> col_x_get_rate;
std::vector<size_t> col_test_op_nr;
std::vector<size_t> col_ns;
std::vector<double> col_get_succ_rate;
std::vector<double> col_put_succ_rate;
std::vector<double> col_del_succ_rate;

std::vector<size_t> col_g_lat_min;
std::vector<size_t> col_g_lat_p5;
std::vector<size_t> col_g_lat_p9;
std::vector<size_t> col_g_lat_p99;
std::vector<size_t> col_g_lat_max;

std::vector<size_t> col_read_hit_lat_min;
std::vector<size_t> col_read_hit_lat_p5;
std::vector<size_t> col_read_hit_lat_p9;
std::vector<size_t> col_read_hit_lat_p99;
std::vector<size_t> col_read_hit_lat_max;

std::vector<size_t> col_read_miss_lat_min;
std::vector<size_t> col_read_miss_lat_p5;
std::vector<size_t> col_read_miss_lat_p9;
std::vector<size_t> col_read_miss_lat_p99;
std::vector<size_t> col_read_miss_lat_max;

std::vector<size_t> col_ins_success_lat_min;
std::vector<size_t> col_ins_success_lat_p5;
std::vector<size_t> col_ins_success_lat_p9;
std::vector<size_t> col_ins_success_lat_p99;
std::vector<size_t> col_ins_success_lat_max;

std::vector<size_t> col_ins_fail_lat_min;
std::vector<size_t> col_ins_fail_lat_p5;
std::vector<size_t> col_ins_fail_lat_p9;
std::vector<size_t> col_ins_fail_lat_p99;
std::vector<size_t> col_ins_fail_lat_max;

struct KVGenConf
{
    uint64_t max_key{1};
    bool use_zip{false};
    double zip_skewness{0};
    std::string desc() const
    {
        if (use_zip)
        {
            return "zip-" + std::to_string(zip_skewness) + "-" +
                   std::to_string(max_key);
        }
        else
        {
            return "uni-" + std::to_string(max_key);
        }
    }
};
inline std::ostream &operator<<(std::ostream &os, const KVGenConf &c)
{
    os << "{kv_conf: max_key: " << c.max_key << ", use_zip: " << c.use_zip
       << ", skewness: " << c.zip_skewness << "}";
    return os;
}

struct BenchConfig
{
    std::string name;
    double insert_prob{0};
    double delete_prob{0};
    double get_prob{0};
    bool auto_extend{false};
    bool first_enter{true};
    bool server_should_leave{true};
    size_t thread_nr{1};
    size_t coro_nr{1};
    size_t test_nr{0};
    bool should_report{false};
    size_t initial_subtable_nr{1};
    bool subtable_use_mr{false};
    KVGenConf kv_gen_conf_;  // remember it for debuging
    IKVRandGenerator::pointer kv_g;

    void validate() const
    {
        double sum = insert_prob + delete_prob + get_prob;
        CHECK_DOUBLE_EQ(sum, 1);
        if (kv_g)
        {
            CHECK_EQ(kv_g.use_count(), 1)
                << "Does not allow sharing by threads. Multiple owner of this "
                   "allocator detected.";
        }
    }
    std::string conf_name() const
    {
        return name;
    }
    BenchConfig clone() const
    {
        BenchConfig ret;
        ret.name = name;
        ret.insert_prob = insert_prob;
        ret.delete_prob = delete_prob;
        ret.get_prob = get_prob;
        ret.auto_extend = auto_extend;
        ret.first_enter = first_enter;
        ret.server_should_leave = server_should_leave;
        ret.thread_nr = thread_nr;
        ret.coro_nr = coro_nr;
        ret.test_nr = test_nr;
        ret.should_report = should_report;
        ret.initial_subtable_nr = initial_subtable_nr;
        ret.subtable_use_mr = subtable_use_mr;
        ret.kv_gen_conf_ = kv_gen_conf_;
        ret.kv_g = kv_g->clone();
        return ret;
    }
    static BenchConfig get_empty_conf(const std::string &name,
                                      const KVGenConf &kv_conf)
    {
        BenchConfig conf;
        conf.name = name;
        conf.initial_subtable_nr = 1;
        conf.insert_prob = 0;
        conf.delete_prob = 0;
        conf.get_prob = 0;
        conf.kv_gen_conf_ = kv_conf;

        if (kv_conf.use_zip)
        {
            conf.kv_g = MehcachedZipfianRandGenerator::new_instance(
                0, kv_conf.max_key, kv_conf.zip_skewness);
        }
        else
        {
            conf.kv_g = UniformRandGenerator::new_instance(0, kv_conf.max_key);
        }

        return conf;
    }
    static BenchConfig get_default_conf(const std::string &name,
                                        const KVGenConf &kv_conf,
                                        size_t initial_subtable_nr,
                                        size_t test_nr,
                                        size_t thread_nr,
                                        size_t coro_nr,
                                        bool auto_extend)
    {
        auto conf = get_empty_conf(name, kv_conf);
        conf.initial_subtable_nr = initial_subtable_nr;
        conf.insert_prob = 0.25;
        conf.delete_prob = 0.25;
        conf.get_prob = 0.5;
        conf.auto_extend = auto_extend;
        conf.thread_nr = thread_nr;
        conf.coro_nr = coro_nr;
        conf.test_nr = test_nr;

        conf.validate();
        return conf;
    }
};
std::ostream &operator<<(std::ostream &os, const BenchConfig &conf)
{
    os << "{conf: name: " << conf.name << ", kv_conf: " << conf.kv_gen_conf_
       << ", ins: " << conf.insert_prob << ", get: " << conf.get_prob
       << ", del: " << conf.delete_prob << ", expand: " << conf.auto_extend
       << ", thread: " << conf.thread_nr << ", coro: " << conf.coro_nr
       << ", test: " << conf.test_nr
       << ", subtable_nr: " << conf.initial_subtable_nr
       << ", enter: " << conf.first_enter
       << ", leave: " << conf.server_should_leave
       << ", report: " << conf.should_report << "}";
    return os;
}

class BenchConfigFactory
{
public:
    static std::vector<BenchConfig> get_single_round_config(
        const std::string &name,
        size_t initial_subtable_nr,
        size_t test_nr,
        size_t thread_nr,
        size_t coro_nr,
        bool expand)
    {
        if (expand)
        {
            CHECK_EQ(thread_nr, 1);
            CHECK_EQ(coro_nr, 1);
        }
        auto warm_conf = BenchConfig::get_default_conf(
            name,
            KVGenConf{
                .max_key = kMaxKey, .use_zip = true, .zip_skewness = 0.99},
            initial_subtable_nr,
            test_nr,
            thread_nr,
            coro_nr,
            expand /* extend */);
        auto eval_conf = warm_conf.clone();
        eval_conf.should_report = true;

        return pipeline({warm_conf, eval_conf});
    }
    static std::vector<BenchConfig> get_expand_config(const std::string &name,
                                                      size_t fill_nr,
                                                      bool subtable_use_mr)
    {
        // fill the table with KVs
        auto conf = BenchConfig::get_empty_conf(
            name,
            KVGenConf{
                .max_key = kMaxKey, .use_zip = true, .zip_skewness = 0.99});
        conf.thread_nr = 1;
        conf.coro_nr = 1;
        conf.insert_prob = 1;
        conf.auto_extend = true;
        conf.test_nr = fill_nr;
        conf.should_report = true;
        conf.subtable_use_mr = subtable_use_mr;

        return pipeline({conf});
    }
    static std::vector<BenchConfig> get_multi_round_config(
        const std::string &name,
        size_t fill_nr,
        size_t test_nr,
        size_t thread_nr,
        size_t coro_nr)
    {
        auto kv_g_conf = KVGenConf{
            .max_key = kMaxKey, .use_zip = true, .zip_skewness = 0.99};

        // fill the table with KVs
        auto insert_conf =
            BenchConfig::get_empty_conf(name + ".load", kv_g_conf);
        insert_conf.thread_nr = 1;
        insert_conf.coro_nr = 1;
        insert_conf.insert_prob = 1;
        insert_conf.auto_extend = true;
        insert_conf.test_nr = fill_nr;
        insert_conf.should_report = false;

        auto query_warmup_conf =
            BenchConfig::get_empty_conf(name + ".query.warmup", kv_g_conf);
        query_warmup_conf.name = name + ".query.warmup";
        query_warmup_conf.thread_nr = thread_nr;
        query_warmup_conf.coro_nr = coro_nr;
        query_warmup_conf.get_prob = 1;
        query_warmup_conf.auto_extend = false;
        query_warmup_conf.test_nr = test_nr;
        query_warmup_conf.should_report = false;

        auto query_report_conf = query_warmup_conf.clone();
        query_report_conf.name = name + ".query";
        query_report_conf.should_report = true;

        auto mix_conf = BenchConfig::get_empty_conf(name + ".mix", kv_g_conf);
        mix_conf.thread_nr = thread_nr;
        mix_conf.coro_nr = coro_nr;
        mix_conf.get_prob = 0.8;
        mix_conf.delete_prob = 0.1;
        mix_conf.insert_prob = 0.1;
        mix_conf.auto_extend = false;
        mix_conf.test_nr = test_nr;
        mix_conf.should_report = true;

        return pipeline(
            {insert_conf, query_warmup_conf, query_report_conf, mix_conf});
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
typename RaceHashing<kE, kB, kS>::Handle::pointer gen_rdma_rhh(
    Patronus::pointer p,
    const RaceHashingHandleConfig &conf,
    bool auto_expand,
    GlobalAddress meta_gaddr,
    CoroContext *ctx)
{
    using HandleT = typename RaceHashing<kE, kB, kS>::Handle;

    auto tid = p->get_thread_id();
    auto dir_id = tid;

    DVLOG(1) << "Getting from race:meta_gaddr got " << meta_gaddr;

    auto handle_rdma_ctx =
        patronus::RdmaAdaptor::new_instance(kServerNodeId, dir_id, p, ctx);

    auto prhh = HandleT::new_instance(
        kServerNodeId, meta_gaddr, conf, auto_expand, handle_rdma_ctx);
    prhh->init();
    return prhh;
}

void init_allocator(Patronus::pointer p, size_t thread_nr)
{
    // for server to handle kv block allocation requests
    // give all to kv blocks
    auto tid = p->get_thread_id();
    auto dir_id = tid;
    CHECK_LT(tid, thread_nr);

    auto rh_buffer = p->get_user_reserved_buffer();

    auto thread_kvblock_pool_size = rh_buffer.size / thread_nr;
    void *thread_kvblock_pool_addr =
        rh_buffer.buffer + tid * thread_kvblock_pool_size;

    mem::SlabAllocatorConfig kvblock_slab_config;
    kvblock_slab_config.block_class = {hash::config::kKVBlockExpectSize};
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
};

template <size_t kE, size_t kB, size_t kS>
void test_basic_client_worker(
    Patronus::pointer p,
    size_t coro_id,
    CoroYield &yield,
    const BenchConfig &bench_conf,
    const RaceHashingHandleConfig &rhh_conf,
    GlobalAddress meta_gaddr,
    CoroExecutionContextWith<kMaxCoroNr, AdditionalCoroCtx> &ex)
{
    auto tid = p->get_thread_id();
    CoroContext ctx(tid, &yield, &ex.master(), coro_id);

    // TODO: the dtor of rhh has performance penalty.
    // dtor out of this function.
    bool auto_expand = bench_conf.auto_extend;
    auto rhh =
        gen_rdma_rhh<kE, kB, kS>(p, rhh_conf, auto_expand, meta_gaddr, &ctx);

    size_t ins_succ_nr = 0;
    size_t ins_retry_nr = 0;
    size_t ins_nomem_nr = 0;
    size_t del_succ_nr = 0;
    size_t del_retry_nr = 0;
    size_t del_not_found_nr = 0;
    size_t get_succ_nr = 0;
    size_t get_not_found_nr = 0;
    size_t executed_nr = 0;

    double insert_prob = bench_conf.insert_prob;
    double delete_prob = bench_conf.delete_prob;

    ChronoTimer timer;
    std::string key;
    std::string value;
    key.resize(sizeof(uint64_t));
    value.resize(8);
    CHECK_NOTNULL(bench_conf.kv_g)->gen_value(&value[0], 8);
    while (ex.get_private_data().thread_remain_task > 0)
    {
        bench_conf.kv_g->gen_key(&key[0], sizeof(uint64_t));

        if (true_with_prob(insert_prob))
        {
            // insert
            ChronoTimer op_timer;
            auto rc = rhh->put(key, value);
            ins_succ_nr += rc == kOk;
            ins_retry_nr += rc == kRetry;
            ins_nomem_nr += rc == kNoMem;
            DCHECK(rc == kOk || rc == kRetry || rc == kNoMem)
                << "** unexpected rc:" << rc;

            auto ns = op_timer.pin();
            ex.get_private_data().g.collect(ns);
            if (rc == kOk)
            {
                ex.get_private_data().insert_succ_lat.collect(ns);
            }
            else if (rc == kNoMem)
            {
                ex.get_private_data().insert_fail_lat.collect(ns);
            }
        }
        else if (true_with_prob(delete_prob))
        {
            // delete
            auto rc = rhh->del(key);
            del_succ_nr += rc == kOk;
            del_retry_nr += rc == kRetry;
            del_not_found_nr += rc == kNotFound;
            DCHECK(rc == kOk || rc == kRetry || rc == kNotFound)
                << "** unexpected rc: " << rc;
        }
        else
        {
            // get
            ChronoTimer op_timer;
            std::string got_value;
            auto rc = rhh->get(key, got_value);
            get_succ_nr += rc == kOk;
            get_not_found_nr += rc == kNotFound;
            DCHECK(rc == kOk || rc == kNotFound) << "** unexpected rc: " << rc;

            auto ns = op_timer.pin();
            ex.get_private_data().g.collect(ns);
            if (rc == kOk)
            {
                ex.get_private_data().read_hit_lat.collect(ns);
            }
            else
            {
                ex.get_private_data().read_miss_lat.collect(ns);
            }
        }

        executed_nr++;
        ex.get_private_data().thread_remain_task--;
    }
    auto ns = timer.pin();

    ex.get_private_data().get_nr += get_succ_nr + get_not_found_nr;
    ex.get_private_data().get_succ_nr += get_succ_nr;
    ex.get_private_data().put_nr += ins_succ_nr + ins_nomem_nr + ins_retry_nr;
    ex.get_private_data().put_succ_nr += ins_succ_nr;
    ex.get_private_data().del_nr +=
        del_not_found_nr + del_succ_nr + del_retry_nr;
    ex.get_private_data().del_succ_nr += del_succ_nr;

    VLOG(1) << "[bench] insert: succ: " << ins_succ_nr
            << ", retry: " << ins_retry_nr << ", nomem: " << ins_nomem_nr
            << ", del: succ: " << del_succ_nr << ", retry: " << del_retry_nr
            << ", not found: " << del_not_found_nr
            << ", get: succ: " << get_succ_nr
            << ", not found: " << get_not_found_nr
            << ". executed: " << executed_nr << ", take: " << ns
            << " ns. ctx: " << ctx;

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
                      const RaceHashingHandleConfig &rhh_conf,
                      uint64_t key)
{
    auto tid = p->get_thread_id();

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
        col_x_kvdist.push_back(bench_conf.kv_gen_conf_.desc());
        col_x_thread_nr.push_back(bench_conf.thread_nr);
        col_x_coro_nr.push_back(bench_conf.coro_nr);
        col_x_put_rate.push_back(bench_conf.insert_prob);
        col_x_del_rate.push_back(bench_conf.delete_prob);
        col_x_get_rate.push_back(bench_conf.get_prob);
        col_test_op_nr.push_back(actual_test_nr);
        col_ns.push_back(ns);

        double get_succ_rate = 1.0 * ex.get_private_data().get_succ_nr /
                               ex.get_private_data().get_nr;
        col_get_succ_rate.push_back(get_succ_rate);
        double put_succ_rate = 1.0 * ex.get_private_data().put_succ_nr /
                               ex.get_private_data().put_nr;
        col_put_succ_rate.push_back(put_succ_rate);
        double del_succ_rate = 1.0 * ex.get_private_data().del_succ_nr /
                               ex.get_private_data().del_nr;
        col_del_succ_rate.push_back(del_succ_rate);

        {
            const auto &m = ex.get_private_data().g;
            col_g_lat_min.push_back(m.min());
            col_g_lat_p5.push_back(m.percentile(0.5));
            col_g_lat_p9.push_back(m.percentile(0.9));
            col_g_lat_p99.push_back(m.percentile(0.99));
            col_g_lat_max.push_back(m.max());
        }
        {
            const auto &m = ex.get_private_data().read_hit_lat;
            col_read_hit_lat_min.push_back(m.min());
            col_read_hit_lat_p5.push_back(m.percentile(0.5));
            col_read_hit_lat_p9.push_back(m.percentile(0.9));
            col_read_hit_lat_p99.push_back(m.percentile(0.99));
            col_read_hit_lat_max.push_back(m.max());
        }
        {
            const auto &m = ex.get_private_data().read_miss_lat;
            col_read_miss_lat_min.push_back(m.min());
            col_read_miss_lat_p5.push_back(m.percentile(0.5));
            col_read_miss_lat_p9.push_back(m.percentile(0.9));
            col_read_miss_lat_p99.push_back(m.percentile(0.99));
            col_read_miss_lat_max.push_back(m.max());
        }
        {
            const auto &m = ex.get_private_data().insert_succ_lat;
            col_ins_success_lat_min.push_back(m.min());
            col_ins_success_lat_p5.push_back(m.percentile(0.5));
            col_ins_success_lat_p9.push_back(m.percentile(0.9));
            col_ins_success_lat_p99.push_back(m.percentile(0.99));
            col_ins_success_lat_max.push_back(m.max());
        }
        {
            const auto &m = ex.get_private_data().insert_fail_lat;
            col_ins_fail_lat_min.push_back(m.min());
            col_ins_fail_lat_p5.push_back(m.percentile(0.5));
            col_ins_fail_lat_p9.push_back(m.percentile(0.9));
            col_ins_fail_lat_p99.push_back(m.percentile(0.99));
            col_ins_fail_lat_max.push_back(m.max());
        }
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
        init_allocator(p, thread_nr);
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
    rhh_configs.push_back(RaceHashingConfigFactory::get_unprotected(
        "unprot", 0 /* no batching */));
    rhh_configs.push_back(RaceHashingConfigFactory::get_mw_protected(
        "patronus", 0 /* no batch */));
    rhh_configs.push_back(
        RaceHashingConfigFactory::get_mr_protected("mr", 0 /* no batch */));

    for (const auto &rhh_conf : rhh_configs)
    {
        LOG_IF(INFO, is_master)
            << "[bench] benching single thread for " << rhh_conf;
        key++;
        auto basic_conf = BenchConfigFactory::get_single_round_config(
            "single_basic", 1, 1_M, 1, 1, true);
        if (is_client)
        {
            for (const auto &bench_conf : basic_conf)
            {
                bench_conf.validate();
                LOG_IF(INFO, is_master)
                    << "[bench] running conf: " << bench_conf;
                benchmark_client<4, 16, 16>(
                    p, bar, is_master, bench_conf, rhh_conf, key);
            }
        }
        else
        {
            benchmark_server<4, 16, 16>(p, bar, is_master, basic_conf, key);
        }

        LOG_IF(INFO, is_master)
            << "[bench] benching multiple threads for " << rhh_conf;
        constexpr size_t capacity = RaceHashing<4, 16, 16>::max_capacity();
        key++;
        auto multithread_conf = BenchConfigFactory::get_multi_round_config(
            "multithread_basic", capacity, 10_M, kThreadNr, 1);
        if (is_client)
        {
            for (const auto &bench_conf : multithread_conf)
            {
                bench_conf.validate();
                LOG_IF(INFO, is_master)
                    << "[bench] running conf: " << bench_conf;
                benchmark_client<4, 16, 16>(
                    p, bar, is_master, bench_conf, rhh_conf, key);
            }
        }
        else
        {
            benchmark_server<4, 16, 16>(
                p, bar, is_master, multithread_conf, key);
        }

        // LOG_IF(INFO, is_master) << "[bench] benching expansion mw";
        // {
        //     constexpr size_t capacity = RaceHashing<4, 16,
        //     16>::max_capacity(); key++; auto expand_mw_conf =
        //     BenchConfigFactory::get_expand_config(
        //         "expand(mw)", capacity, false /* st use mr */);
        //     if (is_client)
        //     {
        //         for (const auto &conf : expand_mw_conf)
        //         {
        //             LOG_IF(INFO, is_master) << "[bench] running conf: " <<
        //             conf; benchmark_client<4, 16, 16>(p, bar, is_master,
        //             conf, key);
        //         }
        //     }
        //     else
        //     {
        //         benchmark_server<4, 16, 16>(p, bar, is_master,
        //         expand_mw_conf, key);
        //     }
        // }

        // LOG_IF(INFO, is_master) << "[bench] benching expansion mr";
        // {
        //     constexpr size_t capacity = RaceHashing<4, 16,
        //     16>::max_capacity(); key++; auto expand_mr_conf =
        //     BenchConfigFactory::get_expand_config(
        //         "expand(mr)", capacity, true /* st use mr*/);
        //     if (is_client)
        //     {
        //         for (const auto &conf : expand_mr_conf)
        //         {
        //             LOG_IF(INFO, is_master) << "[bench] running conf: " <<
        //             conf; benchmark_client<4, 16, 16>(p, bar, is_master,
        //             conf, key);
        //         }
        //     }
        //     else
        //     {
        //         benchmark_server<4, 16, 16>(p, bar, is_master,
        //         expand_mr_conf, key);
        //     }
        // }
    }
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    PatronusConfig pconfig;
    pconfig.machine_nr = kMachineNr;
    pconfig.block_class = {2_MB, 4_KB};
    pconfig.block_ratio = {0.5, 0.5};
    pconfig.reserved_buffer_size = 2_GB;
    pconfig.lease_buffer_size = (kDSMCacheSize - 2_GB) / 2;
    pconfig.alloc_buffer_size = (kDSMCacheSize - 2_GB) / 2;

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
    df.load_column<std::string>("x_kv_dist", std::move(col_x_kvdist));
    df.load_column<size_t>("x_thread_nr", std::move(col_x_thread_nr));
    df.load_column<size_t>("x_coro_nr", std::move(col_x_coro_nr));
    df.load_column<double>("x_put_rate", std::move(col_x_put_rate));
    df.load_column<double>("x_del_rate", std::move(col_x_del_rate));
    df.load_column<double>("x_get_rate", std::move(col_x_get_rate));
    df.load_column<size_t>("test_nr(total)", std::move(col_test_op_nr));
    df.load_column<size_t>("test_ns(total)", std::move(col_ns));

    df.load_column<double>("get_succ_rate", std::move(col_get_succ_rate));
    df.load_column<double>("put_succ_rate", std::move(col_put_succ_rate));
    df.load_column<double>("del_succ_rate", std::move(col_del_succ_rate));

    df.load_column<size_t>("global_lat(min)", std::move(col_g_lat_min));
    df.load_column<size_t>("global_lat(p5)", std::move(col_g_lat_p5));
    df.load_column<size_t>("global_lat(p9)", std::move(col_g_lat_p9));
    df.load_column<size_t>("global_lat(p99)", std::move(col_g_lat_p99));
    df.load_column<size_t>("global_lat(max)", std::move(col_g_lat_max));

    df.load_column<size_t>("read_hit_lat(min)",
                           std::move(col_read_hit_lat_min));
    df.load_column<size_t>("read_hit_lat(p5)", std::move(col_read_hit_lat_p5));
    df.load_column<size_t>("read_hit_lat(p9)", std::move(col_read_hit_lat_p9));
    df.load_column<size_t>("read_hit_lat(p99)",
                           std::move(col_read_hit_lat_p99));
    df.load_column<size_t>("read_hit_lat(max)",
                           std::move(col_read_hit_lat_max));

    df.load_column<size_t>("read_miss_lat(min)",
                           std::move(col_read_miss_lat_min));
    df.load_column<size_t>("read_miss_lat(p5)",
                           std::move(col_read_miss_lat_p5));
    df.load_column<size_t>("read_miss_lat(p9)",
                           std::move(col_read_miss_lat_p9));
    df.load_column<size_t>("read_miss_lat(p99)",
                           std::move(col_read_miss_lat_p99));
    df.load_column<size_t>("read_miss_lat(max)",
                           std::move(col_read_miss_lat_max));

    df.load_column<size_t>("ins_succ_lat(min)",
                           std::move(col_ins_success_lat_min));
    df.load_column<size_t>("ins_succ_lat(p5)",
                           std::move(col_ins_success_lat_p5));
    df.load_column<size_t>("ins_succ_lat(p9)",
                           std::move(col_ins_success_lat_p9));
    df.load_column<size_t>("ins_succ_lat(p99)",
                           std::move(col_ins_success_lat_p99));
    df.load_column<size_t>("ins_succ_lat(max)",
                           std::move(col_ins_success_lat_max));

    df.load_column<size_t>("ins_fail_lat(min)",
                           std::move(col_ins_fail_lat_min));
    df.load_column<size_t>("ins_fail_lat(p5)", std::move(col_ins_fail_lat_p5));
    df.load_column<size_t>("ins_fail_lat(p9)", std::move(col_ins_fail_lat_p9));
    df.load_column<size_t>("ins_fail_lat(p99)",
                           std::move(col_ins_fail_lat_p99));
    df.load_column<size_t>("ins_fail_lat(max)",
                           std::move(col_ins_fail_lat_max));

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