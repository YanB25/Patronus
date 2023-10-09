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
#include "util/Pre.h"
#include "util/Rand.h"

using namespace patronus::hash;
using namespace util::literals;
using namespace patronus;
using namespace hmdf;

[[maybe_unused]] constexpr uint16_t kClientNodeId = 1;
[[maybe_unused]] constexpr uint16_t kServerNodeId = 0;
constexpr uint32_t kMachineNr = 2;
constexpr static size_t kThreadNr = 8;
constexpr static size_t kMaxCoroNr = 16;
constexpr static uint64_t kMaxKey = 10_M;
constexpr static uint32_t kMonitorThreadBindCore = 16;
static_assert(kMonitorThreadBindCore >= kThreadNr);

DEFINE_string(exec_meta, "", "The meta data of this execution");
std::map<std::string, std::string> extra;

std::vector<std::string> col_idx;
std::vector<size_t> col_inserted_nr;
std::vector<double> col_load_factor;

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

struct ThreadCommunication
{
    std::atomic<uint64_t> inserted_nr{};
    std::atomic<bool> finished{false};
    std::atomic<bool> started{false};
    void clear()
    {
        inserted_nr = 0;
        finished = false;
        started = false;
    }
};

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
    static std::vector<BenchConfig> get_expand_config(
        const std::string &name, size_t fill_nr, size_t kvblock_expect_size)
    {
        // fill the table with KVs
        auto conf = BenchConfig::get_empty_conf(
            name,
            KVGenConf{.max_key = kMaxKey,
                      .use_zip = false,
                      .zip_skewness = 0,
                      .kvblock_expect_size = kvblock_expect_size});
        conf.thread_nr = 1;
        conf.coro_nr = 1;
        conf.insert_prob = 1;
        conf.auto_extend = true;
        conf.test_nr = fill_nr;
        conf.should_report = true;
        conf.subtable_use_mr = false;

        return pipeline({conf});
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

    auto handle_rdma_ctx = patronus::RdmaAdaptor::new_instance(
        kServerNodeId, dir_id, p, conf.bypass_prot, false /* two sided */, ctx);

    auto prhh = HandleT::new_instance(
        kServerNodeId, meta_gaddr, conf, auto_expand, handle_rdma_ctx);
    prhh->init();
    return prhh;
}

void init_allocator(Patronus::pointer p,
                    size_t thread_nr,
                    size_t kvblock_expect_size)
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

struct AdditionalCoroCtx
{
    ssize_t thread_remain_task{0};
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
    const BenchConfig &bench_conf,
    const RaceHashingHandleConfig &rhh_conf,
    GlobalAddress meta_gaddr,
    CoroExecutionContextWith<kMaxCoroNr, AdditionalCoroCtx> &ex,
    ThreadCommunication &tc)
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
    size_t rdma_protection_nr = 0;
    size_t executed_nr = 0;

    double insert_prob = bench_conf.insert_prob;
    double delete_prob = bench_conf.delete_prob;

    ChronoTimer timer;
    std::string key;
    std::string value;
    key.resize(sizeof(uint64_t));
    value.resize(8);
    CHECK_NOTNULL(bench_conf.kv_g)->gen_value(&value[0], 8);
    util::TraceManager tm(0);
    while (true)
    {
        auto trace = tm.trace("test");
        trace.set("tid", std::to_string(tid));
        trace.set("k", key);
        trace.set("v", value);

        bench_conf.kv_g->gen_key(&key[0], sizeof(uint64_t));

        if (true_with_prob(insert_prob))
        {
            trace.set("op", "put");
            // insert
            auto rc = rhh->put(key, value, trace);
            ins_succ_nr += rc == kOk;
            ins_retry_nr += rc == kRetry;
            ins_nomem_nr += rc == kNoMem;
            rdma_protection_nr += rc == kRdmaProtectionErr;
            DCHECK(rc == kOk || rc == kRetry || rc == kNoMem ||
                   rc == kRdmaProtectionErr)
                << "** unexpected rc:" << rc;
            DCHECK_NE(rc, kRdmaProtectionErr);
            tc.inserted_nr += rc == kOk;

            if (unlikely(rc == RC::kMockCrashed))
            {
                LOG(INFO) << "[bench] MOCK crashed. inserted: " << ins_succ_nr;
                extra["fault_at"] = std::to_string(ins_succ_nr);
            }

            if (unlikely(rc == kRetry))
            {
                continue;
            }
            if (rc == kNoMem)
            {
                LOG(WARNING) << "[bench] no mem! finished benching.";
                break;
            }
        }
        else if (true_with_prob(delete_prob))
        {
            // delete
            auto rc = rhh->del(key);
            del_succ_nr += rc == kOk;
            del_retry_nr += rc == kRetry;
            del_not_found_nr += rc == kNotFound;
            rdma_protection_nr += rc == kRdmaProtectionErr;
            DCHECK(rc == kOk || rc == kRetry || rc == kNotFound ||
                   rc == kRdmaProtectionErr)
                << "** unexpected rc: " << rc;
        }
        else
        {
            // get
            std::string got_value;
            auto rc = rhh->get(key, got_value);
            get_succ_nr += rc == kOk;
            get_not_found_nr += rc == kNotFound;
            rdma_protection_nr += rc == kRdmaProtectionErr;
            DCHECK(rc == kOk || rc == kNotFound || rc == kRdmaProtectionErr)
                << "** unexpected rc: " << rc;
        }
        executed_nr++;
        ex.get_private_data().thread_remain_task--;
    }
    auto ns = timer.pin();

    auto &comm = ex.get_private_data();
    comm.get_nr += get_succ_nr + get_not_found_nr;
    comm.get_succ_nr += get_succ_nr;
    comm.put_nr += ins_succ_nr + ins_retry_nr + ins_nomem_nr;
    comm.put_succ_nr += ins_succ_nr;
    comm.del_nr += del_succ_nr + del_retry_nr + del_not_found_nr;
    comm.del_succ_nr += del_succ_nr;
    comm.rdma_protection_nr += rdma_protection_nr;

    LOG(INFO) << "[bench] insert: succ: " << ins_succ_nr
              << ", retry: " << ins_retry_nr << ", nomem: " << ins_nomem_nr
              << ", del: succ: " << del_succ_nr << ", retry: " << del_retry_nr
              << ", not found: " << del_not_found_nr
              << ", get: succ: " << get_succ_nr
              << ", not found: " << get_not_found_nr
              << ", rdma_protection_nr: " << rdma_protection_nr
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
    std::ignore = test_nr;
    std::ignore = atm_task_nr;
    auto tid = p->get_thread_id();

    CoroContext mctx(tid, &yield, ex.workers());
    CHECK(mctx.is_master());

    for (size_t i = 0; i < coro_nr; ++i)
    {
        mctx.yield_to_worker(i);
    }

    coro_t coro_buf[2 * kMaxCoroNr];
    while (true)
    {
        auto nr = p->try_get_client_continue_coros(coro_buf, 2 * kMaxCoroNr);
        for (size_t i = 0; i < nr; ++i)
        {
            auto coro_id = coro_buf[i];
            // DVLOG(1) << "[bench] yielding due to CQE: " << (int) coro_id;
            mctx.yield_to_worker(coro_id);
        }

        if (ex.is_finished_all())
        {
            break;
        }
    }
}

std::atomic<ssize_t> g_total_test_nr;
GlobalAddress g_meta_gaddr;
template <size_t kE, size_t kB, size_t kS>
void benchmark_client(Patronus::pointer p,
                      boost::barrier &bar,
                      ThreadCommunication &tc,
                      bool is_master,
                      const BenchConfig &bench_conf,
                      const RaceHashingHandleConfig &rhh_conf,
                      uint64_t key)
{
    auto coro_nr = bench_conf.coro_nr;
    auto thread_nr = bench_conf.thread_nr;
    bool first_enter = bench_conf.first_enter;
    bool server_should_leave = bench_conf.server_should_leave;
    CHECK_LE(coro_nr, kMaxCoroNr);
    size_t actual_test_nr = bench_conf.test_nr * rhh_conf.test_nr_scale_factor;

    std::thread monitor_thread;

    auto tid = p->get_thread_id();
    if (is_master)
    {
        // init here by master
        tc.clear();

        monitor_thread = std::thread([&tc]() {
            bindCore(kMonitorThreadBindCore);

            constexpr static auto kMonitorEach = 100us;
            Sequence<uint64_t> inserted_nr_seq;

            while (!tc.started)
            {
                util::time::busy_wait_for(kMonitorEach / 10);
            }
            auto now = std::chrono::steady_clock::now();
            while (!tc.finished)
            {
                auto inserted_nr =
                    tc.inserted_nr.load(std::memory_order_relaxed);
                inserted_nr_seq.push_back(inserted_nr);
                util::time::busy_wait_until(now, kMonitorEach);
            }

            auto vec = inserted_nr_seq.to_vector();
            LOG(INFO) << "[bench] seq(1000): " << util::pre_vec(vec, 1000);
            col_inserted_nr = std::move(vec);
            col_idx.reserve(col_inserted_nr.size());
            for (size_t i = 0; i < col_inserted_nr.size(); ++i)
            {
                col_idx.emplace_back(std::to_string(i));
                double l = 1.0 * col_inserted_nr[i] / col_inserted_nr.back();
                col_load_factor.push_back(l);
            }
            extra["epoch"] = std::to_string(util::time::to_ns(kMonitorEach));
        });

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

    if (is_master)
    {
        tc.started = true;
    }

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
            ex.worker(i) =
                CoroCall([p,
                          coro_id = i,
                          &bench_conf,
                          &rhh_conf,
                          &ex,
                          &tc,
                          meta_gaddr = g_meta_gaddr](CoroYield &yield) {
                    test_basic_client_worker<kE, kB, kS>(p,
                                                         coro_id,
                                                         yield,
                                                         bench_conf,
                                                         rhh_conf,
                                                         meta_gaddr,
                                                         ex,
                                                         tc);
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

    if (is_master)
    {
        tc.finished = true;
        monitor_thread.join();
    }

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

template <size_t kE, size_t kB, size_t kS>
void benchmark_server(Patronus::pointer p,
                      boost::barrier &bar,
                      bool is_master,
                      const std::vector<BenchConfig> &confs,
                      uint64_t key)
{
    auto thread_nr = confs[0].thread_nr;
    auto initial_subtable = confs[0].initial_subtable_nr;
    auto kvblock_expect_size = confs[0].kv_gen_conf_.kvblock_expect_size;
    for (const auto &conf : confs)
    {
        thread_nr = std::max(thread_nr, conf.thread_nr);
        DCHECK_EQ(initial_subtable, conf.initial_subtable_nr);
        DCHECK_EQ(conf.kv_gen_conf_.kvblock_expect_size, kvblock_expect_size);
    }

    using RaceHashingT = RaceHashing<kE, kB, kS>;
    auto tid = p->get_thread_id();

    typename RaceHashingT::pointer rh;
    if (tid < thread_nr)
    {
        init_allocator(p, thread_nr, kvblock_expect_size);
    }
    else
    {
        CHECK(!is_master) << "** master not initing allocator";
    }
    if (is_master)
    {
        p->finished(key);
        rh = gen_rdma_rh<kE, kB, kS>(p, initial_subtable);
        LOG(INFO) << "[bench] rh: " << pre_rh_explain(*rh);
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

void benchmark(Patronus::pointer p,
               boost::barrier &bar,
               ThreadCommunication &tc,
               bool is_client)
{
    uint64_t key = 0;
    bool is_master = p->get_thread_id() == 0;
    bar.wait();

    auto lease_time = 10ms;
    extra["lease_time"] = std::to_string(util::time::to_ns(lease_time));

    // set the rhh we like to test
    std::vector<RaceHashingHandleConfig> rhh_configs;
    for (size_t kvblock_expect_size : {64_B})
    {
        rhh_configs.push_back(
            RaceHashingConfigFactory::get_mw_protected_expand_fault_tolerance(
                "patronus-lease",
                kvblock_expect_size,
                1 /* crash 1 times */,
                lease_time));
    }

    for (const auto &rhh_conf : rhh_configs)
    {
        {
            LOG_IF(INFO, is_master)
                << "[bench] benching single thread for " << rhh_conf;
            key++;
            auto max_capacity = RaceHashing<4, 16, 16>::max_capacity();
            auto basic_conf = BenchConfigFactory::get_expand_config(
                "single_basic", max_capacity - 5, 64);
            if (is_client)
            {
                for (const auto &bench_conf : basic_conf)
                {
                    bench_conf.validate();
                    LOG_IF(INFO, is_master)
                        << "[sub-conf] running conf: " << bench_conf;
                    benchmark_client<4, 16, 16>(
                        p, bar, tc, is_master, bench_conf, rhh_conf, key);
                }
            }
            else
            {
                benchmark_server<4, 16, 16>(p, bar, is_master, basic_conf, key);
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
    pconfig.lease_buffer_size = (::config::kDefaultDSMSize - 2_GB) / 2;
    pconfig.alloc_buffer_size = (::config::kDefaultDSMSize - 2_GB) / 2;

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

    ThreadCommunication tc;

    for (size_t i = 0; i < kThreadNr - 1; ++i)
    {
        threads.emplace_back([patronus, &bar, &tc, is_client]() {
            if (is_client)
            {
                patronus->registerClientThread();
            }
            else
            {
                patronus->registerServerThread();
            }
            benchmark(patronus, bar, tc, is_client);
        });
    }
    benchmark(patronus, bar, tc, is_client);

    for (auto &t : threads)
    {
        t.join();
    }

    StrDataFrame df;
    df.load_index(std::move(col_idx));
    df.load_column<size_t>("inserted", std::move(col_inserted_nr));
    df.load_column<double>("load_factor", std::move(col_load_factor));

    auto filename = binary_to_csv_filename(argv[0], FLAGS_exec_meta, extra);
    df.write<std::ostream, std::string, size_t, double>(std::cout,
                                                        io_format::csv2);
    df.write<std::string, size_t, double>(filename.c_str(), io_format::csv2);

    patronus->keeper_barrier("finished", 100ms);

    LOG(INFO) << "finished. ctrl+C to quit.";
}