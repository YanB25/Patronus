#include <algorithm>
#include <random>
#include <set>

#include "Common.h"
#include "boost/thread/barrier.hpp"
#include "glog/logging.h"
#include "patronus/Patronus.h"
#include "patronus/RdmaAdaptor.h"
#include "patronus/memory/direct_allocator.h"
#include "thirdparty/racehashing/hashtable.h"
#include "thirdparty/racehashing/hashtable_handle.h"
#include "thirdparty/racehashing/utils.h"
#include "util/DataFrameF.h"
#include "util/Rand.h"

using namespace patronus::hash;
using namespace define::literals;
using namespace patronus;
using namespace hmdf;

[[maybe_unused]] constexpr uint16_t kClientNodeId = 1;
[[maybe_unused]] constexpr uint16_t kServerNodeId = 0;
constexpr uint32_t kMachineNr = 2;
constexpr static size_t kThreadNr = 8;
constexpr static size_t kMaxCoroNr = 16;

DEFINE_string(exec_meta, "", "The meta data of this execution");

std::vector<std::string> col_idx;
std::vector<size_t> col_x_thread_nr;
std::vector<size_t> col_x_coro_nr;
std::vector<double> col_x_put_rate;
std::vector<double> col_x_del_rate;
std::vector<double> col_x_get_rate;
std::vector<size_t> col_test_op_nr;
std::vector<size_t> col_ns;

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
    void validate()
    {
        double sum = insert_prob + delete_prob + get_prob;
        CHECK_DOUBLE_EQ(sum, 1);
    }
    std::string conf_name() const
    {
        return name;
    }
    static BenchConfig get_default_conf(const std::string &name,
                                        size_t initial_subtable_nr,
                                        size_t test_nr,
                                        size_t thread_nr,
                                        size_t coro_nr,
                                        bool auto_extend)
    {
        BenchConfig conf;
        conf.name = name;
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
    os << "{conf: name: " << conf.name << ", ins: " << conf.insert_prob
       << ", get: " << conf.get_prob << ", del: " << conf.delete_prob
       << ", expand: " << conf.auto_extend << ", thread: " << conf.thread_nr
       << ", coro: " << conf.coro_nr << ", test: " << conf.test_nr
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
        auto warm_conf = BenchConfig::get_default_conf(name,
                                                       initial_subtable_nr,
                                                       test_nr,
                                                       thread_nr,
                                                       coro_nr,
                                                       expand /* extend */);
        auto eval_conf = warm_conf;
        eval_conf.should_report = true;

        return pipeline({warm_conf, eval_conf});
    }
    static std::vector<BenchConfig> get_multi_round_config(
        const std::string &name,
        size_t fill_nr,
        size_t test_nr,
        size_t thread_nr,
        size_t coro_nr)
    {
        // fill the table with KVs
        BenchConfig insert_conf;
        insert_conf.name = name + ".insert";
        insert_conf.thread_nr = 1;
        insert_conf.coro_nr = 1;
        insert_conf.insert_prob = 1;
        insert_conf.auto_extend = true;
        insert_conf.test_nr = fill_nr;
        insert_conf.should_report = false;

        BenchConfig query_warmup_conf;
        query_warmup_conf.name = name + ".query.warmup";
        query_warmup_conf.thread_nr = thread_nr;
        query_warmup_conf.coro_nr = coro_nr;
        query_warmup_conf.get_prob = 1;
        query_warmup_conf.auto_extend = false;
        query_warmup_conf.test_nr = test_nr;
        query_warmup_conf.should_report = false;

        auto query_report_conf = query_warmup_conf;
        query_report_conf.name = name + ".query";
        query_report_conf.should_report = true;

        return pipeline({insert_conf, query_warmup_conf, query_report_conf});
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
    Patronus::pointer p, bool auto_expand, CoroContext *ctx)
{
    using HandleT = typename RaceHashing<kE, kB, kS>::Handle;

    auto tid = p->get_thread_id();
    auto dir_id = tid;

    auto meta_gaddr = p->get_object<GlobalAddress>("race:meta_gaddr", 1ms);
    DVLOG(1) << "Getting from race:meta_gaddr got " << meta_gaddr;

    RaceHashingHandleConfig handle_conf;
    handle_conf.auto_expand = auto_expand;
    handle_conf.auto_update_dir = auto_expand;
    auto handle_rdma_ctx =
        patronus::RdmaAdaptor::new_instance(kServerNodeId, dir_id, p, ctx);

    auto prhh = HandleT::new_instance(
        kServerNodeId, meta_gaddr, handle_conf, handle_rdma_ctx);
    prhh->init();
    return prhh;
}

void init_allocator(Patronus::pointer p, size_t thread_nr)
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
    kvblock_slab_config.block_class = {hash::config::kKVBlockAllocBatchSize};
    kvblock_slab_config.block_ratio = {1.0};
    auto kvblock_allocator =
        mem::SlabAllocator::new_instance(thread_kvblock_pool_addr,
                                         thread_kvblock_pool_size,
                                         kvblock_slab_config);
    p->reg_allocator(hash::config::kAllocHintKVBlock, kvblock_allocator);
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
};

template <size_t kE, size_t kB, size_t kS>
void test_basic_client_worker(
    Patronus::pointer p,
    size_t coro_id,
    CoroYield &yield,
    const BenchConfig &conf,
    CoroExecutionContextWith<kMaxCoroNr, AdditionalCoroCtx> &ex)
{
    auto auto_expand = conf.auto_extend;

    auto tid = p->get_thread_id();
    CoroContext ctx(tid, &yield, &ex.master(), coro_id);

    auto rhh = gen_rdma_rhh<kE, kB, kS>(p, auto_expand, &ctx);

    constexpr static size_t kKeySize = 3;
    constexpr static size_t kValueSize = 3;
    char key_buf[kKeySize + 5];
    char val_buf[kValueSize + 5];

    size_t ins_succ_nr = 0;
    size_t ins_retry_nr = 0;
    size_t ins_nomem_nr = 0;
    size_t del_succ_nr = 0;
    size_t del_retry_nr = 0;
    size_t del_not_found_nr = 0;
    size_t get_succ_nr = 0;
    size_t get_not_found_nr = 0;
    size_t executed_nr = 0;

    std::map<std::string, std::string> inserted;
    std::set<std::string> keys;

    double insert_prob = conf.insert_prob;
    double delete_prob = conf.delete_prob;

    ChronoTimer timer;
    while (ex.get_private_data().thread_remain_task > 0)
    {
        fast_pseudo_fill_buf(key_buf, kKeySize);
        fast_pseudo_fill_buf(val_buf, kValueSize);
        std::string key(key_buf, kKeySize);
        std::string value(val_buf, kValueSize);

        if (true_with_prob(insert_prob))
        {
            // insert
            auto rc = rhh->put(key, value);
            ins_succ_nr += rc == kOk;
            ins_retry_nr += rc == kRetry;
            ins_nomem_nr += rc == kNoMem;
            DCHECK(rc == kOk || rc == kRetry || rc == kNoMem)
                << "** unexpected rc:" << rc;
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
            std::string got_value;
            auto rc = rhh->get(key, got_value);
            get_succ_nr += rc == kOk;
            get_not_found_nr += rc == kNotFound;
            DCHECK(rc == kOk || rc == kNotFound) << "** unexpected rc: " << rc;
        }
        executed_nr++;
        ex.get_private_data().thread_remain_task--;
    }
    auto ns = timer.pin();

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

std::atomic<ssize_t> total_test_nr;

template <size_t kE, size_t kB, size_t kS>
void benchmark_client(Patronus::pointer p,
                      boost::barrier &bar,
                      bool is_master,
                      const BenchConfig &conf,
                      uint64_t key)
{
    auto coro_nr = conf.coro_nr;
    auto thread_nr = conf.thread_nr;
    bool first_enter = conf.first_enter;
    bool server_should_leave = conf.server_should_leave;
    CHECK_LE(coro_nr, kMaxCoroNr);

    auto tid = p->get_thread_id();
    if (is_master)
    {
        // init here by master
        total_test_nr = conf.test_nr;
        if (first_enter)
        {
            p->keeper_barrier("server_ready-" + std::to_string(key), 100ms);
        }
    }
    bar.wait();

    ChronoTimer timer;
    if (tid < thread_nr)
    {
        CoroExecutionContextWith<kMaxCoroNr, AdditionalCoroCtx> ex;
        ex.get_private_data().thread_remain_task = 0;
        for (size_t i = coro_nr; i < kMaxCoroNr; ++i)
        {
            // no that coro, so directly finished.
            ex.worker_finished(i);
        }
        for (size_t i = 0; i < coro_nr; ++i)
        {
            ex.worker(i) =
                CoroCall([p, coro_id = i, &conf, &ex](CoroYield &yield) {
                    test_basic_client_worker<kE, kB, kS>(
                        p, coro_id, yield, conf, ex);
                });
        }
        auto &master = ex.master();
        master = CoroCall(
            [p, &ex, test_nr = conf.test_nr, coro_nr](CoroYield &yield) {
                test_basic_client_master(
                    p, yield, test_nr, total_test_nr, coro_nr, ex);
            });

        master();
    }
    bar.wait();
    auto ns = timer.pin();

    double ops = 1e9 * conf.test_nr / ns;
    double avg_ns = 1.0 * ns / conf.test_nr;
    LOG_IF(INFO, is_master)
        << "[bench] total op: " << conf.test_nr << ", ns: " << ns
        << ", ops: " << ops << ", avg " << avg_ns << " ns";

    if (is_master && server_should_leave)
    {
        LOG(INFO) << "p->finished(" << key << ")";
        p->finished(key);
    }

    if (is_master && conf.should_report)
    {
        col_idx.push_back(conf.conf_name());
        col_x_thread_nr.push_back(conf.thread_nr);
        col_x_coro_nr.push_back(conf.coro_nr);
        col_x_put_rate.push_back(conf.insert_prob);
        col_x_del_rate.push_back(conf.delete_prob);
        col_x_get_rate.push_back(conf.get_prob);
        col_test_op_nr.push_back(conf.test_nr);
        col_ns.push_back(ns);
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
        LOG(INFO) << "[bench] rh: " << pre_rh_explain(*rh);
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

std::atomic<size_t> battle_master{0};
void benchmark(Patronus::pointer p, boost::barrier &bar, bool is_client)
{
    uint64_t key = 0;
    bool is_master = battle_master.fetch_add(1) == 0;
    bar.wait();

    LOG_IF(INFO, is_master) << "[bench] benching single thread";
    key++;
    auto basic_conf = BenchConfigFactory::get_single_round_config(
        "single_basic", 1, 10_K, 1, 1, true);
    if (is_client)
    {
        for (const auto &conf : basic_conf)
        {
            LOG_IF(INFO, is_master) << "[bench] running conf: " << conf;
            benchmark_client<4, 16, 16>(p, bar, is_master, conf, key);
        }
    }
    else
    {
        benchmark_server<4, 16, 16>(p, bar, is_master, basic_conf, key);
    }

    LOG_IF(INFO, is_master) << "[bench] benching multiple threads";
    key++;
    auto multithread_conf = BenchConfigFactory::get_multi_round_config(
        "multithread_basic", 3_K, 5_M, kThreadNr, kMaxCoroNr);
    if (is_client)
    {
        for (const auto &conf : multithread_conf)
        {
            LOG_IF(INFO, is_master) << "[bench] running conf: " << conf;
            benchmark_client<4, 16, 16>(p, bar, is_master, conf, key);
        }
    }
    else
    {
        benchmark_server<4, 16, 16>(p, bar, is_master, multithread_conf, key);
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
    df.load_column<size_t>("x_thread_nr", std::move(col_x_thread_nr));
    df.load_column<size_t>("x_coro_nr", std::move(col_x_coro_nr));
    df.load_column<double>("x_put_rate", std::move(col_x_put_rate));
    df.load_column<double>("x_del_rate", std::move(col_x_del_rate));
    df.load_column<double>("x_get_rate", std::move(col_x_get_rate));
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
    df.consolidate<double, size_t, double>(
        "ops(total)", "x_thread_nr", "ops(thread)", div_f2, false);
    df.consolidate<double, size_t, double>(
        "ops(thread)", "x_coro_nr", "ops(coro)", div_f2, false);

    auto filename = binary_to_csv_filename(argv[0], FLAGS_exec_meta);
    df.write<std::ostream, std::string, size_t, double>(std::cout,
                                                        io_format::csv2);
    df.write<std::string, size_t, double>(filename.c_str(), io_format::csv2);

    patronus->keeper_barrier("finished", 100ms);

    LOG(INFO) << "finished. ctrl+C to quit.";
}