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
#include "thirdparty/racehashing/hashtable.h"
#include "thirdparty/racehashing/hashtable_handle.h"
#include "thirdparty/racehashing/utils.h"
#include "util/Rand.h"

using namespace patronus::hash;
using namespace define::literals;
using namespace patronus;

[[maybe_unused]] constexpr uint16_t kClientNodeId = 0;
[[maybe_unused]] constexpr uint16_t kServerNodeId = 1;
constexpr uint32_t kMachineNr = 2;
constexpr static size_t kThreadNr = 8;
constexpr static size_t kMaxCoroNr = 16;
constexpr static size_t kKVBlockExpectSize = 64;

DEFINE_string(exec_meta, "", "The meta data of this execution");

struct BenchConfig
{
    double insert_prob{0};
    double delete_exist_prob{0};
    double delete_unexist_prob{0};
    double get_exist_prob{0};
    double get_unexist_prob{0};
    bool auto_extend{false};
    bool first_enter{true};
    bool server_should_leave{true};
    size_t thread_nr{1};
    size_t coro_nr{1};
    size_t test_nr{0};
    void validate()
    {
        double sum = insert_prob + delete_exist_prob + delete_unexist_prob +
                     get_exist_prob + get_unexist_prob;
        CHECK_DOUBLE_EQ(sum, 1);
    }
    static BenchConfig get_default_conf(size_t test_nr,
                                        size_t thread_nr,
                                        size_t coro_nr,
                                        bool auto_extend,
                                        bool first_enter,
                                        bool server_should_leave)
    {
        BenchConfig conf;
        conf.insert_prob = 0.5;
        conf.delete_exist_prob = 0.125;
        conf.delete_unexist_prob = 0.125;
        conf.get_exist_prob = 0.125;
        conf.get_unexist_prob = 0.125;
        conf.auto_extend = auto_extend;
        conf.first_enter = first_enter;
        conf.server_should_leave = server_should_leave;
        conf.thread_nr = thread_nr;
        conf.coro_nr = coro_nr;
        conf.test_nr = test_nr;

        conf.validate();
        return conf;
    }
};

class BenchConfigFactory
{
public:
    static std::vector<BenchConfig> get_single_round_config(size_t test_nr,
                                                            size_t thread_nr,
                                                            size_t coro_nr,
                                                            bool expand)
    {
        if (expand)
        {
            CHECK_EQ(thread_nr, 1);
            CHECK_EQ(coro_nr, 1);
        }
        return {BenchConfig::get_default_conf(test_nr,
                                              thread_nr,
                                              coro_nr,
                                              expand /* extend */,
                                              true /* enter */,
                                              true /* leave */)};
    }
    static std::vector<BenchConfig> get_multi_round_config(size_t fill_nr,
                                                           size_t test_nr,
                                                           size_t thread_nr,
                                                           size_t coro_nr)
    {
        BenchConfig insert_conf;
        insert_conf.thread_nr = 1;
        insert_conf.coro_nr = 1;
        insert_conf.insert_prob = 1;
        insert_conf.auto_extend = true;
        insert_conf.first_enter = true;
        insert_conf.server_should_leave = false;
        insert_conf.test_nr = fill_nr;

        BenchConfig query_conf;
        query_conf.thread_nr = thread_nr;
        query_conf.coro_nr = coro_nr;
        query_conf.get_exist_prob = 0.5;
        query_conf.get_unexist_prob = 0.5;
        query_conf.auto_extend = false;
        query_conf.first_enter = false;
        query_conf.server_should_leave = true;
        query_conf.test_nr = test_nr;
        return {insert_conf, query_conf};
    }

private:
};

template <size_t kE, size_t kB, size_t kS>
typename RaceHashing<kE, kB, kS>::Handle::pointer gen_rdma_rhh(
    Patronus::pointer p, bool auto_expand, CoroContext *ctx)
{
    using HandleT = typename RaceHashing<kE, kB, kS>::Handle;

    auto tid = p->get_thread_id();
    auto dir_id = tid;

    auto meta_gaddr = p->get_object<GlobalAddress>("race:meta_gaddr", 1ms);
    LOG(INFO) << "Getting from race:meta_gaddr got " << meta_gaddr;

    RaceHashingHandleConfig handle_conf;
    handle_conf.kvblock_expect_size = kKVBlockExpectSize;
    auto handle_rdma_ctx =
        patronus::RdmaAdaptor::new_instance(kServerNodeId, dir_id, p, ctx);

    auto prhh = HandleT::new_instance(
        kServerNodeId, meta_gaddr, handle_conf, auto_expand, handle_rdma_ctx);
    prhh->init();
    return prhh;
}

void init_allocator(Patronus::pointer p, size_t thread_nr, size_t kvblock_size)
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
    kvblock_slab_config.block_class = {kvblock_size};
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

template <size_t kE, size_t kB, size_t kS>
void test_basic_client_worker(Patronus::pointer p,
                              size_t coro_id,
                              CoroYield &yield,
                              const BenchConfig &conf,
                              CoroExecutionContext<kMaxCoroNr> &ex)
{
    auto test_nr = conf.test_nr;
    auto auto_expand = conf.auto_extend;

    auto tid = p->get_thread_id();
    CoroContext ctx(0, &yield, &ex.master(), coro_id);
    auto worker_id = tid * kMaxCoroNr + coro_id;

    auto rhh = gen_rdma_rhh<kE, kB, kS>(p, auto_expand, &ctx);
    // auto &rhh = *prhh;

    constexpr static size_t kKeySize = 3;
    constexpr static size_t kValueSize = 3;
    char key_buf[kKeySize + 5];
    char val_buf[kValueSize + 5];

    size_t ins_succ_nr = 0;
    size_t ins_fail_nr = 0;
    size_t del_succ_nr = 0;
    size_t del_fail_nr = 0;
    size_t get_succ_nr = 0;
    size_t get_fail_nr = 0;

    size_t executed_nr = 0;

    std::map<std::string, std::string> inserted;
    std::set<std::string> keys;

    HashContext dctx(tid);

    double insert_prob = conf.insert_prob;
    double delete_exist_prob = conf.delete_exist_prob;
    double delete_unexist_prob = conf.delete_unexist_prob;
    double get_exist_prob = conf.get_exist_prob;

    for (size_t i = 0; i < test_nr; ++i)
    {
        if (i % (test_nr / 10) == 0)
        {
            LOG(INFO) << "Finished " << 1.0 * i / test_nr * 100 << "%. "
                      << dctx;
        }
        fast_pseudo_fill_buf(key_buf, kKeySize);
        fast_pseudo_fill_buf(val_buf, kValueSize);
        std::string key(key_buf, kKeySize);
        std::string value(val_buf, kValueSize);

        // so each thread will not modify others value
        char mark = 'a' + worker_id;
        key[0] = mark;

        if (true_with_prob(insert_prob))
        {
            // insert
            dctx.key = key;
            dctx.value = value;
            dctx.op = "p";
            auto rc = rhh->put(key, value, &dctx);
            if (rc == kOk)
            {
                keys.insert(key);
                inserted[key] = value;
                ins_succ_nr++;
            }
            else
            {
                ins_fail_nr++;
            }
        }
        else if (true_with_prob(delete_exist_prob))
        {
            // delete exist
            if (keys.empty())
            {
                continue;
            }
            key = random_choose(keys);
            dctx.key = key;
            dctx.value = value;
            dctx.op = "d";
            auto rc = rhh->del(key, &dctx);
            CHECK_EQ(rc, kOk) << "tid " << tid << " deleting key `" << key
                              << "` expect to succeed.";
            inserted.erase(key);
            keys.erase(key);
            del_succ_nr++;
        }
        else if (true_with_prob(delete_unexist_prob))
        {
            // delete unexist
            bool exist = keys.count(key) == 1;
            if (exist)
            {
                dctx.key = key;
                dctx.value = value;
                dctx.op = "d";
                auto rc = rhh->del(key, &dctx);
                CHECK_EQ(rc, kOk) << dctx;
                inserted.erase(key);
                keys.erase(key);
                del_succ_nr++;
            }
            else
            {
                dctx.key = key;
                dctx.value = value;
                dctx.op = "d";
                auto rc = rhh->del(key, &dctx);
                CHECK_EQ(rc, kNotFound) << dctx;
                del_fail_nr++;
            }
        }
        else if (true_with_prob(get_exist_prob))
        {
            // get exist
            if (keys.empty())
            {
                continue;
            }
            std::string got_value;
            key = random_choose(keys);
            CHECK_EQ(key[0], mark) << dctx;
            dctx.key = key;
            dctx.value = value;
            dctx.op = "g";
            auto rc = rhh->get(key, got_value, &dctx);
            CHECK_EQ(rc, kOk) << "Tid: " << tid << " getting key `" << key
                              << "` expect to succeed";
            CHECK_EQ(got_value, inserted[key]) << dctx;
            get_succ_nr++;
        }
        else
        {
            // get unexist
            bool exist = keys.count(key) == 1;
            if (exist)
            {
                if (keys.empty())
                {
                    continue;
                }
                std::string got_value;
                key = random_choose(keys);
                dctx.key = key;
                dctx.value = value;
                dctx.op = "g";
                auto rc = rhh->get(key, got_value, &dctx);
                CHECK_EQ(rc, kOk) << dctx;
                CHECK_EQ(got_value, inserted[key]) << dctx;
                get_succ_nr++;
            }
            else
            {
                std::string got_value;
                dctx.key = key;
                dctx.value = value;
                dctx.op = "g";
                auto rc = rhh->get(key, got_value, &dctx);
                CHECK_EQ(rc, kNotFound) << dctx;
                get_fail_nr++;
            }
        }
        executed_nr++;
    }

    for (const auto &[k, v] : inserted)
    {
        std::string get_v;
        dctx.key = k;
        dctx.value = v;
        CHECK_EQ(rhh->get(k, get_v, &dctx), kOk) << dctx;
        CHECK_EQ(get_v, v) << dctx;
        dctx.key = k;
        dctx.value = v;
        CHECK_EQ(rhh->del(k, &dctx), kOk) << dctx;
    }

    // coro finished
    LOG(INFO) << "tid " << tid << " coro: " << ctx << " finished. ";
    ex.worker_finished(coro_id);
    ctx.yield_to_master();
}

void test_basic_client_master(Patronus::pointer p,
                              CoroYield &yield,
                              size_t coro_nr,
                              CoroExecutionContext<kMaxCoroNr> &ex)
{
    auto tid = p->get_thread_id();
    auto mid = tid;

    CoroContext mctx(tid, &yield, ex.workers());
    CHECK(mctx.is_master());

    for (size_t i = 0; i < coro_nr; ++i)
    {
        mctx.yield_to_worker(i);
    }

    LOG(INFO) << "Return back to master. start to recv messages";
    coro_t coro_buf[2 * kMaxCoroNr];
    while (!ex.is_finished_all())
    {
        auto nr =
            p->try_get_client_continue_coros(mid, coro_buf, 2 * kMaxCoroNr);
        for (size_t i = 0; i < nr; ++i)
        {
            auto coro_id = coro_buf[i];
            DVLOG(1) << "[bench] yielding due to CQE: " << (int) coro_id;
            mctx.yield_to_worker(coro_id);
        }
    }
}

template <size_t kE, size_t kB, size_t kS>
void test_basic_client(Patronus::pointer p,
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
    if (is_master && first_enter)
    {
        p->keeper_barrier("server_ready-" + std::to_string(key), 100ms);
    }
    bar.wait();

    LOG(INFO) << "[bench] tid " << tid << " Start to bench...";
    if (tid < thread_nr)
    {
        CoroExecutionContext<kMaxCoroNr> ex;
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
        master = CoroCall([p, &ex, coro_nr](CoroYield &yield) {
            test_basic_client_master(p, yield, coro_nr, ex);
        });

        master();
    }

    bar.wait();
    if (is_master && server_should_leave)
    {
        LOG(INFO) << "p->finished(" << key << ")";
        p->finished(key);
    }
}

template <size_t kE, size_t kB, size_t kS>
void test_basic_server(Patronus::pointer p,
                       boost::barrier &bar,
                       size_t thread_nr,
                       bool is_master,
                       size_t initial_subtable,
                       uint64_t key)
{
    using RaceHashingT = RaceHashing<kE, kB, kS>;
    auto tid = p->get_thread_id();
    auto mid = tid;

    typename RaceHashingT::pointer rh;
    if (tid < thread_nr)
    {
        init_allocator(p, thread_nr, kKVBlockExpectSize);
    }
    else
    {
        CHECK(!is_master) << "** master not initing allocator";
    }
    if (is_master)
    {
        p->finished(key);
        rh = gen_rdma_rh<kE, kB, kS>(p, initial_subtable);
        p->keeper_barrier("server_ready-" + std::to_string(key), 100ms);
    }
    bar.wait();

    p->server_serve(mid, key);
    bar.wait();
}

std::atomic<size_t> battle_master{0};
void client(Patronus::pointer p, boost::barrier &bar)
{
    bool master = false;
    if (battle_master.fetch_add(1) == 0)
    {
        master = true;
    }
    bar.wait();

    LOG_IF(INFO, master) << "[bench] Test basic single thread";
    // single thread small
    auto small_conf = BenchConfigFactory::get_single_round_config(
        100 /* test */, 1 /* thread */, 1 /* coro */, false /* expand */);
    for (const auto &conf : small_conf)
    {
        test_basic_client<4, 64, 64>(p, bar, master, conf, 0 /* key */);
    }

    LOG_IF(INFO, master) << "[bench] Burn basic single thread";
    auto burn_conf = BenchConfigFactory::get_single_round_config(
        10_K /* test */, 1 /* thread */, 1 /* coro */, false /* expand */);
    for (const auto &conf : burn_conf)
    {
        test_basic_client<4, 64, 64>(p, bar, master, conf, 1 /* key */);
    }

    LOG_IF(INFO, master) << "[bench] test expand single thread";
    auto expand_conf = BenchConfigFactory::get_single_round_config(
        100_K, 1 /* thread */, 1 /* coro */, true /* expand */);
    for (const auto &conf : expand_conf)
    {
        test_basic_client<128, 4, 4>(p, bar, master, conf, 2 /* key */);
    }

    LOG_IF(INFO, master) << "[bench] Test basic multiple thread";
    auto multithread_conf = BenchConfigFactory::get_single_round_config(
        1_K, kThreadNr, kMaxCoroNr, false);
    for (const auto &conf : multithread_conf)
    {
        test_basic_client<4, 64, 64>(p, bar, master, conf, 3 /* key */);
    }

    LOG_IF(INFO, master) << "[bench] Test multi-round complex workload";
    auto multi_round_conf = BenchConfigFactory::get_multi_round_config(
        100_K /* fill */, 1_K /* test */, kThreadNr, kMaxCoroNr);
    for (const auto &conf : multi_round_conf)
    {
        test_basic_client<32, 4, 4>(p, bar, master, conf, 5 /* key */);
    }
}

void server(Patronus::pointer p, boost::barrier &bar)
{
    bool is_master = false;
    is_master = battle_master.fetch_add(1) == 0;

    test_basic_server<4, 64, 64>(
        p, bar, 1, is_master, 1 /* subtable_nr */, 0 /* key */);
    test_basic_server<4, 64, 64>(
        p, bar, 1, is_master, 4 /* subtable_nr */, 1 /* key */);
    test_basic_server<128, 4, 4>(
        p, bar, 1, is_master, 1 /* subtable_nr */, 2 /* key */);
    test_basic_server<4, 64, 64>(
        p, bar, kThreadNr, is_master, 4 /* subtable_nr */, 3 /* key */);
    test_basic_server<32, 4, 4>(
        p, bar, kThreadNr, is_master, 1 /* subtable_nr */, 5 /* key */);
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

    if (nid == kClientNodeId)
    {
        patronus->registerClientThread();
        for (size_t i = 0; i < kThreadNr - 1; ++i)
        {
            threads.emplace_back([patronus, &bar]() {
                patronus->registerClientThread();
                client(patronus, bar);
            });
        }
        client(patronus, bar);
    }
    else
    {
        patronus->registerServerThread();
        for (size_t i = 0; i < kThreadNr - 1; ++i)
        {
            threads.emplace_back([patronus, &bar]() {
                patronus->registerServerThread();
                server(patronus, bar);
            });
        }
        server(patronus, bar);
    }

    for (auto &t : threads)
    {
        t.join();
    }

    patronus->keeper_barrier("finished", 100ms);

    LOG(INFO) << "finished. ctrl+C to quit.";
}