#include <algorithm>
#include <random>
#include <set>

#include "Common.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "patronus/memory/direct_allocator.h"
#include "thirdparty/racehashing/hashtable.h"
#include "thirdparty/racehashing/hashtable_handle.h"
#include "thirdparty/racehashing/utils.h"
#include "util/Rand.h"

using namespace patronus::hash;
using namespace util::literals;

// constexpr static size_t kTestTime = 1_M;
// constexpr static size_t kBucketGroupNr = 128;
// constexpr static size_t kSlotNr = 128;
// constexpr static size_t kMemoryLimit = 1_G;

constexpr static size_t kKVBlockPoolReserveSize = 512_MB;

DEFINE_string(exec_meta, "", "The meta data of this execution");

template <size_t kA, size_t kB, size_t kC>
using TablePair =
    std::pair<typename RaceHashing<kA, kB, kC>::pointer,
              std::vector<typename RaceHashing<kA, kB, kC>::Handle::pointer>>;

template <size_t kDEntry, size_t kBucketNr, size_t kSlotNr>
TablePair<kDEntry, kBucketNr, kSlotNr> gen_mock_rdma_rh(size_t initial_subtable,
                                                        size_t thread_nr,
                                                        bool auto_expand)
{
    using RaceHashingT = RaceHashing<kDEntry, kBucketNr, kSlotNr>;
    using RaceHashingHandleT = typename RaceHashingT::Handle;

    size_t kvblock_size = 64;
    // generate rh here
    auto allocator = std::make_shared<patronus::mem::RawAllocator>();
    RaceHashingConfig conf;
    conf.initial_subtable = initial_subtable;
    conf.g_kvblock_pool_size = kKVBlockPoolReserveSize;
    // let the memory leak, I don't care
    conf.g_kvblock_pool_addr = malloc(conf.g_kvblock_pool_size);

    auto server_rdma_ctx = MockRdmaAdaptor::new_instance({});
    server_rdma_ctx->reg_default_allocator(
        patronus::mem::MallocAllocator::new_instance());
    patronus::mem::SlabAllocatorConfig slab_conf;
    // slab_conf.block_class = {patronus::hash::config::kKVBlockAllocBatchSize};
    slab_conf.block_class = {kvblock_size};
    slab_conf.block_ratio = {1.0};
    auto slab_allocator = std::make_shared<patronus::mem::SlabAllocator>(
        conf.g_kvblock_pool_addr, conf.g_kvblock_pool_size, slab_conf);
    server_rdma_ctx->reg_allocator(patronus::hash::config::kAllocHintKVBlock,
                                   slab_allocator);

    auto rh = std::make_shared<RaceHashingT>(server_rdma_ctx, allocator, conf);

    // generate rhh here
    std::vector<std::shared_ptr<RaceHashingHandleT>> rhhs;
    for (size_t i = 0; i < thread_nr; ++i)
    {
        // RaceHashingHandleConfig handle_conf;
        auto handle_conf = RaceHashingConfigFactory::get_unprotected(
            "mock", kvblock_size, 1, false /* mock kvblock match */);
        auto handle_rdma_ctx = MockRdmaAdaptor::new_instance(server_rdma_ctx);
        auto rhh = std::make_shared<RaceHashingHandleT>(0 /* node_id */,
                                                        rh->meta_gaddr(),
                                                        handle_conf,
                                                        auto_expand,
                                                        handle_rdma_ctx);
        rhh->init();
        rhhs.push_back(rhh);
    }
    return {rh, rhhs};
}

template <size_t kE, size_t kB, size_t kS>
void tear_down_mock_rdma_rh(
    typename RaceHashing<kE, kB, kS>::pointer rh,
    std::vector<typename RaceHashing<kE, kB, kS>::Handle::pointer> rhhs)
{
    const auto &rh_conf = rh->config();
    free(rh_conf.g_kvblock_pool_addr);
    std::ignore = rhhs;
}

void test_basic(size_t initial_subtable)
{
    auto [rh_ptr, rhh_ptrs] = gen_mock_rdma_rh<4, 64, 64>(
        initial_subtable, 1 /* thread_nr */, false /* auto-expand */);
    auto &rh = *rh_ptr;
    auto &rhh = *rhh_ptrs[0];

    CHECK_EQ(rhh.put("abc", "def"), kOk);
    std::string get;
    CHECK_EQ(rhh.get("abc", get), kOk);
    CHECK_EQ(get, "def");
    CHECK_EQ(rhh.put("abc", "!!!"), kOk);
    CHECK_EQ(rhh.get("abc", get), kOk);
    CHECK_EQ(get, "!!!");
    CHECK_GT(rh.utilization(), 0);
    CHECK_EQ(rhh.del("abc"), kOk);
    CHECK_EQ(rhh.get("abc", get), kNotFound);
    CHECK_EQ(rhh.del("abs"), kNotFound);
    CHECK_EQ(rh.utilization(), 0);
    LOG(INFO) << "max capacity: " << rh.max_capacity();

    tear_down_mock_rdma_rh<4, 64, 64>(rh_ptr, rhh_ptrs);
}

void test_capacity(size_t initial_subtable)
{
    auto [rh_ptr, rhh_ptrs] = gen_mock_rdma_rh<4, 16, 16>(
        initial_subtable, 1 /* thread_nr */, false /* auto-expand */);
    auto &rh = *rh_ptr;
    auto &rhh = *rhh_ptrs[0];

    std::string key;
    std::string value;
    key.resize(8);
    value.resize(8);
    std::map<std::string, std::string> inserted;
    size_t succ_nr = 0;
    size_t fail_nr = 0;
    bool first_fail = true;

    util::TraceManager tm(0);

    for (size_t i = 0; i < rh.max_capacity(); ++i)
    {
        fast_pseudo_fill_buf(key.data(), key.size());
        fast_pseudo_fill_buf(value.data(), value.size());
        auto trace = tm.trace("mock");
        trace.set("k", key);
        trace.set("v", value);
        trace.set("op", "put");

        auto rc = rhh.put(key, value, trace);
        if (rc == kOk)
        {
            inserted.emplace(key, value);
            succ_nr++;
        }
        else if (rc == kNoMem)
        {
            if (first_fail)
            {
                LOG(INFO) << "First insert fail. " << rh;
                CHECK_GE(rh.utilization(), 0.5)
                    << "Expect to at least utilize 50%";
                first_fail = false;
            }
            fail_nr++;
        }
        else
        {
            CHECK(false) << "Unknow return code: " << rc;
        }
        if (i == rh.max_capacity() / 2)
        {
            LOG(INFO) << "Inserted a half: " << rh;
        }
    }
    CHECK_GE(rh.utilization(), 0.9)
        << "Expect to have at least 90%% utilization";

    LOG(WARNING) << "Inserted " << succ_nr << ", failed: " << fail_nr
                 << ", ultilization: " << 1.0 * succ_nr / rh.max_capacity()
                 << ", success rate: " << 1.0 * succ_nr / (succ_nr + fail_nr);

    LOG(INFO) << "Checking integrity";

    LOG(INFO) << rh;

    util::TraceManager validate_tm(0);
    for (const auto &[key, expect_value] : inserted)
    {
        std::string get_val;
        auto trace = validate_tm.trace("get");
        trace.set("key", key);
        trace.set("value", expect_value);
        CHECK_EQ(rhh.get(key, get_val, trace), kOk)
            << "** Failed to get back key `" << key << "`";
        CHECK_EQ(get_val, expect_value)
            << "** getting key `" << key << "` expect value `" << expect_value
            << "`";
        trace = validate_tm.trace("del");
        trace.set("key", key);
        CHECK_EQ(rhh.del(key, trace), kOk)
            << "** Failed to delete existing key `" << key << "`";
        trace = validate_tm.trace("del again");
        trace.set("key", key);
        CHECK_EQ(rhh.del(key, trace), kNotFound)
            << "** delete an already-deleted-key `" << key
            << "` expects failure";
        trace = validate_tm.trace("get after del");
        trace.set("key", key);
        CHECK_EQ(rhh.get(key, get_val, trace), kNotFound)
            << "** Getting an already-deleted-key `" << key
            << "` expects failure";
    }
    CHECK_EQ(rh.utilization(), 0)
        << "Removed all the items should result in 0 utilization";
    LOG(INFO) << rh;

    tear_down_mock_rdma_rh<4, 16, 16>(rh_ptr, rhh_ptrs);
}

template <size_t kA, size_t kB, size_t kC>
void test_thread(typename RaceHashing<kA, kB, kC>::pointer rh,
                 typename RaceHashing<kA, kB, kC>::Handle::pointer rhh,
                 size_t tid,
                 size_t test_nr)
{
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

    std::map<std::string, std::string> inserted;
    std::set<std::string> keys;

    // HashContext dctx(tid);
    util::TraceManager tm(0);

    for (size_t i = 0; i < test_nr; ++i)
    {
        if (i % (test_nr / 10) == 0)
        {
            LOG(INFO) << "Finished " << 1.0 * i / test_nr * 100 << "%. tid "
                      << tid;
        }
        fast_pseudo_fill_buf(key_buf, kKeySize);
        fast_pseudo_fill_buf(val_buf, kValueSize);
        std::string key(key_buf, kKeySize);
        std::string value(val_buf, kValueSize);

        auto trace = tm.trace("op");
        trace.set("tid", std::to_string(tid));

        // so each thread will not modify others value
        char mark = 'a' + tid;
        key[0] = mark;

        if (true_with_prob(0.5))
        {
            // insert
            trace.set("k", key);
            trace.set("v", value);
            trace.set("op", "put");
            auto rc = rhh->put(key, value, trace);
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
        else if (true_with_prob(0.125))
        {
            // delete exist
            if (keys.empty())
            {
                continue;
            }
            key = random_choose(keys);
            trace.set("k", key);
            trace.set("v", value);
            trace.set("op", "del");
            CHECK_EQ(rhh->del(key, trace), kOk)
                << "tid " << tid << " deleting key `" << key
                << "` expect to succeed. inserted.count(key): "
                << inserted.count(key);
            inserted.erase(key);
            keys.erase(key);
            del_succ_nr++;
        }
        else if (true_with_prob(0.125))
        {
            // delete unexist
            bool exist = keys.count(key) == 1;
            if (exist)
            {
                trace.set("k", key);
                trace.set("v", value);
                trace.set("op", "del");
                CHECK_EQ(rhh->del(key, trace), kOk)
                    << util::pre_map(trace.kv());
                inserted.erase(key);
                keys.erase(key);
                del_succ_nr++;
            }
            else
            {
                trace.set("k", key);
                trace.set("v", value);
                trace.set("op", "del");
                CHECK_EQ(rhh->del(key, trace), kNotFound)
                    << util::pre_map(trace.kv());
                del_fail_nr++;
            }
        }
        else if (true_with_prob(0.125))
        {
            // get exist
            if (keys.empty())
            {
                continue;
            }
            std::string got_value;
            key = random_choose(keys);
            CHECK_EQ(key[0], mark) << util::pre_map(trace.kv());
            trace.set("k", key);
            trace.set("v", value);
            trace.set("op", "get");
            CHECK_EQ(rhh->get(key, got_value, trace), kOk)
                << "Tid: " << tid << " getting key `" << key
                << "` expect to succeed";
            CHECK_EQ(got_value, inserted[key]) << util::pre_map(trace.kv());
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
                trace.set("k", key);
                trace.set("v", value);
                trace.set("op", "get");
                CHECK_EQ(rhh->get(key, got_value, trace), kOk)
                    << util::pre_map(trace.kv());
                CHECK_EQ(got_value, inserted[key]) << util::pre_map(trace.kv());
                get_succ_nr++;
            }
            else
            {
                std::string got_value;
                trace.set("k", key);
                trace.set("v", value);
                trace.set("op", "get");
                CHECK_EQ(rhh->get(key, got_value, trace), kNotFound)
                    << util::pre_map(trace.kv());
                get_fail_nr++;
            }
        }
    }

    LOG(INFO) << "Finished test. tid: " << tid << ". Table: " << *rh;

    for (const auto &[k, v] : inserted)
    {
        auto trace = tm.trace("validate");
        trace.set("k", k);
        trace.set("v", v);
        trace.set("op", "get");
        std::string get_v;
        CHECK_EQ(rhh->get(k, get_v, trace), kOk) << util::pre_map(trace.kv());
        CHECK_EQ(get_v, v) << util::pre_map(trace.kv());
        auto trace2 = tm.trace("validate");
        trace2.set("k", k);
        trace2.set("v", v);
        trace2.set("op", "get");
        CHECK_EQ(rhh->del(k, trace2), kOk) << util::pre_map(trace.kv());
    }

    LOG(INFO) << "Tear down. tid: " << tid << ". Table: " << *rh;
}

template <size_t kDEntryNr, size_t kBucketGroupNr, size_t kSlotNr>
void test_multithreads(size_t thread_nr, size_t test_nr, bool expand)
{
    auto [rh_ptr, rhh_ptrs] =
        gen_mock_rdma_rh<kDEntryNr, kBucketGroupNr, kSlotNr>(
            1, thread_nr, expand);

    std::vector<std::thread> threads;
    for (size_t i = 0; i < thread_nr; ++i)
    {
        threads.emplace_back(
            [tid = i, test_nr, rh = rh_ptr, rhh = rhh_ptrs[i]]() {
                test_thread<kDEntryNr, kBucketGroupNr, kSlotNr>(
                    rh, rhh, tid, test_nr);
            });
    }
    for (auto &t : threads)
    {
        t.join();
    }

    tear_down_mock_rdma_rh<kDEntryNr, kBucketGroupNr, kSlotNr>(rh_ptr,
                                                               rhh_ptrs);
}

void test_expand_once_single_thread()
{
    auto [rh_ptr, rhhs_ptr] = gen_mock_rdma_rh<2, 4, 4>(1, 1, false);
    auto &rh = *rh_ptr;
    auto &rhh = *rhhs_ptr[0];

    std::string key;
    std::string value;
    constexpr static size_t kKeySize = 8;
    constexpr static size_t kValueSize = 8;
    char key_buf[kKeySize + 5];
    char val_buf[kValueSize + 5];
    key.resize(kKeySize);
    value.resize(kValueSize);
    size_t insert_nr = 0;
    std::map<std::string, std::string> inserted;

    util::TraceManager tm(0);
    while (true)
    {
        fast_pseudo_fill_buf(key_buf, kKeySize);
        fast_pseudo_fill_buf(val_buf, kValueSize);
        std::string key(key_buf, kKeySize);
        std::string value(val_buf, kValueSize);

        auto trace = tm.trace("expand_once");
        trace.set("k", key);
        trace.set("v", value);
        trace.set("op", "put");
        trace.set("tid", std::to_string(0));

        auto rc = rhh.put(key, value, trace);
        if (rc == kNoMem)
        {
            LOG(INFO) << "[bench] inserted: " << insert_nr << ". Table: " << rh;
            break;
        }
        CHECK_EQ(rc, kOk);
        inserted.emplace(key, value);
        insert_nr++;
    }
    for (const auto &[k, v] : inserted)
    {
        std::string got_v;
        CHECK_EQ(rhh.get(k, got_v), kOk);
        CHECK_EQ(got_v, v);
    }
    LOG(INFO) << "[bench] begin to expand";
    auto trace2 = tm.trace("to expand");
    trace2.set("op", "expand");
    rhh.expand(0, trace2);
    for (const auto &[k, v] : inserted)
    {
        auto trace3 = tm.trace("to expand validate");
        trace3.set("k", k);
        trace3.set("v", v);
        trace3.set("op", "get");
        std::string got_v;
        CHECK_EQ(rhh.get(k, got_v, trace3), kOk)
            << "failed to get back key " << k;
        CHECK_EQ(got_v, v);
    }
    // what you inserted will not be lost
    size_t another_insert = 0;
    while (true)
    {
        fast_pseudo_fill_buf(key_buf, kKeySize);
        fast_pseudo_fill_buf(val_buf, kValueSize);
        std::string key(key_buf, kKeySize);
        std::string value(val_buf, kValueSize);
        auto rc = rhh.put(key, value);
        if (rc == kNoMem)
        {
            LOG(WARNING) << "[bench] inserted another: " << another_insert
                         << ". Table: " << rh;
            break;
        }
        else
        {
            CHECK_EQ(rc, kOk);
            inserted.emplace(key, value);
        }
        DCHECK_EQ(rc, kOk);
        another_insert++;
    }

    for (const auto &[k, v] : inserted)
    {
        std::string get_v;
        CHECK_EQ(rhh.get(k, get_v), kOk);
        CHECK_EQ(get_v, v);
    }

    // insert another to fill the hashtable
    for (size_t i = 0; i < rh.max_capacity(); ++i)
    {
        fast_pseudo_fill_buf(key_buf, kKeySize);
        fast_pseudo_fill_buf(val_buf, kValueSize);
        std::string key(key_buf, kKeySize);
        std::string value(val_buf, kValueSize);
        auto rc = rhh.put(key, value);
        CHECK(rc == kOk || rc == kNoMem);
        if (rc == kOk)
        {
            inserted.emplace(key, value);
        }
        another_insert++;
    }
    LOG(WARNING) << "[bench] fill the hashtable: " << rh;

    for (const auto &[k, v] : inserted)
    {
        std::string get_v;
        CHECK_EQ(rhh.get(k, get_v), kOk);
        CHECK_EQ(get_v, v);
        CHECK_EQ(rhh.del(k), kOk);
    }
    LOG(WARNING) << "[bench] after deleted. " << rh;

    tear_down_mock_rdma_rh<2, 4, 4>(rh_ptr, rhhs_ptr);
}

void test_expand_multiple_single_thread()
{
    auto [rh_ptr, rhh_ptrs] = gen_mock_rdma_rh<16, 4, 4>(1, 1, true);
    auto &rh = *rh_ptr;
    auto &rhh = *rhh_ptrs[0];

    std::string key;
    std::string value;
    constexpr static size_t kKeySize = 8;
    constexpr static size_t kValueSize = 8;
    char key_buf[kKeySize + 5];
    char val_buf[kValueSize + 5];
    key.resize(kKeySize);
    value.resize(kValueSize);
    std::map<std::string, std::string> inserted;

    size_t inserted_nr = 0;
    util::TraceManager tm(0);
    while (true)
    {
        auto trace = tm.trace("expand multiple times");
        trace.set("k", key);
        trace.set("v", value);
        trace.set("op", "put");

        fast_pseudo_fill_buf(key_buf, kKeySize);
        fast_pseudo_fill_buf(val_buf, kValueSize);
        std::string key(key_buf, kKeySize);
        std::string value(val_buf, kValueSize);

        auto rc = rhh.put(key, value, trace);
        if (rc == kNoMem)
        {
            LOG(INFO) << "[bench] nomem. out of directory entries. table: "
                      << rh;

            break;
        }
        CHECK_EQ(rc, kOk);
        inserted.emplace(key, value);
        inserted_nr++;
    }
    for (const auto &[k, v] : inserted)
    {
        auto trace = tm.trace("expand multiple times: validate");
        trace.set("k", k);
        trace.set("v", v);
        trace.set("op", "get");

        std::string got_v;
        auto rc = rhh.get(k, got_v);
        if (rc != kOk)
        {
            LOG(WARNING) << "Failed to find key `" << k
                         << "`. Start to debug.... Got rc: " << rc;
            // do it again with debug
            rc = rhh.get(k, got_v, trace);
            CHECK(false) << "Failed to get `" << k << "`. Expect value `" << v
                         << "`. Got: " << rc;
        }
        CHECK_EQ(got_v, v);
    }

    // check integrity and tear down

    for (const auto &[k, v] : inserted)
    {
        auto trace = tm.trace("expand multiple times: validate2");
        trace.set("k", k);
        trace.set("v", v);
        trace.set("op", "get");

        std::string got_v;
        CHECK_EQ(rhh.get(k, got_v), kOk) << "failed to get back key " << k;
        CHECK_EQ(got_v, v);
        CHECK_EQ(rhh.del(k, trace), kOk) << "failed to delete key " << k;
    }
    LOG(INFO) << "[bench] tear downed. table: " << rh;

    tear_down_mock_rdma_rh<16, 4, 4>(rh_ptr, rhh_ptrs);
}
void test_burn_expand_single_thread()
{
    auto [rh_ptr, rhh_ptrs] = gen_mock_rdma_rh<128, 2, 4>(1, 1, true);
    auto &rh = *rh_ptr;
    auto &rhh = *rhh_ptrs[0];

    std::string key;
    std::string value;
    constexpr static size_t kKeySize = 8;
    constexpr static size_t kValueSize = 8;
    char key_buf[kKeySize + 5];
    char val_buf[kValueSize + 5];
    key.resize(kKeySize);
    value.resize(kValueSize);
    std::map<std::string, std::string> inserted;
    size_t inserted_nr = 0;

    util::TraceManager tm(0);
    while (true)
    {
        auto trace = tm.trace("burn expand");

        fast_pseudo_fill_buf(key_buf, kKeySize);
        fast_pseudo_fill_buf(val_buf, kValueSize);
        std::string key(key_buf, kKeySize);
        std::string value(val_buf, kValueSize);

        trace.set("k", key);
        trace.set("v", value);
        trace.set("op", "put");

        auto rc = rhh.put(key, value, trace);
        if (rc == kNoMem)
        {
            LOG(INFO) << "[bench] nomem. out of directory entries. table: "
                      << rhh;

            break;
        }
        CHECK_EQ(rc, kOk);
        inserted.emplace(key, value);
        inserted_nr++;
    }
    LOG(INFO) << rh;
    for (const auto &[k, v] : inserted)
    {
        std::string got_v;
        auto rc = rhh.get(k, got_v);
        if (rc != kOk)
        {
            LOG(WARNING) << "Failed to find key `" << k
                         << "`. Start to debug.... Got rc: " << rc;

            auto trace = tm.trace("burn expand: validate");
            trace.set("k", key);
            trace.set("v", value);
            trace.set("op", "get");
            rc = rhh.get(k, got_v, trace);
            CHECK(false) << "Failed to get `" << k << "`. Expect value `" << v
                         << "`. Got: " << rc;
        }
        CHECK_EQ(got_v, v);
    }

    // check integrity
    for (const auto &[k, v] : inserted)
    {
        auto trace = tm.trace("burn expand: validate2");
        trace.set("k", k);
        trace.set("v", v);
        trace.set("op", "get");

        std::string got_v;
        CHECK_EQ(rhh.get(k, got_v, trace), kOk)
            << "failed to get back key " << k;
        CHECK_EQ(got_v, v);
    }
    LOG(INFO) << "[bench] pass integrity check. See whether we can fill the "
                 "hashtable to full";
    size_t another_inserted_nr = 0;
    for (size_t i = 0; i < rh.max_capacity(); ++i)
    {
        fast_pseudo_fill_buf(key_buf, kKeySize);
        fast_pseudo_fill_buf(val_buf, kValueSize);
        std::string key(key_buf, kKeySize);
        std::string value(val_buf, kValueSize);

        auto trace = tm.trace("burn expand: fill");
        trace.set("k", key);
        trace.set("v", value);
        trace.set("op", "put");

        auto rc = rhh.put(key, value, trace);
        CHECK(rc == kNoMem || rc == kOk);
        if (rc == kOk)
        {
            inserted.emplace(key, value);
            another_inserted_nr++;
        }
    }
    LOG(INFO) << "[bench] after filling the hashtable: " << rh
              << ", with another inserted: " << another_inserted_nr
              << ", actual capacity: "
              << 1.0 * (inserted_nr + another_inserted_nr) / rh.max_capacity();

    // tear down
    for (const auto &[k, v] : inserted)
    {
        auto trace = tm.trace("burn expand: tear down");
        trace.set("k", k);
        trace.set("v", v);
        trace.set("op", "del");

        CHECK_EQ(rhh.del(k, trace), kOk) << "failed to delete key " << k;
    }
    LOG(INFO) << "[bench] tear downed. table: " << rh;
    LOG(INFO) << "[bench] inserted_nr: " << inserted_nr
              << ", another_inserted: " << another_inserted_nr
              << ", table_size: " << rh.max_capacity()
              << " actual utilization: "
              << 1.0 * (inserted_nr + another_inserted_nr) / rh.max_capacity();

    tear_down_mock_rdma_rh<128, 2, 4>(rh_ptr, rhh_ptrs);
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    test_basic(1);
    test_capacity(1);
    test_capacity(4);

    test_multithreads<4, 8, 8>(8, 100_K, false);

    test_expand_once_single_thread();

    test_expand_multiple_single_thread();
    test_burn_expand_single_thread();

    // NOTE: not runnable, not correct
    // There are so many corner cases under expansion
    // so not going to check the correctness
    test_multithreads<16, 4, 4>(8, 10, true);

    LOG(INFO) << "PASS ALL TESTS";
    LOG(INFO) << "finished. ctrl+C to quit.";
}