#include <algorithm>
#include <random>
#include <set>

#include "Common.h"
#include "glog/logging.h"
#include "patronus/memory/direct_allocator.h"
#include "thirdparty/racehashing/hashtable.h"
#include "thirdparty/racehashing/hashtable_handle.h"
#include "thirdparty/racehashing/utils.h"
#include "util/Rand.h"

using namespace patronus::hash;
using namespace define::literals;

// constexpr static size_t kTestTime = 1_M;
// constexpr static size_t kBucketGroupNr = 128;
// constexpr static size_t kSlotNr = 128;
// constexpr static size_t kMemoryLimit = 1_G;

DEFINE_string(exec_meta, "", "The meta data of this execution");

void test_basic(size_t initial_subtable)
{
    using RaceHashingT = RaceHashing<4, 64, 64>;

    auto allocator = std::make_shared<patronus::mem::RawAllocator>();
    RaceHashingConfig conf;
    conf.initial_subtable = initial_subtable;
    RaceHashingT rh(allocator, conf);

    RaceHashingHandleConfig handle_conf;
    handle_conf.auto_expand = false;
    handle_conf.auto_update_dir = false;

    RaceHashingT::Handle rhh(
        rh.meta_addr(), handle_conf, RaceHashingRdmaContext::new_instance());

    CHECK_EQ(rhh.put("abc", "def"), kOk);
    std::string get;
    CHECK_EQ(rhh.get("abc", get), kOk);
    CHECK_EQ(get, "def");
    CHECK_EQ(rhh.put("abc", "!!!"), kOk);
    CHECK_EQ(rhh.get("abc", get), kOk);
    CHECK_EQ(get, "!!!");
    CHECK_EQ(rhh.del("abc"), kOk);
    CHECK_EQ(rhh.get("abc", get), kNotFound);
    CHECK_EQ(rhh.del("abs"), kNotFound);
    LOG(INFO) << "max capacity: " << rh.max_capacity();
}

void test_capacity(size_t initial_subtable)
{
    using RaceHashingT = RaceHashing<4, 16, 16>;
    auto allocator = std::make_shared<patronus::mem::RawAllocator>();
    RaceHashingConfig conf;
    conf.initial_subtable = initial_subtable;
    RaceHashingT rh(allocator, conf);

    RaceHashingHandleConfig handle_conf;
    handle_conf.auto_expand = false;
    handle_conf.auto_update_dir = false;

    HashContext dctx(0);
    RaceHashingT::Handle rhh(rh.meta_addr(),
                             handle_conf,
                             RaceHashingRdmaContext::new_instance(&dctx));

    std::string key;
    std::string value;
    key.resize(8);
    value.resize(8);
    std::map<std::string, std::string> inserted;
    size_t succ_nr = 0;
    size_t fail_nr = 0;
    bool first_fail = true;

    for (size_t i = 0; i < rh.max_capacity(); ++i)
    {
        fast_pseudo_fill_buf(key.data(), key.size());
        fast_pseudo_fill_buf(value.data(), value.size());
        dctx.key = key;
        dctx.value = value;

        dctx.op = "put";
        auto rc = rhh.put(key, value, &dctx);
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
    for (const auto &[key, expect_value] : inserted)
    {
        std::string get_val;
        dctx.key = key;
        dctx.value = expect_value;
        dctx.op = "get";
        CHECK_EQ(rhh.get(key, get_val, &dctx), kOk);
        CHECK_EQ(get_val, expect_value);
        CHECK_EQ(rhh.del(key, &dctx), kOk);
        CHECK_EQ(rhh.del(key), kNotFound);
        CHECK_EQ(rhh.get(key, get_val), kNotFound);
    }
    CHECK_EQ(rh.utilization(), 0)
        << "Removed all the items should result in 0 utilization";
    LOG(INFO) << rh;
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

    HashContext dctx(tid);

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
        char mark = 'a' + tid;
        key[0] = mark;

        if (true_with_prob(0.5))
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
        else if (true_with_prob(0.125))
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
            CHECK_EQ(rhh->del(key, &dctx), kOk)
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
                dctx.key = key;
                dctx.value = value;
                dctx.op = "d";
                CHECK_EQ(rhh->del(key, &dctx), kOk) << dctx;
                inserted.erase(key);
                keys.erase(key);
                del_succ_nr++;
            }
            else
            {
                dctx.key = key;
                dctx.value = value;
                dctx.op = "d";
                CHECK_EQ(rhh->del(key, &dctx), kNotFound) << dctx;
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
            CHECK_EQ(key[0], mark) << dctx;
            dctx.key = key;
            dctx.value = value;
            dctx.op = "g";
            CHECK_EQ(rhh->get(key, got_value, &dctx), kOk)
                << "Tid: " << tid << " getting key `" << key
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
                CHECK_EQ(rhh->get(key, got_value, &dctx), kOk) << dctx;
                CHECK_EQ(got_value, inserted[key]) << dctx;
                get_succ_nr++;
            }
            else
            {
                std::string got_value;
                dctx.key = key;
                dctx.value = value;
                dctx.op = "g";
                CHECK_EQ(rhh->get(key, got_value, &dctx), kNotFound) << dctx;
                get_fail_nr++;
            }
        }
    }

    LOG(INFO) << "Finished test. tid: " << tid << ". Table: " << *rh;

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

    LOG(INFO) << "Tear down. tid: " << tid << ". Table: " << *rh;
}

template <size_t kDEntryNr, size_t kBucketGroupNr, size_t kSlotNr>
void test_multithreads(size_t thread_nr, size_t test_nr, bool expand)
{
    auto allocator = std::make_shared<patronus::mem::RawAllocator>();
    RaceHashingConfig conf;
    conf.initial_subtable = expand ? 1 : kDEntryNr;
    auto rh = std::make_shared<RaceHashing<kDEntryNr, kBucketGroupNr, kSlotNr>>(
        allocator, conf);

    std::vector<std::thread> threads;
    // maintain a shared_ptr copy here
    // so that will not be dctor-ed when other threads are still running.
    std::vector<typename RaceHashing<kDEntryNr, kBucketGroupNr, kSlotNr>::
                    Handle::pointer>
        handles;
    for (size_t i = 0; i < thread_nr; ++i)
    {
        RaceHashingHandleConfig handle_conf;
        handle_conf.auto_expand = expand;
        handle_conf.auto_update_dir = expand;
        auto handle = std::make_shared<
            typename RaceHashing<kDEntryNr, kBucketGroupNr, kSlotNr>::Handle>(
            rh->meta_addr(),
            handle_conf,
            RaceHashingRdmaContext::new_instance());
        handles.push_back(handle);
        threads.emplace_back([tid = i, test_nr, handle, rh]() {
            test_thread<kDEntryNr, kBucketGroupNr, kSlotNr>(
                rh, handle, tid, test_nr);
        });
    }
    for (auto &t : threads)
    {
        t.join();
    }
}

void test_expand_once_single_thread()
{
    using RaceHashingT = RaceHashing<2, 4, 4>;

    auto allocator = std::make_shared<patronus::mem::RawAllocator>();
    RaceHashingConfig conf;
    conf.initial_subtable = 1;
    RaceHashingT rh(allocator, conf);

    RaceHashingHandleConfig handle_conf;
    handle_conf.auto_expand = false;
    handle_conf.auto_update_dir = true;
    RaceHashingT::Handle rhh(
        rh.meta_addr(), handle_conf, RaceHashingRdmaContext::new_instance());

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
    HashContext ctx(0);
    ctx.tid = 0;
    while (true)
    {
        fast_pseudo_fill_buf(key_buf, kKeySize);
        fast_pseudo_fill_buf(val_buf, kValueSize);
        std::string key(key_buf, kKeySize);
        std::string value(val_buf, kValueSize);

        ctx.key = key;
        ctx.value = value;
        ctx.op = "put";
        auto rc = rhh.put(key, value, &ctx);
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
    ctx.op = "expand";
    ctx.key = "";
    ctx.value = "";
    rhh.expand(0, &ctx);
    for (const auto &[k, v] : inserted)
    {
        ctx.key = k;
        ctx.value = v;
        ctx.op = "get";
        std::string got_v;
        CHECK_EQ(rhh.get(k, got_v, &ctx), kOk)
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
}

void test_expand_multiple_single_thread()
{
    using RaceHashingT = RaceHashing<16, 4, 4>;

    auto allocator = std::make_shared<patronus::mem::RawAllocator>();
    RaceHashingConfig conf;
    conf.initial_subtable = 1;
    RaceHashingT rh(allocator, conf);

    RaceHashingHandleConfig handle_conf;
    handle_conf.auto_expand = true;
    handle_conf.auto_update_dir = true;
    RaceHashingT::Handle rhh(
        rh.meta_addr(), handle_conf, RaceHashingRdmaContext::new_instance());

    std::string key;
    std::string value;
    constexpr static size_t kKeySize = 8;
    constexpr static size_t kValueSize = 8;
    char key_buf[kKeySize + 5];
    char val_buf[kValueSize + 5];
    key.resize(kKeySize);
    value.resize(kValueSize);
    std::map<std::string, std::string> inserted;
    HashContext ctx(0);
    ctx.tid = 0;
    size_t inserted_nr = 0;
    while (true)
    {
        fast_pseudo_fill_buf(key_buf, kKeySize);
        fast_pseudo_fill_buf(val_buf, kValueSize);
        std::string key(key_buf, kKeySize);
        std::string value(val_buf, kValueSize);

        ctx.key = key;
        ctx.value = value;
        ctx.op = "put";

        auto rc = rhh.put(key, value, &ctx);
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
        ctx.key = k;
        ctx.value = v;
        ctx.op = "get";

        std::string got_v;
        auto rc = rhh.get(k, got_v);
        if (rc != kOk)
        {
            LOG(WARNING) << "Failed to find key `" << k
                         << "`. Start to debug.... Got rc: " << rc;
            // do it again with debug
            ctx.key = k;
            ctx.value = v;
            ctx.op = "get";
            rc = rhh.get(k, got_v, &ctx);
            CHECK(false) << "Failed to get `" << k << "`. Expect value `" << v
                         << "`. Got: " << rc;
        }
        CHECK_EQ(got_v, v);
    }

    // check integrity and tear down

    for (const auto &[k, v] : inserted)
    {
        ctx.key = k;
        ctx.value = v;
        ctx.op = "get";

        std::string got_v;
        CHECK_EQ(rhh.get(k, got_v), kOk) << "failed to get back key " << k;
        CHECK_EQ(got_v, v);
        CHECK_EQ(rhh.del(k, &ctx), kOk) << "failed to delete key " << k;
    }
    LOG(INFO) << "[bench] tear downed. table: " << rh;
}
void test_burn_expand_single_thread()
{
    using RaceHashingT = RaceHashing<128, 2, 2>;

    auto allocator = std::make_shared<patronus::mem::RawAllocator>();
    RaceHashingConfig conf;
    conf.initial_subtable = 1;
    RaceHashingT rh(allocator, conf);

    RaceHashingHandleConfig handle_conf;
    handle_conf.auto_expand = true;
    handle_conf.auto_update_dir = true;
    RaceHashingT::Handle rhh(
        rh.meta_addr(), handle_conf, RaceHashingRdmaContext::new_instance());

    std::string key;
    std::string value;
    constexpr static size_t kKeySize = 8;
    constexpr static size_t kValueSize = 8;
    char key_buf[kKeySize + 5];
    char val_buf[kValueSize + 5];
    key.resize(kKeySize);
    value.resize(kValueSize);
    std::map<std::string, std::string> inserted;
    HashContext ctx(0);
    ctx.tid = 0;
    size_t inserted_nr = 0;
    while (true)
    {
        fast_pseudo_fill_buf(key_buf, kKeySize);
        fast_pseudo_fill_buf(val_buf, kValueSize);
        std::string key(key_buf, kKeySize);
        std::string value(val_buf, kValueSize);

        ctx.key = key;
        ctx.value = value;
        ctx.op = "put";

        auto rc = rhh.put(key, value, &ctx);
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
        ctx.key = k;
        ctx.value = v;
        ctx.op = "get";

        std::string got_v;
        auto rc = rhh.get(k, got_v);
        if (rc != kOk)
        {
            LOG(WARNING) << "Failed to find key `" << k
                         << "`. Start to debug.... Got rc: " << rc;
            // do it again with debug
            ctx.key = k;
            ctx.value = v;
            ctx.op = "get";
            rc = rhh.get(k, got_v, &ctx);
            CHECK(false) << "Failed to get `" << k << "`. Expect value `" << v
                         << "`. Got: " << rc;
        }
        CHECK_EQ(got_v, v);
    }

    // check integrity
    for (const auto &[k, v] : inserted)
    {
        ctx.key = k;
        ctx.value = v;
        ctx.op = "get";

        std::string got_v;
        CHECK_EQ(rhh.get(k, got_v), kOk) << "failed to get back key " << k;
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

        ctx.key = key;
        ctx.value = value;
        ctx.op = "put";

        auto rc = rhh.put(key, value, &ctx);
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
        ctx.key = k;
        ctx.value = v;
        ctx.op = "del";

        CHECK_EQ(rhh.del(k, &ctx), kOk) << "failed to delete key " << k;
    }
    LOG(INFO) << "[bench] tear downed. table: " << rh;
    LOG(INFO) << "[bench] inserted_nr: " << inserted_nr
              << ", another_inserted: " << another_inserted_nr
              << ", table_size: " << rh.max_capacity()
              << " actual utilization: "
              << 1.0 * (inserted_nr + another_inserted_nr) / rh.max_capacity();
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // test_basic(1);
    // test_capacity(1);
    // test_capacity(4);

    // test_multithreads<4, 8, 8>(8, 1_M, false);

    // test_expand_once_single_thread();
    // for (size_t i = 0; i < 100; ++i)
    // {
    //     test_expand_multiple_single_thread();
    // }

    // test_expand_multiple_single_thread();
    // test_burn_expand_single_thread();

    test_multithreads<16, 4, 4>(8, 10, true);

    LOG(INFO) << "PASS ALL TESTS";
    LOG(INFO) << "finished. ctrl+C to quit.";
}