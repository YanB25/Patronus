#include <algorithm>
#include <random>
#include <set>

#include "Common.h"
#include "glog/logging.h"
#include "patronus/memory/direct_allocator.h"
#include "thirdparty/racehashing/hashtable.h"
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
    auto allocator = std::make_shared<patronus::mem::RawAllocator>();
    RaceHashing<4, 64, 64> rh(
        allocator, initial_subtable, fast_pseudo_rand_int());
    CHECK_EQ(rh.put("abc", "def"), kOk);
    std::string get;
    CHECK_EQ(rh.get("abc", get), kOk);
    CHECK_EQ(get, "def");
    CHECK_EQ(rh.put("abc", "!!!"), kOk);
    CHECK_EQ(rh.get("abc", get), kOk);
    CHECK_EQ(get, "!!!");
    CHECK_EQ(rh.del("abc"), kOk);
    CHECK_EQ(rh.get("abc", get), kNotFound);
    CHECK_EQ(rh.del("abs"), kNotFound);
    LOG(INFO) << "max capacity: " << rh.max_capacity();
}

void test_capacity(size_t initial_subtable)
{
    auto allocator = std::make_shared<patronus::mem::RawAllocator>();
    RaceHashing<4, 16, 16> rh(
        allocator, initial_subtable, fast_pseudo_rand_int());

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
        auto rc = rh.put(key, value);
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

    LOG(INFO) << "Inserted " << succ_nr << ", failed: " << fail_nr
              << ", ultilization: " << 1.0 * succ_nr / rh.max_capacity()
              << ", success rate: " << 1.0 * succ_nr / (succ_nr + fail_nr);

    LOG(INFO) << "Checking integrity";

    LOG(INFO) << rh;
    for (const auto &[key, expect_value] : inserted)
    {
        std::string get_val;
        CHECK_EQ(rh.get(key, get_val), kOk);
        CHECK_EQ(get_val, expect_value);
        CHECK_EQ(rh.del(key), kOk);
        CHECK_EQ(rh.del(key), kNotFound);
        CHECK_EQ(rh.get(key, get_val), kNotFound);
    }
    LOG(INFO) << rh;
}

template <size_t kA, size_t kB, size_t kC>
void test_thread(typename RaceHashing<kA, kB, kC>::pointer rh,
                 size_t tid,
                 size_t test_nr,
                 bool thread_modify_local_key)
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

    HashContext dctx(tid, "", "");

    for (size_t i = 0; i < test_nr; ++i)
    {
        fast_pseudo_fill_buf(key_buf, kKeySize);
        fast_pseudo_fill_buf(val_buf, kValueSize);
        std::string key(key_buf, kKeySize);
        std::string value(val_buf, kValueSize);

        // so each thread will not modify others value
        char mark = 'a' + tid;
        if (thread_modify_local_key)
        {
            key[0] = mark;
        }

        if (true_with_prob(0.5))
        {
            // insert
            dctx.key = key;
            dctx.value = value;
            dctx.op = "p";
            auto rc = rh->put(key, value, &dctx);
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
            CHECK_EQ(rh->del(key, &dctx), kOk)
                << "tid " << tid << " deleting key `" << key
                << "` expect to succeed";
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
                CHECK_EQ(rh->del(key, &dctx), kOk);
                inserted.erase(key);
                keys.erase(key);
                del_succ_nr++;
            }
            else
            {
                dctx.key = key;
                dctx.value = value;
                dctx.op = "d";
                CHECK_EQ(rh->del(key, &dctx), kNotFound);
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
            CHECK_EQ(key[0], mark);
            dctx.key = key;
            dctx.value = value;
            dctx.op = "g";
            CHECK_EQ(rh->get(key, got_value, &dctx), kOk)
                << "Tid: " << tid << " getting key `" << key
                << "` expect to succeed";
            CHECK_EQ(got_value, inserted[key]);
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
                CHECK_EQ(rh->get(key, got_value, &dctx), kOk);
                CHECK_EQ(got_value, inserted[key]);
                get_succ_nr++;
            }
            else
            {
                std::string got_value;
                dctx.key = key;
                dctx.value = value;
                dctx.op = "g";
                CHECK_EQ(rh->get(key, got_value, &dctx), kNotFound);
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
        CHECK_EQ(rh->get(k, get_v, &dctx), kOk);
        CHECK_EQ(get_v, v);
        dctx.key = k;
        dctx.value = v;
        CHECK_EQ(rh->del(k, &dctx), kOk);
    }

    LOG(INFO) << "Tear down. tid: " << tid << ". Table: " << *rh;
}

void test_multithreads(size_t initial_subtable,
                       size_t thread_nr,
                       size_t test_nr,
                       bool thread_modify_local_key)
{
    auto allocator = std::make_shared<patronus::mem::RawAllocator>();
    auto rh = std::make_shared<RaceHashing<4, 8, 8>>(
        allocator, initial_subtable, fast_pseudo_rand_int());

    std::vector<std::thread> threads;
    for (size_t i = 0; i < thread_nr; ++i)
    {
        threads.emplace_back(
            [&rh, tid = i, test_nr, thread_modify_local_key]() {
                test_thread<4, 8, 8>(rh, tid, test_nr, thread_modify_local_key);
            });
    }
    for (auto &t : threads)
    {
        t.join();
    }
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    test_basic(1);
    test_capacity(1);
    test_capacity(4);

    test_multithreads(4, 8, 1_M, true);

    LOG(INFO) << "PASS ALL TESTS";
    LOG(INFO) << "finished. ctrl+C to quit.";
}