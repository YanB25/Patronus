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
    LOG(INFO) << "max capacity: " << rh.max_capacity();
}

void test_capacity(size_t initial_subtable)
{
    auto allocator = std::make_shared<patronus::mem::RawAllocator>();
    RaceHashing<1, 16, 16> rh(
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
                first_fail = false;
            }
            fail_nr++;
        }
        else
        {
            CHECK(false) << "Unknow return code: " << rc;
        }
    }
    LOG(INFO) << "Inserted " << succ_nr << ", failed: " << fail_nr
              << ", ultilization: " << 1.0 * succ_nr / rh.max_capacity()
              << ", success rate: " << 1.0 * succ_nr / (succ_nr + fail_nr);

    LOG(INFO) << "Checking integrity";

    for (const auto &[key, expect_value] : inserted)
    {
        std::string get_val;
        CHECK_EQ(rh.get(key, get_val), kOk);
        CHECK_EQ(get_val, expect_value);
    }
    LOG(INFO) << rh;
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    test_basic(1);
    test_capacity(1);

    LOG(INFO) << "PASS ALL TESTS";
    LOG(INFO) << "finished. ctrl+C to quit.";
}