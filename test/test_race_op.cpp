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

DEFINE_string(exec_meta, "", "The meta data of this execution");

void test_capacity(size_t initial_subtable)
{
    auto allocator = std::make_shared<patronus::mem::RawAllocator>();
    RaceHashing<1, 2, 2> rh(
        allocator, initial_subtable, fast_pseudo_rand_int());

    std::string key;
    std::string value;
    std::map<std::string, std::string> inserted;
    size_t succ_nr = 0;
    size_t fail_nr = 0;
    char key_buf[128];
    char value_buf[128];
    for (size_t i = 0; i < 16; ++i)
    {
        fast_pseudo_fill_buf(key_buf, 8);
        fast_pseudo_fill_buf(value_buf, 8);
        key = std::string(key_buf, 8);
        value = std::string(value_buf, 8);
        LOG(INFO) << "\nTrying to push " << key << ", " << value;
        auto rc = rh.put(key, value);
        if (rc == kOk)
        {
            inserted.emplace(key, value);
            succ_nr++;
        }
        else if (rc == kNoMem)
        {
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

    test_capacity(1);

    LOG(INFO) << "finished. ctrl+C to quit.";
}