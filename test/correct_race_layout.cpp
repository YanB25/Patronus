#include <algorithm>
#include <random>
#include <set>

#include "Common.h"
#include "glog/logging.h"
#include "patronus/memory/direct_allocator.h"
#include "thirdparty/racehashing/hashtable.h"
#include "util/Rand.h"

using namespace patronus::hash;
using namespace define::literals;

// constexpr static size_t kTestTime = 1_M;
constexpr static size_t kBucketGroupNr = 128;
constexpr static size_t kSlotNr = 128;
constexpr static size_t kMemoryLimit = 1_G;

DEFINE_string(exec_meta, "", "The meta data of this execution");

void check_not_overlapped(const std::vector<void *> addrs, size_t size)
{
    for (size_t i = 0; i < addrs.size(); ++i)
    {
        for (size_t j = 0; j < i; ++j)
        {
            CHECK_GE(std::abs((int64_t) addrs[i] - (int64_t) addrs[j]), size)
                << "lhs: addr[" << i << "] " << (void *) addrs[i]
                << ", rhs: addr[" << j << "] " << (void *) addrs[j];
        }
    }
}
void check_in_rng(const std::vector<void *> addrs,
                  void *base_addr,
                  size_t addr_size)
{
    for (size_t i = 0; i < addrs.size(); ++i)
    {
        CHECK_GE(addrs[i], base_addr);
        CHECK_LT((char *) addrs[i], (char *) base_addr + addr_size);
    }
}

void test_slot_not_overlapped(Bucket<kSlotNr> bucket)
{
    std::vector<void *> addrs;
    for (size_t i = 0; i < kSlotNr; ++i)
    {
        addrs.push_back(bucket.slot(i).addr());
    }

    // slots are disjoint
    check_not_overlapped(addrs, Slot::size_bytes());

    check_in_rng(addrs, bucket.addr(), bucket.size_bytes());
}

// whether subtable.combined_bucket works well
void test_subtable_gen_combined_bucket(SubTable<kBucketGroupNr, kSlotNr> &st)
{
    for (size_t i = 0; i < kBucketGroupNr; ++i)
    {
        auto id = i * 2;
        // the combined bucket should have overflow disjoint
        auto cb1 = st.combined_bucket(id);
        auto cb2 = st.combined_bucket(id + 1);

        auto m1 = cb1.main_bucket();
        auto o_from_cb1 = cb1.overflow_bucket();
        auto m2 = cb2.main_bucket();
        auto o_from_cb2 = cb2.overflow_bucket();
        CHECK_EQ(o_from_cb1.addr(), o_from_cb2.addr())
            << "Expect combined bucket from the same BucketGroup to share the "
               "same overflow bucket";
        CHECK_EQ(o_from_cb1.addr(),
                 (char *) m1.addr() + Bucket<kSlotNr>::size_bytes());
        CHECK_EQ(m2.addr(),
                 (char *) o_from_cb2.addr() + Bucket<kSlotNr>::size_bytes());
    }
}

void test_bucket_not_overlapped(BucketGroup<kSlotNr> bucket_group)
{
    std::vector<void *> addrs;

    auto m0 = bucket_group.main_bucket_0();
    auto m1 = bucket_group.main_bucket_1();
    auto o = bucket_group.overflow_bucket();
    addrs.push_back(m0.addr());
    addrs.push_back(m1.addr());
    addrs.push_back(o.addr());

    test_slot_not_overlapped(m0);
    test_slot_not_overlapped(m1);
    test_slot_not_overlapped(o);

    // buckets are disjoint
    check_not_overlapped(addrs, Bucket<kSlotNr>::size_bytes());
    // buckets are in range
    check_in_rng(addrs, bucket_group.addr(), bucket_group.size_bytes());
}

void test_bucket_group_not_overlapped()
{
    void *addr = hugePageAlloc(kMemoryLimit);
    SubTable<kBucketGroupNr, kSlotNr> sub_table(0, addr, kMemoryLimit, 0);
    size_t expect_size = SubTable<kBucketGroupNr, kSlotNr>::size_bytes();
    CHECK_GE(kMemoryLimit, expect_size);

    std::vector<void *> addrs;
    for (size_t i = 0; i < kBucketGroupNr; ++i)
    {
        auto bucket_group = sub_table.bucket_group(i);
        addrs.push_back(CHECK_NOTNULL(bucket_group.addr()));

        test_bucket_not_overlapped(bucket_group);
    }
    // every bucket group is disjoint
    check_not_overlapped(addrs, BucketGroup<kSlotNr>::size_bytes());
    // every bucket group within valid range.
    check_in_rng(addrs, addr, sub_table.size_bytes());

    test_subtable_gen_combined_bucket(sub_table);

    hugePageFree(addr, kMemoryLimit);
}

struct Record
{
    Record(size_t bgid, size_t bid, size_t sid) : bgid(bgid), bid(bid), sid(sid)
    {
    }
    size_t bgid;
    size_t bid;
    size_t sid;
    bool operator<(const Record &rhs) const
    {
        if (bgid != rhs.bgid)
        {
            return bgid < rhs.bgid;
        }
        if (bid != rhs.bid)
        {
            return bid < rhs.bid;
        }
        return sid < rhs.sid;
    }
};

void test_writable()
{
    void *addr = hugePageAlloc(kMemoryLimit);
    SubTable<kBucketGroupNr, kSlotNr> sub_table(0, addr, kMemoryLimit, 0);

    std::map<Record, std::shared_ptr<int>> records;
    for (size_t i = 0; i < 10_K; ++i)
    {
        auto bgid = fast_pseudo_rand_int(0, kBucketGroupNr - 1);
        auto group = sub_table.bucket_group(bgid);
        auto bid = fast_pseudo_rand_int(0, 2);
        auto bucket = group.bucket(bid);
        auto sid = fast_pseudo_rand_int(0, kSlotNr - 1);
        auto &slot = bucket.slot(sid);
        auto sptr = std::make_shared<int>();
        slot.with_view().set_ptr(sptr.get());

        records[Record(bgid, bid, sid)] = sptr;
    }
    for (const auto &[record, sptr] : records)
    {
        auto bgid = record.bgid;
        auto group = sub_table.bucket_group(bgid);
        auto bid = record.bid;
        auto bucket = group.bucket(bid);
        auto sid = record.sid;
        auto &slot = bucket.slot(sid);
        auto *got_ptr = slot.view().ptr();
        CHECK_EQ(got_ptr, sptr.get());
    }

    hugePageFree(addr, kMemoryLimit);
}

void test_put_get(size_t initial_subtable)
{
    auto allocator = std::make_shared<patronus::mem::RawAllocator>();
    RaceHashingConfig conf;
    conf.initial_subtable = initial_subtable;
    RaceHashing<4, 64, 64> rh(allocator, conf);
    CHECK_EQ(rh.put("abc", "def"), kOk);
    std::string get;
    CHECK_EQ(rh.get("abc", get), kOk);
    CHECK_EQ(get, "def");
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    LOG(INFO) << "Testing bucket_group";
    test_bucket_group_not_overlapped();

    LOG(INFO) << "Testing data write in mem";
    test_writable();

    LOG(INFO) << "Testing insert data";
    test_put_get(1);

    LOG(INFO) << "PASS ALL TESTS";
    LOG(INFO) << "finished. ctrl+C to quit.";
}