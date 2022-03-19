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
                  const void *base_addr,
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
        addrs.push_back((void *) &bucket.slot(i));
    }

    // slots are disjoint
    check_not_overlapped(addrs, Slot::size_bytes());

    check_in_rng(addrs, bucket.buffer_addr(), bucket.size_bytes());
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
        CHECK_EQ((char *) o_from_cb1.buffer_addr(),
                 (char *) o_from_cb2.buffer_addr())
            << "Expect combined bucket from the same BucketGroup to share the "
               "same overflow bucket";
        CHECK_EQ((char *) o_from_cb1.buffer_addr(),
                 (char *) m1.buffer_addr() + Bucket<kSlotNr>::size_bytes());
        CHECK_EQ(
            (char *) m2.buffer_addr(),
            (char *) o_from_cb2.buffer_addr() + Bucket<kSlotNr>::size_bytes());
    }
}

void test_bucket_not_overlapped(BucketGroup<kSlotNr> bucket_group)
{
    std::vector<void *> addrs;

    auto m0 = bucket_group.main_bucket_0();
    auto m1 = bucket_group.main_bucket_1();
    auto o = bucket_group.overflow_bucket();
    addrs.push_back((void *) m0.buffer_addr());
    addrs.push_back((void *) m1.buffer_addr());
    addrs.push_back((void *) o.buffer_addr());

    test_slot_not_overlapped(m0);
    test_slot_not_overlapped(m1);
    test_slot_not_overlapped(o);

    // buckets are disjoint
    check_not_overlapped(addrs, Bucket<kSlotNr>::size_bytes());
    // buckets are in range
    check_in_rng(
        addrs, (void *) bucket_group.buffer_addr(), bucket_group.size_bytes());
}

void test_bucket_group_not_overlapped()
{
    void *addr = hugePageAlloc(kMemoryLimit);
    SubTable<kBucketGroupNr, kSlotNr> sub_table(addr, kMemoryLimit);
    size_t expect_size = SubTable<kBucketGroupNr, kSlotNr>::size_bytes();
    CHECK_GE(kMemoryLimit, expect_size);

    std::vector<void *> addrs;
    for (size_t i = 0; i < kBucketGroupNr; ++i)
    {
        auto bucket_group = sub_table.bucket_group(i);
        addrs.push_back(CHECK_NOTNULL((void *) bucket_group.buffer_addr()));

        test_bucket_not_overlapped(bucket_group);
    }
    // every bucket group is disjoint
    check_not_overlapped(addrs, BucketGroup<kSlotNr>::size_bytes());
    // every bucket group within valid range.
    check_in_rng(addrs, addr, sub_table.size_bytes());

    test_subtable_gen_combined_bucket(sub_table);

    hugePageFree(addr, kMemoryLimit);
}

void test_bucket_handle(const BucketHandle<kSlotNr> &bh)
{
    std::vector<void *> addrs;
    for (size_t i = 0; i < kSlotNr; ++i)
    {
        addrs.push_back((void *) bh.slot_remote_addr(i));
    }
    check_not_overlapped(addrs, Slot::size_bytes());
    check_in_rng(addrs, (void *) bh.remote_addr(), bh.size_bytes());
}

void test_combined_bucket_handle(const CombinedBucketHandle<kSlotNr> &cb)
{
    auto mh = cb.main_bucket_handle();
    auto oh = cb.overflow_bucket_handle();
    test_bucket_handle(mh);
    test_bucket_handle(oh);
    std::vector<void *> addrs;
    addrs.push_back((void *) mh.remote_addr());
    addrs.push_back((void *) oh.remote_addr());
    check_not_overlapped(addrs, Bucket<kSlotNr>::size_bytes());
    check_in_rng(addrs, (void *) cb.remote_addr(), cb.size_bytes());
}

void test_bucket_group_not_overlapped_handle()
{
    void *addr = hugePageAlloc(kMemoryLimit);
    SubTableHandle<kBucketGroupNr, kSlotNr> sub_table(
        0, (uint64_t) addr, kMemoryLimit, 0);
    size_t expect_size = SubTable<kBucketGroupNr, kSlotNr>::size_bytes();
    CHECK_GE(kMemoryLimit, expect_size);

    std::vector<void *> left_addrs;
    std::vector<void *> right_addrs;
    auto rdma_ctx = RaceHashingRdmaContext::new_instance();
    CHECK_EQ(sub_table.kCombinedBucketNr % 2, 0);
    std::vector<void *> buckets;
    for (size_t i = 0; i < sub_table.kCombinedBucketNr; i += 2)
    {
        auto left_idx = i;
        auto right_idx = i + 1;
        auto cb_left = sub_table.combined_bucket_handle(left_idx);
        auto cb_right = sub_table.combined_bucket_handle(right_idx);
        auto overflow_left_addr =
            cb_left.overflow_bucket_handle().remote_addr();
        auto overflow_right_addr =
            cb_right.overflow_bucket_handle().remote_addr();
        CHECK_EQ(overflow_left_addr, overflow_right_addr);
        auto left_main_addr = cb_left.main_bucket_handle().remote_addr();
        auto right_main_addr = cb_right.main_bucket_handle().remote_addr();
        CHECK_EQ(left_main_addr + Bucket<kSlotNr>::size_bytes(),
                 overflow_left_addr);
        CHECK_EQ(overflow_right_addr + Bucket<kSlotNr>::size_bytes(),
                 right_main_addr);
        test_combined_bucket_handle(cb_left);
        test_combined_bucket_handle(cb_right);
        buckets.push_back((void *) left_main_addr);
        buckets.push_back((void *) right_main_addr);
        buckets.push_back((void *) overflow_left_addr);
        buckets.push_back((void *) overflow_right_addr);
    }
    // every bucket group within valid range.
    check_in_rng(buckets, addr, sub_table.size_bytes());

    hugePageFree(addr, kMemoryLimit);
}

void test_bucket_group_not_overlapped_handle2()
{
    void *addr = hugePageAlloc(kMemoryLimit);
    SubTableHandle<kBucketGroupNr, kSlotNr> sub_table(
        0, (uint64_t) addr, kMemoryLimit, 0);
    size_t expect_size = SubTable<kBucketGroupNr, kSlotNr>::size_bytes();
    CHECK_GE(kMemoryLimit, expect_size);

    std::vector<void *> addrs;
    auto rdma_ctx = RaceHashingRdmaContext::new_instance();
    for (size_t i = 0; i < sub_table.kCombinedBucketNr; ++i)
    {
        auto cb = sub_table.combined_bucket_handle(i);
        CHECK_EQ(cb.read(*rdma_ctx), kOk);
        CHECK_EQ(rdma_ctx->commit(), kOk);
        addrs.push_back((void *) cb.remote_addr());
        test_combined_bucket_handle(cb);
    }
    // every bucket group within valid range.
    check_in_rng(addrs, addr, sub_table.size_bytes());

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

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    LOG(INFO) << "Testing bucket_group";
    test_bucket_group_not_overlapped();

    LOG(INFO) << "Testing bucket_group for handle";
    test_bucket_group_not_overlapped_handle();

    test_bucket_group_not_overlapped_handle2();

    LOG(INFO) << "PASS ALL TESTS";
    LOG(INFO) << "finished. ctrl+C to quit.";
}