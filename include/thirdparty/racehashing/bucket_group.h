#pragma once
#ifndef PERTRONUS_RACEHASHING_BUCKETGROUP_H_
#define PERTRONUS_RACEHASHING_BUCKETGROUP_H_

#include <cstddef>
#include <cstdint>

#include "./bucket.h"

namespace patronus::hash
{
template <size_t kSlotNr>
class CombinedBucketView
{
public:
    CombinedBucketView(void *main, void *overflow)
        : main_(Bucket<kSlotNr>(main)), overflow_(Bucket<kSlotNr>(overflow))
    {
    }
    Bucket<kSlotNr> main_bucket()
    {
        return main_;
    }
    Bucket<kSlotNr> overflow_bucket()
    {
        return overflow_;
    }
    std::vector<SlotWithView> locate_migratable(size_t test_bit)
    {
        auto main_ret = main_.locate_migratable(test_bit);
        auto o_ret = overflow_.locate_migratable(test_bit);
        main_ret.insert(main_ret.begin(),
                        std::make_move_iterator(o_ret.begin()),
                        std::make_move_iterator(o_ret.end()));
        return main_ret;
    }
    std::vector<SlotWithView> fetch_empty(size_t limit) const
    {
        // NOTE:
        // fetch from main bucket first
        // this is done in local memory, should be very fast.
        auto main_ret = main_.fetch_empty(limit);
        if (main_ret.size() >= limit)
        {
            return main_ret;
        }
        auto overflow_ret = overflow_.fetch_empty(limit - main_ret.size());
        main_ret.insert(main_ret.end(),
                        std::make_move_iterator(overflow_ret.begin()),
                        std::make_move_iterator(overflow_ret.end()));
        return main_ret;
    }
    std::vector<SlotWithView> locate(uint8_t fp, HashContext *dctx = nullptr)
    {
        auto slots_main = main_.locate(fp, dctx);
        auto slots_overflow = overflow_.locate(fp, dctx);
        DVLOG(5) << "[race][cb] locate fp " << pre_fp(fp)
                 << " from main bucket " << slots_main.size()
                 << " slots, overflow " << slots_overflow.size() << " slots.";
        slots_main.insert(slots_main.end(),
                          std::make_move_iterator(slots_overflow.begin()),
                          std::make_move_iterator(slots_overflow.end()));
        return slots_main;
    }
    std::pair<std::vector<SlotWithView>, SlotWithView> locate_or_empty(
        uint8_t fp)
    {
        auto [main_slots, main_empty_slot] = main_.locate_or_empty(fp);
        auto [overflow_slots, overflow_empty_slot] =
            overflow_.locate_or_empty(fp);
        DVLOG(5) << "[race][cb] locate_or_empty fp " << pre_fp(fp)
                 << " main bucket " << main_slots.size()
                 << " slots, find empty: " << (main_empty_slot != nullptr)
                 << "; overflow bucket: " << overflow_slots.size()
                 << " slots, find empty: " << (overflow_empty_slot != nullptr);

        main_slots.insert(main_slots.end(),
                          std::make_move_iterator(overflow_slots.begin()),
                          std::make_move_iterator(overflow_slots.end()));
        Slot *ret_empty_slot =
            main_empty_slot ? main_empty_slot : overflow_empty_slot;
        return {main_slots, ret_empty_slot};
    }
    RetCode validate_staleness(uint32_t expect_ld, uint32_t suffix)
    {
        auto ec = main_.validate_staleness(expect_ld, suffix);
        return ec;
    }

private:
    Bucket<kSlotNr> main_;
    Bucket<kSlotNr> overflow_;
};

// a bucket group is {MainBucket_1, OverflowBucket, MainBucket_2}
template <size_t kSlotNr>
class BucketGroup
{
public:
    BucketGroup(void *addr) : addr_((Bucket<kSlotNr> *) addr)
    {
    }
    Bucket<kSlotNr> bucket(size_t idx) const
    {
        DCHECK_GE(idx, 0);
        DCHECK_LT(idx, 3);
        return Bucket<kSlotNr>((char *) addr_ + idx * kItemSize);
    }
    Bucket<kSlotNr> main_bucket_0() const
    {
        return bucket(0);
    }
    Bucket<kSlotNr> main_bucket_1() const
    {
        return bucket(2);
    }
    Bucket<kSlotNr> overflow_bucket() const
    {
        return bucket(1);
    }
    CombinedBucketView<kSlotNr> combined_bucket_0()
    {
        return CombinedBucketView<kSlotNr>((char *) addr_,
                                           (char *) addr_ + 1 * kItemSize);
    }
    CombinedBucketView<kSlotNr> &combined_bucket_1()
    {
        return CombinedBucketView<kSlotNr>((char *) addr_ + 2 * kItemSize,
                                           (char *) addr_ + 1 * kItemSize);
    }
    constexpr static size_t max_item_nr()
    {
        return Bucket<kSlotNr>::max_item_nr() * 3;
    }
    void *addr() const
    {
        return addr_;
    }
    constexpr static size_t size_bytes()
    {
        return Bucket<kSlotNr>::size_bytes() * 3;
    }
    double utilization() const
    {
        double sum = main_bucket_0().utilization() +
                     main_bucket_1().utilization() +
                     overflow_bucket().utilization();
        return sum / 3;
    }
    void setup_header(uint32_t ld, uint32_t hash_suffix)
    {
        main_bucket_0().setup_header(ld, hash_suffix);
        main_bucket_1().setup_header(ld, hash_suffix);
        overflow_bucket().setup_header(ld, hash_suffix);
    }

private:
    constexpr static size_t kItemSize = Bucket<kSlotNr>::size_bytes();
    Bucket<kSlotNr> *addr_{nullptr};
};
}  // namespace patronus::hash
#endif