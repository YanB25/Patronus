#pragma once
#ifndef PATRONUS_LOCK_MANAGER_H_
#define PATRONUS_LOCK_MANAGER_H_

#include <atomic>
#include <cinttypes>
#include <vector>

#include "Common.h"

namespace patronus
{
/**
 * Minimal concurrent access from the same bucket.
 */
template <size_t kBucketNr, size_t kSlotNr>
class LockManager
{
public:
    static constexpr size_t V = ::config::verbose::kPatronusUtils;
    using bucket_t = uint32_t;
    using slot_t = uint32_t;

    static_assert(std::numeric_limits<bucket_t>::max() > kBucketNr);
    static_assert(std::numeric_limits<slot_t>::max() > kSlotNr);
    constexpr static size_t kSlotAlign = 4096 * 8;

    static constexpr size_t slot_nr()
    {
        return constexpr_ceil(((double) (kSlotNr)) / (kSlotAlign)) * kSlotAlign;
    }
    static constexpr size_t bucket_nr()
    {
        return kBucketNr;
    }

    bool try_lock(bucket_t b, slot_t s)
    {
        DCHECK_LT(b, bucket_nr());
        DCHECK_LT(s, slot_nr());
        auto slot_byte = s / 8;
        auto slot_bit = s % 8;
        DCHECK_LT(b, locks_.size());
        DCHECK_LT(slot_byte, locks_[b].size());

        auto &bucket = locks_[b];
        std::atomic<uint8_t> &byte = bucket[slot_byte];

        uint8_t expect = byte.load(std::memory_order_relaxed);
        uint8_t target_bit = 1 << slot_bit;
        DVLOG(V) << "[lock] locking bucket " << b << ", slot_byte " << slot_byte
                 << ", bit " << slot_bit
                 << ", expect: " << std::bitset<8>(expect)
                 << ", addr: " << (void *) &byte;
        if (unlikely(expect & target_bit))
        {
            return false;
        }
        return byte.compare_exchange_strong(
            expect, expect | target_bit, std::memory_order_acquire);
    }

    /**
     * @brief unlock is always idempotent
     * - unlocking an unlocked slot is a well-defined behavior
     */
    bool unlock(bucket_t b, slot_t s)
    {
        DCHECK_LT(b, bucket_nr());
        DCHECK_LT(s, slot_nr());
        auto slot_byte = s / 8;
        auto slot_bit = s % 8;
        DCHECK_LT(b, locks_.size());
        DCHECK_LT(slot_byte, locks_[b].size());

        auto &bucket = locks_[b];
        std::atomic<uint8_t> &byte = bucket[slot_byte];

        uint8_t expect = byte.load(std::memory_order_relaxed);
        uint8_t target_bit = 1 << slot_bit;
        DVLOG(V) << "[lock] unlocking bucket " << b
                 << ", slot_byte: " << slot_byte << ", bit " << slot_bit
                 << ", expect: " << std::bitset<8>(expect)
                 << ", addr: " << (void *) &byte;
        bool locked = expect & target_bit;
        if (unlikely(!locked))
        {
            return false;
        }
        while (expect & target_bit)
        {
            bool succ = byte.compare_exchange_strong(
                expect, expect & (~target_bit), std::memory_order_release);
            if (succ)
            {
                return true;
            }
        }
        return false;
    }

private:
    static constexpr std::int32_t constexpr_ceil(float num)
    {
        std::int32_t inum = static_cast<std::int32_t>(num);
        if (num == static_cast<float>(inum))
        {
            return inum;
        }
        return inum + (num > 0 ? 1 : 0);
    }

    // batch 8 bits into one byte.
    constexpr static size_t impl_bucket_nr = bucket_nr();
    constexpr static size_t impl_slot_nr = slot_nr() / 8;
    using impl_bucket_t = std::array<std::atomic<uint8_t>, impl_slot_nr>;
    using impl_locks_t = std::array<impl_bucket_t, impl_bucket_nr>;
    impl_locks_t locks_{};
};

}  // namespace patronus
#endif