#pragma once
#ifndef PERTRONUS_RACEHASHING_SUBTABLE_H_
#define PERTRONUS_RACEHASHING_SUBTABLE_H_

#include <cstddef>
#include <cstdint>

#include "./bucket_group.h"
#include "./kv_block.h"
#include "util/PerformanceReporter.h"

namespace patronus::hash
{
constexpr static bool kEnableDebug = config::kEnableDebug;
// NOTE:
// one bucket group has two bucket and one overflow bucket
// when using hash value to indexing the bucket
// the bucket_nr == 2 * bucket_group_nr
template <size_t kBucketGroupNr, size_t kSlotNr>
class SubTable
{
public:
    constexpr static size_t kMainBucketNr = 2 * kBucketGroupNr;
    constexpr static size_t kOverflowBucketNr = kBucketGroupNr;
    constexpr static size_t kTotalBucketNr = kMainBucketNr + kOverflowBucketNr;

    constexpr static size_t kCombinedBucketNr = kMainBucketNr;

    static_assert(is_power_of_two(kCombinedBucketNr));

    static constexpr size_t size_bytes()
    {
        return BucketGroup<kSlotNr>::size_bytes() * kBucketGroupNr;
    }

    SubTable(uint32_t ld, void *addr, size_t size, uint64_t hash_suffix)
        : ld_(ld), addr_(addr), size_(size), hash_suffix_(hash_suffix)
    {
        CHECK_GE(size_, size_bytes());
        update_header(ld, hash_suffix);
    }

    void update_header(uint32_t ld, uint32_t suffix)
    {
        for (size_t b = 0; b < kBucketGroupNr; ++b)
        {
            auto bg = bucket_group(b);
            bg.setup_header(ld, suffix);
        }
    }
    constexpr static size_t max_capacity()
    {
        return kTotalBucketNr * Bucket<kSlotNr>::max_capacity();
    }

    BucketGroup<kSlotNr> bucket_group(size_t idx) const
    {
        DCHECK_LT(idx, kBucketGroupNr);
        auto *addr = (char *) addr_ + idx * kItemSize;
        return BucketGroup<kSlotNr>(addr);
    }

    double utilization() const
    {
        OnePassMonitor m;
        for (size_t i = 0; i < kBucketGroupNr; ++i)
        {
            double util = bucket_group(i).utilization();
            m.collect(util);
        }
        return m.average();
    }

    constexpr static size_t kItemSize = BucketGroup<kSlotNr>::size_bytes();

    CombinedBucket<kSlotNr> combined_bucket(size_t idx)
    {
        idx = idx % kCombinedBucketNr;
        if (idx % 2 == 0)
        {
            // even
            idx /= 2;
            void *bucket_group_addr = (char *) addr_ + kItemSize * idx;
            return CombinedBucket<kSlotNr>(bucket_group_addr, true);
        }
        else
        {
            // odd
            idx = (idx - 1) / 2;
            void *bucket_group_addr = (char *) addr_ + kItemSize * idx;
            void *overflow_bucket_addr =
                (char *) bucket_group_addr + Bucket<kSlotNr>::size_bytes();
            return CombinedBucket<kSlotNr>(overflow_bucket_addr, false);
        }
    }

private:
    uint32_t ld_;
    void *addr_;
    size_t size_;
    uint64_t hash_suffix_;
};

template <size_t kBucketGroupNr, size_t kSlotNr>
class SubTableHandle
{
public:
    constexpr static size_t kMainBucketNr = 2 * kBucketGroupNr;
    constexpr static size_t kOverflowBucketNr = kBucketGroupNr;
    constexpr static size_t kTotalBucketNr = kMainBucketNr + kOverflowBucketNr;

    constexpr static size_t kCombinedBucketNr = kMainBucketNr;

    static_assert(is_power_of_two(kCombinedBucketNr));

    SubTableHandle(uint32_t expect_ld,
                   uint64_t st_addr,
                   size_t st_size,
                   uint64_t hash_suffix)
        : expect_ld_(expect_ld),
          st_addr_(st_addr),
          st_size_(st_size),
          hash_suffix_(hash_suffix)
    {
        CHECK_GE(st_size, size_bytes());
        CHECK_NE(st_addr_, 0);
    }
    uint32_t expect_ld() const
    {
        return expect_ld_;
    }
    uint32_t hash_suffix() const
    {
        return hash_suffix_;
    }

    /**
     * @brief Get the combine buckets object
     *
     * @param h1 the one combined bucket
     * @param h2 the two combined bucket
     * @return TwoCombinedBucketHandle<kSlotNr>
     */
    TwoCombinedBucketHandle<kSlotNr> get_two_combined_bucket_handle(
        uint64_t h1, uint64_t h2, RaceHashingRdmaContext &rdma_ctx)
    {
        auto cb1 = combined_bucket_handle(h1);
        auto cb2 = combined_bucket_handle(h2);
        return TwoCombinedBucketHandle<kSlotNr>(
            h1, h2, std::move(cb1), std::move(cb2), rdma_ctx);
    }
    constexpr static size_t max_item_nr()
    {
        return kBucketGroupNr * BucketGroup<kSlotNr>::max_item_nr();
    }

    void *st_addr() const
    {
        return (void *) st_addr_;
    }

    static constexpr size_t size_bytes()
    {
        return BucketGroup<kSlotNr>::size_bytes() * kBucketGroupNr;
    }

    static constexpr size_t max_capacity()
    {
        return SubTable<kBucketGroupNr, kSlotNr>::max_capacity();
    }
    CombinedBucketHandle<kSlotNr> combined_bucket_handle(size_t idx)
    {
        auto origin_idx = idx;
        idx = idx % kCombinedBucketNr;
        if (idx % 2 == 0)
        {
            // even
            idx /= 2;
            void *bucket_group_addr = (char *) st_addr_ + kItemSize * idx;
            DLOG_IF(INFO, config::kEnableMemoryDebug)
                << "[race][mem] combined_bucket_handle: left. indexing "
                << origin_idx << ", map to idx " << idx
                << ", from kCombinedBucketNr: " << kCombinedBucketNr
                << ", st_addr: " << (void *) st_addr_
                << ", kItemSize: " << kItemSize << ", got "
                << (void *) bucket_group_addr;
            return CombinedBucketHandle<kSlotNr>((uint64_t) bucket_group_addr,
                                                 true);
        }
        else
        {
            // odd
            idx = (idx - 1) / 2;
            void *bucket_group_addr = (char *) st_addr_ + kItemSize * idx;
            void *overflow_bucket_addr =
                (char *) bucket_group_addr + Bucket<kSlotNr>::size_bytes();
            LOG_IF(INFO, config::kEnableMemoryDebug)
                << "[race][mem] combined_bucket_handle: right. indexing "
                << origin_idx << ", map to idx " << idx
                << " from kCombinedBucketNr: " << kCombinedBucketNr
                << ", st_addr: " << (void *) st_addr_
                << ", kItemSize: " << kItemSize << ", got "
                << (void *) overflow_bucket_addr;
            return CombinedBucketHandle<kSlotNr>(
                (uint64_t) overflow_bucket_addr, false);
        }
    }

private:
    constexpr static size_t kItemSize =
        BucketGroupHandle<kSlotNr>::size_bytes();
    uint32_t expect_ld_;
    uint64_t st_addr_;
    size_t st_size_;
    uint64_t hash_suffix_;
};

template <size_t kBucketGroupNr, size_t kSlotNr>
inline std::ostream &operator<<(std::ostream &os,
                                const SubTable<kBucketGroupNr, kSlotNr> &st)
{
    OnePassMonitorImpl<double> m;
    for (size_t i = 0; i < kBucketGroupNr; ++i)
    {
        double util = st.bucket_group(i).utilization();
        m.collect(util);
    }
    os << "Util: " << m.abs_average() << ": " << m;
    return os;
}
}  // namespace patronus::hash

#endif