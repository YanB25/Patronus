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

    SubTable(void *addr, size_t size) : addr_(addr), size_(size)
    {
        CHECK_GE(size_, size_bytes());
    }
    uint32_t ld() const
    {
        auto first_bucket = Bucket<kSlotNr>(addr_);
        auto &header = first_bucket.header();
        return header.ld;
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
    void *addr_;
    size_t size_;
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
    RetCode try_del_slot(SlotHandle slot_handle,
                         RaceHashingRdmaContext &rdma_ctx)
    {
        auto remote = slot_handle.remote_addr();
        uint64_t expect_val = slot_handle.slot_view().val();
        auto clear_view = slot_handle.view_after_clear();
        auto *rdma_buf = DCHECK_NOTNULL(rdma_ctx.get_rdma_buffer(8));
        CHECK_EQ(
            rdma_ctx.rdma_cas(remote, expect_val, clear_view.val(), rdma_buf),
            kOk);
        CHECK_EQ(rdma_ctx.commit(), kOk);
        uint64_t read = *(uint64_t *) rdma_buf;
        bool success = read == expect_val;
        if (success)
        {
            return kOk;
        }
        return kRetry;
    }
    RetCode del_slot(SlotHandle slot_handle, RaceHashingRdmaContext &rdma_ctx)
    {
        CHECK_EQ(try_del_slot(slot_handle, rdma_ctx), kOk);
        return kOk;
    }
    RetCode put_slot(SlotMigrateHandle slot_handle,
                     RaceHashingRdmaContext &rdma_ctx,
                     SlotHandle *ret,
                     HashContext *dctx)
    {
        auto hash = slot_handle.hash();
        auto h1 = hash_1(hash);
        auto h2 = hash_2(hash);

        auto cbs = get_two_combined_bucket_handle(h1, h2, rdma_ctx);
        CHECK_EQ(rdma_ctx.commit(), kOk);

        std::vector<BucketHandle<kSlotNr>> buckets;
        buckets.reserve(4);
        CHECK_EQ(cbs.get_bucket_handle(buckets), kOk);
        for (auto &bucket : buckets)
        {
            auto poll_slot_idx = fast_pseudo_rand_int(1, kSlotNr - 1);
            constexpr auto kDataSlotNr = Bucket<kSlotNr>::kDataSlotNr;
            for (size_t i = 0; i < kDataSlotNr; ++i)
            {
                auto idx = (poll_slot_idx + i) % kDataSlotNr + 1;
                DCHECK_GE(idx, 1);
                DCHECK_LT(idx, kSlotNr);
                auto view = bucket.slot_view(idx);
                if (view.empty())
                {
                    auto rc = bucket.do_insert(bucket.slot_handle(idx),
                                               slot_handle.slot_view(),
                                               rdma_ctx,
                                               ret,
                                               dctx);
                    CHECK(rc == kOk || rc == kRetry) << "Unexpected rc " << rc;
                    if (rc == kOk)
                    {
                        return rc;
                    }
                }
                else
                {
                    DLOG_IF(INFO,
                            config::kEnableExpandDebug &&
                                config::kEnableMemoryDebug && dctx != nullptr)
                        << "[race][trace] Subtable::put_slot: failed to insert "
                           "to slot "
                        << view << ": slot not empty. At slot_idx " << i << ". "
                        << *dctx;
                }
            }
        }
        return kNoMem;
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

    RetCode update_bucket_header_nodrain(uint32_t ld,
                                         uint32_t suffix,
                                         RaceHashingRdmaContext &rdma_ctx,
                                         HashContext *dctx)
    {
        for (size_t i = 0; i < kTotalBucketNr; ++i)
        {
            auto b = BucketHandle<kSlotNr>(
                st_addr_ + i * Bucket<kSlotNr>::size_bytes(), nullptr);
            auto rc = b.update_header_nodrain(ld, suffix, rdma_ctx, dctx);
            if (rc != kOk)
            {
                return rc;
            }
        }
        return kOk;
    }

    RetCode init_and_update_bucket_header_drain(
        uint32_t ld,
        uint32_t suffix,
        RaceHashingRdmaContext &rdma_ctx,
        HashContext *dctx)
    {
        DLOG_IF(INFO, config::kEnableExpandDebug && dctx != nullptr)
            << "[race][trace] init_and_update_bucket_header_nodrain: ld: " << ld
            << ", suffix: " << suffix;
        auto st_size = size_bytes();
        auto *rdma_buf = rdma_ctx.get_rdma_buffer(st_size);
        memset(rdma_buf, 0, st_size);
        for (size_t i = 0; i < kTotalBucketNr; ++i)
        {
            auto *bucket_buf_addr =
                (char *) rdma_buf + i * Bucket<kSlotNr>::size_bytes();
            auto bucket = Bucket<kSlotNr>(bucket_buf_addr);
            bucket.header().ld = ld;
            bucket.header().suffix = suffix;
        }

        CHECK_EQ(rdma_ctx.rdma_write(st_addr_, rdma_buf, st_size), kOk);
        CHECK_EQ(rdma_ctx.commit(), kOk);

        return kOk;
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