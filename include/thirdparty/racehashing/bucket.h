#pragma once
#ifndef PERTRONUS_RACEHASHING_BUCKET_H_
#define PERTRONUS_RACEHASHING_BUCKET_H_

#include <cstddef>
#include <cstdint>
#include <unordered_set>
#include <vector>

#include "./slot.h"
#include "Common.h"
#include "util/Rand.h"

namespace patronus::hash
{
struct BucketHeader
{
    uint32_t ld;
    uint32_t suffix;
} __attribute__((packed));
static_assert(sizeof(BucketHeader) == 8);

// the kSlotNr includes header.
// the actual number of slot_nr == kSlotNr - 1
template <size_t kSlotNr>
class Bucket
{
public:
    static_assert(kSlotNr > 1);
    constexpr static size_t kDataSlotNr = kSlotNr - 1;

    Bucket(void *bucket_buf) : bucket_buf_(bucket_buf)
    {
    }
    constexpr static size_t max_item_nr()
    {
        return kDataSlotNr;
    }

    Slot &slot(size_t idx) const
    {
        DCHECK_LT(idx, kSlotNr);
        auto *slot = (Slot *) bucket_buf_ + idx;
        return *slot;
    }
    SlotView slot_view(size_t idx) const
    {
        return SlotView(slot(idx).val());
    }

    BucketHeader &header()
    {
        return *(BucketHeader *) bucket_buf_;
    }
    const BucketHeader &header() const
    {
        return *(BucketHeader *) bucket_buf_;
    }

    const void *buffer_addr() const
    {
        return bucket_buf_;
    }
    constexpr static size_t size_bytes()
    {
        return Slot::size_bytes() * kSlotNr;
    }

    static constexpr size_t max_capacity()
    {
        return kDataSlotNr;
    }
    double utilization() const
    {
        size_t full_nr = 0;
        for (size_t i = 1; i < kSlotNr; ++i)
        {
            if (!slot_view(i).empty())
            {
                full_nr++;
            }
        }
        // DVLOG(1) << "[slot] utilization: full_nr: " << full_nr
        //          << ", total: " << kDataSlotNr
        //          << ", util: " << 1.0 * full_nr / kDataSlotNr;
        return 1.0 * full_nr / kDataSlotNr;
    }
    void setup_header(uint32_t ld, uint32_t suffix)
    {
        auto &h = header();
        h.ld = ld;
        h.suffix = suffix;
    }

private:
    constexpr static size_t kItemSize = Slot::size_bytes();
    void *bucket_buf_{nullptr};
};

template <size_t kSlotNr>
class BucketHandle
{
public:
    static_assert(kSlotNr > 1);
    constexpr static size_t kDataSlotNr = kSlotNr - 1;

    BucketHandle(uint64_t addr, char *bucket_buf)
        : addr_(addr), bucket_buf_(bucket_buf)
    {
    }
    constexpr static size_t max_item_nr()
    {
        return kDataSlotNr;
    }

    uint64_t slot_remote_addr(size_t idx) const
    {
        return addr_ + idx * sizeof(Slot);
    }
    SlotHandle slot_handle(size_t idx) const
    {
        // Slot *slot = (Slot *) bucket_buf_ + idx;
        // return SlotView(slot->val());
        return SlotHandle(slot_remote_addr(idx), slot_view(idx));
    }
    SlotView slot_view(size_t idx) const
    {
        auto *slot = (Slot *) ((char *) bucket_buf_ + idx * sizeof(Slot));
        return SlotView(slot->val());
    }
    // Slot &slot(size_t idx)
    // {
    //     DCHECK_LT(idx, kSlotNr);
    //     return *((Slot *) bucket_buf_ + idx);
    // }
    // const Slot &slot(size_t idx) const
    // {
    //     DCHECK_LT(idx, kSlotNr);
    //     return *((Slot *) bucket_buf_ + idx);
    // }
    BucketHeader &header()
    {
        return *(BucketHeader *) bucket_buf_;
    }
    const BucketHeader &header() const
    {
        return *(BucketHeader *) bucket_buf_;
    }
    uint64_t remote_addr() const
    {
        return addr_;
    }
    const void *bucket_buffer() const
    {
        return bucket_buf_;
    }
    constexpr static size_t size_bytes()
    {
        return Slot::size_bytes() * kSlotNr;
    }
    RetCode locate(uint8_t fp,
                   uint32_t ld,
                   uint32_t suffix,
                   std::unordered_set<SlotHandle> &ret,
                   HashContext *dctx) const
    {
        RetCode rc;
        if ((rc = validate_staleness(ld, suffix)) != kOk)
        {
            return rc;
        }

        for (size_t i = 0; i < kSlotNr; ++i)
        {
            auto view_handle = slot_handle(i);
            if (view_handle.match(fp))
            {
                DLOG_IF(INFO,
                        config::kEnableRaceHashingDebug && dctx != nullptr)
                    << "[race][trace] locate: fp " << pre_fp(fp)
                    << " got matched FP. view " << view_handle << ". " << *dctx;
                ret.insert(view_handle);
            }
        }
        return kOk;
    }

    static constexpr size_t max_capacity()
    {
        return kDataSlotNr;
    }

    RetCode validate_staleness(uint32_t expect_ld, uint32_t suffix) const
    {
        auto &h = header();
        if (expect_ld == h.ld)
        {
            auto rounded_suffix = round_hash_to_bit(suffix, expect_ld);
            auto rounded_header_suffix = round_hash_to_bit(h.suffix, expect_ld);

            if (rounded_suffix != rounded_header_suffix)
            {
                DVLOG(6) << "[bench][bucket] validate_staleness kStale (short"
                            "period of RC): match "
                            "ld but suffix mismatch.expect_ld: "
                         << expect_ld << ", expect_suffix: " << suffix << "("
                         << rounded_suffix << ")"
                         << ", header_ld: " << h.ld
                         << ", header_suffix: " << h.suffix << "("
                         << rounded_header_suffix << ")";
                return kCacheStale;
            }
            DVLOG(6) << "[bench][bucket] validate_staleness kOk: expect_ld: "
                     << expect_ld << ", expect_suffix: " << suffix << "("
                     << rounded_suffix << ")"
                     << ", header_ld: " << h.ld
                     << ", header_suffix: " << h.suffix << "("
                     << rounded_header_suffix << ")";
            return kOk;
        }
        auto rounded_bit = std::max(h.ld, expect_ld);
        auto rounded_suffix = round_hash_to_bit(suffix, rounded_bit);
        auto rounded_header_suffix = round_hash_to_bit(h.suffix, rounded_bit);
        if (rounded_suffix == rounded_header_suffix)
        {
            // stale but tolerant-able
            DVLOG(6) << "[bench][bucket] validate_staleness kOk (tolerable):"
                        "expect_ld: "
                     << expect_ld << ", expect_suffix: " << suffix
                     << ", rounded to: " << rounded_suffix
                     << ", header_ld: " << h.ld
                     << ", header_suffix: " << h.suffix
                     << ", rounded to: " << rounded_header_suffix
                     << ". Rounded  bit: " << rounded_bit;
            return kOk;
        }
        DVLOG(6) << "[bench][bucket] validate_staleness kCacheStale: "
                    "expect_ld: "
                 << expect_ld << ", expect_suffix: " << suffix
                 << ", rounded to: " << rounded_suffix
                 << ", header_ld: " << h.ld << ", header_suffix: " << h.suffix
                 << ", rounded to: " << rounded_header_suffix
                 << ". Rounded bit: " << rounded_bit;
        return kCacheStale;
    }

private:
    constexpr static size_t kItemSize = Slot::size_bytes();
    uint64_t addr_{0};
    char *bucket_buf_{nullptr};
};

}  // namespace patronus::hash

#endif