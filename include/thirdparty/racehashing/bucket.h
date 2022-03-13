#pragma once
#ifndef PERTRONUS_RACEHASHING_BUCKET_H_
#define PERTRONUS_RACEHASHING_BUCKET_H_

#include <cstddef>
#include <cstdint>
#include <vector>

#include "./slot.h"
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

    Bucket(void *addr) : addr_((Slot *) addr)
    {
    }
    Slot &slot(size_t idx)
    {
        DCHECK_LT(idx, kSlotNr);
        return *(addr_ + idx);
    }
    const Slot &slot(size_t idx) const
    {
        DCHECK_LT(idx, kSlotNr);
        return *(addr_ + idx);
    }
    BucketHeader &header()
    {
        return *(BucketHeader *) &slot(0);
    }
    const BucketHeader &header() const
    {
        return *(BucketHeader *) &slot(0);
    }
    void *addr() const
    {
        return addr_;
    }
    constexpr static size_t size_bytes()
    {
        return Slot::size_bytes() * kSlotNr;
    }
    std::vector<SlotWithView> locate(uint8_t fp)
    {
        std::vector<SlotWithView> ret;
        for (size_t i = 1; i < kSlotNr; ++i)
        {
            auto view = slot(i).with_view();
            if (view.match(fp))
            {
                ret.push_back(view);
            }
        }
        return ret;
    }

    std::vector<SlotWithView> fetch_empty(size_t limit) const
    {
        std::vector<SlotWithView> ret;
        auto poll_slot_idx = fast_pseudo_rand_int(1, kSlotNr - 1);
        for (size_t i = 0; i < kDataSlotNr; ++i)
        {
            auto idx = (poll_slot_idx + i) % kDataSlotNr + 1;
            CHECK_GE(idx, 1);
            CHECK_LT(idx, kSlotNr);
            auto view = slot(idx).with_view();

            if (view.empty())
            {
                ret.push_back(view);
                if (ret.size() >= limit)
                {
                    return ret;
                }
            }
        }
        return ret;
    }

    std::pair<std::vector<SlotWithView>, SlotWithView> locate_or_empty(
        uint8_t fp)
    {
        std::vector<SlotWithView> ret;
        auto poll_slot_idx = fast_pseudo_rand_int(1, kSlotNr - 1);
        SlotWithView first_empty_slot_view;
        for (size_t i = 0; i < kDataSlotNr; ++i)
        {
            auto idx = (poll_slot_idx + i) % kDataSlotNr + 1;
            CHECK_GE(idx, 1);
            CHECK_LT(idx, kSlotNr);
            auto view = slot(idx).view();

            if (view.match(fp))
            {
                DVLOG(6) << "[bench][bucket] Search for fp " << pre_fp(fp)
                         << " got possible match slot at " << idx;
                ret.push_back(view);
            }

            if (view.empty() && first_empty_slot_view.slot() == nullptr)
            {
                DVLOG(6) << "[bench][bucket] Search for fp " << pre_fp(fp)
                         << " got first empty slot at " << idx;
                first_empty_slot_view = view;
            }
        }

        return {ret, first_empty_slot_view};
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
            if (!slot(i).view().empty())
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
    RetCode validate_staleness(uint32_t expect_ld, uint32_t suffix)
    {
        auto &h = header();
        if (expect_ld == h.ld)
        {
            auto rounded_suffix = round_hash_to_bit(suffix, expect_ld);
            auto rounded_header_suffix = round_hash_to_bit(h.suffix, expect_ld);
            CHECK_EQ(rounded_suffix, rounded_header_suffix)
                << "Inconsistency detected";
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
            DVLOG(6) << "[bench][bucket] validate_staleness kOk (tolerable): "
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
    Slot *addr_{nullptr};
};

}  // namespace patronus::hash

#endif