#pragma once
#ifndef PERTRONUS_RACEHASHING_BUCKET_H_
#define PERTRONUS_RACEHASHING_BUCKET_H_

#include <cstddef>
#include <cstdint>
#include <unordered_set>
#include <vector>

#include "./conf.h"
#include "./kv_block.h"
#include "./mock_rdma_adaptor.h"
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

    BucketHandle(GlobalAddress gaddr, char *bucket_buf)
        : gaddr_(gaddr), bucket_buf_(bucket_buf)
    {
    }
    constexpr static size_t max_item_nr()
    {
        return kDataSlotNr;
    }
    RetCode do_insert(SlotHandle slot_handle,
                      SlotView new_slot,
                      IRdmaAdaptor &rdma_ctx,
                      RemoteMemHandle &subtable_mem_handle,
                      SlotHandle *ret_slot,
                      HashContext *dctx)
    {
        uint64_t expect_val = slot_handle.val();
        auto rdma_buf = rdma_ctx.get_rdma_buffer(8);
        DCHECK_GE(rdma_buf.size, 8);
        CHECK_EQ(rdma_ctx.rdma_cas(slot_handle.remote_addr(),
                                   expect_val,
                                   new_slot.val(),
                                   rdma_buf.buffer,
                                   subtable_mem_handle),
                 kOk);
        CHECK_EQ(rdma_ctx.commit(), kOk);
        bool success = memcmp(rdma_buf.buffer, &expect_val, 8) == 0;

        SlotView expect_slot(expect_val);
        if (success)
        {
            CHECK(expect_slot.empty())
                << "[trace][handle] the succeess of insert should happen on an "
                   "empty slot";
            DLOG_IF(INFO, config::kEnableDebug && dctx != nullptr)
                << "[race][handle] slot " << slot_handle << " update to "
                << new_slot << *dctx;
            // set ret_slot here
            if (ret_slot)
            {
                *ret_slot = SlotHandle(slot_handle.remote_addr(),
                                       slot_handle.slot_view());
            }
            return kOk;
        }
        DVLOG(4) << "[race][handle] do_update FAILED: new_slot " << new_slot;
        DLOG_IF(INFO, config::kEnableDebug && dctx != nullptr)
            << "[race][trace][handle] do_update FAILED: cas failed. slot "
            << slot_handle << *dctx;
        return kRetry;
    }
    RetCode should_migrate(size_t bit,
                           IRdmaAdaptor &rdma_ctx,
                           RemoteMemHandle &kvblock_mem_handle,
                           std::unordered_set<SlotMigrateHandle> &ret,
                           HashContext *dctx)
    {
        for (size_t i = 1; i < kSlotNr; ++i)
        {
            auto h = slot_handle(i);
            if (h.empty())
            {
                continue;
            }
            auto kvblock_remote_addr = GlobalAddress(h.ptr());
            // we don't need the key and value content
            // just the buffered hash.
            auto kvblock_len = sizeof(KVBlock);
            auto rdma_buf = rdma_ctx.get_rdma_buffer(kvblock_len);
            DCHECK_GE(rdma_buf.size, kvblock_len);
            CHECK_EQ(rdma_ctx.rdma_read(DCHECK_NOTNULL(rdma_buf.buffer),
                                        kvblock_remote_addr,
                                        kvblock_len,
                                        kvblock_mem_handle),
                     kOk);
            CHECK_EQ(rdma_ctx.commit(), kOk);
            KVBlock &kv_block = *(KVBlock *) rdma_buf.buffer;
            auto hash = kv_block.hash;
            if (hash & (1 << bit))
            {
                DLOG_IF(INFO, config::kEnableExpandDebug && dctx != nullptr)
                    << "[race][trace] should_migrate: slot " << h
                    << " should migrate for tested bit " << bit;
                ret.insert(SlotMigrateHandle(h, hash));
            }
        }
        DLOG_IF(INFO, config::kEnableExpandDebug && dctx != nullptr)
            << "[race][trace] Bucket::should_migrate: collected migrate "
               "entries (accumulated): "
            << ret.size() << ". " << *dctx;
        return kOk;
    }
    RetCode update_header_nodrain(uint32_t ld,
                                  uint32_t suffix,
                                  IRdmaAdaptor &rdma_ctx,
                                  RemoteMemHandle &mem_handle,
                                  HashContext *dctx)
    {
        // header is at the first of the addr.
        std::ignore = dctx;
        auto rdma_buf = rdma_ctx.get_rdma_buffer(sizeof(BucketHeader));
        DCHECK_GE(rdma_buf.size, sizeof(BucketHeader));
        auto &header = *(BucketHeader *) rdma_buf.buffer;
        header.ld = ld;
        header.suffix = suffix;
        return rdma_ctx.rdma_write(
            gaddr_, rdma_buf.buffer, sizeof(BucketHeader), mem_handle);
    }

    GlobalAddress slot_remote_addr(size_t idx) const
    {
        return gaddr_ + idx * sizeof(Slot);
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

    BucketHeader &header()
    {
        return *(BucketHeader *) bucket_buf_;
    }
    const BucketHeader &header() const
    {
        return *(BucketHeader *) bucket_buf_;
    }
    GlobalAddress remote_addr() const
    {
        return gaddr_;
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
        if ((rc = validate_staleness(ld, suffix, dctx)) != kOk)
        {
            return rc;
        }

        for (size_t i = 1; i < kSlotNr; ++i)
        {
            auto view_handle = slot_handle(i);
            if (view_handle.match(fp))
            {
                DLOG_IF(INFO, config::kEnableLocateDebug && dctx != nullptr)
                    << "[race][trace] locate: fp " << pre_fp(fp)
                    << " got matched FP " << pre_fp(fp) << ". view "
                    << view_handle << ". " << *dctx;
                ret.insert(view_handle);
            }
        }
        return kOk;
    }

    static constexpr size_t max_capacity()
    {
        return kDataSlotNr;
    }

    RetCode validate_staleness(uint32_t expect_ld,
                               uint32_t suffix,
                               HashContext *dctx) const
    {
        auto &h = header();
        if (expect_ld == h.ld)
        {
            auto rounded_suffix = round_hash_to_bit(suffix, expect_ld);
            auto rounded_header_suffix = round_hash_to_bit(h.suffix, expect_ld);

            if (rounded_suffix != rounded_header_suffix)
            {
                DLOG_IF(INFO, config::kEnableDebug && dctx != nullptr)
                    << "[race][trace] validate_staleness kStale: (short"
                       "period of RC): match "
                       "ld but suffix mismatch.expect_ld: "
                    << "expect (" << expect_ld << ", " << suffix
                    << ") suffix rounded to " << rounded_suffix << ". Got ("
                    << h.ld << ", " << h.suffix << ") suffix rounded to "
                    << rounded_header_suffix << ". " << *dctx;
                return kCacheStale;
            }
            // DLOG_IF(INFO, config::kEnableDebug && dctx != nullptr)
            //     << "[race][trace] validate_staleness kOk: expect_ld: "
            //     << expect_ld << ", expect_suffix: " << suffix << "("
            //     << rounded_suffix << ")"
            //     << ", header_ld: " << h.ld << ", header_suffix: " << h.suffix
            //     << "(" << rounded_header_suffix << "). " << *dctx;
            return kOk;
        }
        auto rounded_bit = std::max(h.ld, expect_ld);
        auto rounded_suffix = round_hash_to_bit(suffix, rounded_bit);
        auto rounded_header_suffix = round_hash_to_bit(h.suffix, rounded_bit);
        if (rounded_suffix == rounded_header_suffix)
        {
            // stale but tolerant-able
            DLOG_IF(INFO, config::kEnableDebug && dctx != nullptr)
                << "[race][trace] validate_staleness kOk (tolerable):"
                   "expect_ld: "
                << expect_ld << ", expect_suffix: " << suffix
                << ", rounded to: " << rounded_suffix << ", header_ld: " << h.ld
                << ", header_suffix: " << h.suffix
                << ", rounded to: " << rounded_header_suffix
                << ". Rounded  bit: " << rounded_bit;
            return kOk;
        }
        DLOG_IF(INFO, config::kEnableDebug && dctx != nullptr)
            << "[race][trace] validate_staleness kCacheStale: "
               "expect_ld: "
            << expect_ld << ", expect_suffix: " << suffix
            << ", rounded to: " << rounded_suffix << ", header_ld: " << h.ld
            << ", header_suffix: " << h.suffix
            << ", rounded to: " << rounded_header_suffix
            << ". Rounded bit: " << rounded_bit;
        return kCacheStale;
    }

private:
    constexpr static size_t kItemSize = Slot::size_bytes();
    GlobalAddress gaddr_{0};
    char *bucket_buf_{nullptr};
};

}  // namespace patronus::hash

#endif