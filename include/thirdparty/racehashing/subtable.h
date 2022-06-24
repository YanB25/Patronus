#pragma once
#ifndef PERTRONUS_RACEHASHING_SUBTABLE_H_
#define PERTRONUS_RACEHASHING_SUBTABLE_H_

#include <cstddef>
#include <cstdint>

#include "./bucket_group.h"
#include "./kv_block.h"
#include "util/PerformanceReporter.h"
#include "util/Util.h"

namespace patronus::hash
{
template <size_t kBucketGroupNr, size_t kSlotNr>
class SubTableHandle;
// NOTE:
// one bucket group has two bucket and one overflow bucket
// when using hash value to indexing the bucket
// the bucket_nr == 2 * bucket_group_nr
template <size_t kBucketGroupNr, size_t kSlotNr>
class SubTable
{
public:
    using Handle = SubTableHandle<kBucketGroupNr, kSlotNr>;
    using BucketT = Bucket<kSlotNr>;

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
    constexpr static size_t bucket_nr()
    {
        return kTotalBucketNr;
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

    SubTableHandle(GlobalAddress st_gaddr, RemoteMemHandle &st_mem_handle)
        : st_gaddr_(st_gaddr), st_mem_handle_(st_mem_handle)
    {
        CHECK(!st_gaddr_.is_null());
    }
    RetCode try_del_slot(SlotHandle slot_handle, IRdmaAdaptor &rdma_adpt)
    {
        auto remote = slot_handle.remote_addr();
        uint64_t expect_val = slot_handle.slot_view().val();
        auto clear_view = slot_handle.view_after_clear();
        auto rdma_buf = rdma_adpt.get_rdma_buffer(8);
        DCHECK_GE(rdma_buf.size, 8);
        rdma_adpt
            .rdma_cas(remote,
                      expect_val,
                      clear_view.val(),
                      rdma_buf.buffer,
                      0 /* flag */,
                      st_mem_handle_)
            .expect(RC::kOk);
        rdma_adpt.commit().expect(RC::kOk);
        uint64_t read = *(uint64_t *) rdma_buf.buffer;
        bool success = read == expect_val;
        if (success)
        {
            return kOk;
        }
        return kRetry;
    }
    RetCode del_slot(SlotHandle slot_handle, IRdmaAdaptor &rdma_adpt)
    {
        CHECK_EQ(try_del_slot(slot_handle, rdma_adpt), kOk);
        return kOk;
    }
    RetCode put_slot(SlotMigrateHandle slot_handle,
                     IRdmaAdaptor &rdma_adpt,
                     SlotHandle *ret,
                     HashContext *dctx)
    {
        auto hash = slot_handle.hash();
        auto [h1, h2] = hash_h1_h2(hash);

        auto cbs = get_two_combined_bucket_handle(h1, h2, rdma_adpt);
        CHECK_EQ(rdma_adpt.commit(), kOk);

        std::vector<BucketHandle<kSlotNr>> buckets;
        buckets.reserve(4);
        CHECK_EQ(cbs.get_bucket_handle(buckets), kOk);
        CHECK_EQ(buckets.size(), 4);
        if constexpr (debug())
        {
            std::vector<Buffer> addrs;
            for (const auto &bucket : buckets)
            {
                char *addr = (char *) bucket.remote_addr().val;
                size_t size = bucket.size_bytes();
                addrs.push_back(Buffer(addr, size));
            }
            validate_buffer_not_overlapped(addrs);
        }
        for (auto &bucket : buckets)
        {
            auto poll_slot_idx = fast_pseudo_rand_int(1, kSlotNr - 1);
            constexpr auto kDataSlotNr = Bucket<kSlotNr>::kDataSlotNr;
            for (size_t i = 0; i < kDataSlotNr; ++i)
            {
                DLOG_IF(INFO, config::kEnableMemoryDebug && dctx != nullptr)
                    << "[race][trace] Subtable::put_slot: trying bucket "
                    << bucket.remote_addr();
                auto idx = (poll_slot_idx + i) % kDataSlotNr + 1;
                DCHECK_GE(idx, 1);
                DCHECK_LT(idx, kSlotNr);
                auto view = bucket.slot_view(idx);
                if (view.empty())
                {
                    auto rc = bucket.do_insert(bucket.slot_handle(idx),
                                               slot_handle.slot_view(),
                                               rdma_adpt,
                                               st_mem_handle_,
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
                        << bucket.slot_handle(idx)
                        << ": slot not empty. At slot_idx " << idx << ". "
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
        uint64_t h1, uint64_t h2, IRdmaAdaptor &rdma_adpt)
    {
        auto cb1_idx = to_combined_bucket_idx(h1);
        auto cb2_idx = to_combined_bucket_idx(h2);
        if (unlikely(cb1_idx == cb2_idx))
        {
            // don't want to colocate to the same combined grous
            // otherwise, resizing will fail to insert by no enough slots
            return get_two_combined_bucket_handle(h1, h2 + 1, rdma_adpt);
        }
        auto cb1 = combined_bucket_handle(h1);
        auto cb2 = combined_bucket_handle(h2);
        return TwoCombinedBucketHandle<kSlotNr>(
            h1, h2, std::move(cb1), std::move(cb2), rdma_adpt, st_mem_handle_);
    }
    static uint64_t to_combined_bucket_idx(uint64_t i)
    {
        i = i % kCombinedBucketNr;
        if (i % 2 == 0)
        {
            return i / 2;
        }
        else
        {
            return (i - 1) / 2;
        }
    }
    constexpr static size_t max_item_nr()
    {
        return kBucketGroupNr * BucketGroup<kSlotNr>::max_item_nr();
    }

    GlobalAddress gaddr() const
    {
        return st_gaddr_;
    }

    static constexpr size_t size_bytes()
    {
        return BucketGroup<kSlotNr>::size_bytes() * kBucketGroupNr;
    }

    static constexpr size_t max_capacity()
    {
        return SubTable<kBucketGroupNr, kSlotNr>::max_capacity();
    }
    RemoteMemHandle &mem_handle()
    {
        return st_mem_handle_;
    }
    const RemoteMemHandle &mem_handle() const
    {
        return st_mem_handle_;
    }

    RetCode update_bucket_header_nodrain(uint32_t ld,
                                         uint32_t suffix,
                                         IRdmaAdaptor &rdma_adpt,
                                         HashContext *dctx)
    {
        for (size_t i = 0; i < kTotalBucketNr; ++i)
        {
            auto bucket_gaddr =
                GlobalAddress(st_gaddr_ + i * Bucket<kSlotNr>::size_bytes());
            auto b = BucketHandle<kSlotNr>(bucket_gaddr, nullptr);
            auto rc = b.update_header_nodrain(
                ld, suffix, rdma_adpt, st_mem_handle_, dctx);
            if (rc != kOk)
            {
                return rc;
            }
        }
        return kOk;
    }

    RetCode init_and_update_bucket_header_drain(uint32_t ld,
                                                uint32_t suffix,
                                                IRdmaAdaptor &rdma_adpt,
                                                HashContext *dctx)
    {
        DLOG_IF(INFO, config::kEnableExpandDebug && dctx != nullptr)
            << "[race][trace] init_and_update_bucket_header_nodrain: ld: " << ld
            << ", suffix: " << suffix;
        auto st_size = size_bytes();
        auto rdma_buf = rdma_adpt.get_rdma_buffer(st_size);
        DCHECK_GE(rdma_buf.size, st_size);
        memset(rdma_buf.buffer, 0, st_size);
        for (size_t i = 0; i < kTotalBucketNr; ++i)
        {
            auto *bucket_buf_addr =
                (char *) rdma_buf.buffer + i * Bucket<kSlotNr>::size_bytes();
            auto bucket = Bucket<kSlotNr>(bucket_buf_addr);
            bucket.header().ld = ld;
            bucket.header().suffix = suffix;
        }

        rdma_adpt
            .rdma_write(st_gaddr_,
                        rdma_buf.buffer,
                        st_size,
                        0 /* flag */,
                        st_mem_handle_)
            .expect(RC::kOk);
        rdma_adpt.commit().expect(RC::kOk);

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
            GlobalAddress bucket_group_gaddr = st_gaddr_ + kItemSize * idx;
            DLOG_IF(INFO, config::kEnableMemoryDebug)
                << "[race][mem] combined_bucket_handle: left. indexing "
                << origin_idx << ", map to idx " << idx
                << ", from kCombinedBucketNr: " << kCombinedBucketNr
                << ", st_addr: " << st_gaddr_ << ", kItemSize: " << kItemSize
                << ", got " << bucket_group_gaddr;
            return CombinedBucketHandle<kSlotNr>(bucket_group_gaddr, true);
        }
        else
        {
            // odd
            idx = (idx - 1) / 2;
            GlobalAddress bucket_group_gaddr = st_gaddr_ + kItemSize * idx;
            GlobalAddress overflow_bucket_gaddr =
                bucket_group_gaddr + Bucket<kSlotNr>::size_bytes();
            LOG_IF(INFO, config::kEnableMemoryDebug)
                << "[race][mem] combined_bucket_handle: right. indexing "
                << origin_idx << ", map to idx " << idx
                << " from kCombinedBucketNr: " << kCombinedBucketNr
                << ", st_addr: " << st_gaddr_ << ", kItemSize: " << kItemSize
                << ", got " << overflow_bucket_gaddr;
            return CombinedBucketHandle<kSlotNr>(overflow_bucket_gaddr, false);
        }
    }

private:
    constexpr static size_t kItemSize =
        BucketGroupHandle<kSlotNr>::size_bytes();
    GlobalAddress st_gaddr_;
    RemoteMemHandle &st_mem_handle_;
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