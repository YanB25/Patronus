#pragma once
#ifndef PERTRONUS_RACEHASHING_BUCKETGROUP_H_
#define PERTRONUS_RACEHASHING_BUCKETGROUP_H_

#include <cstddef>
#include <cstdint>

#include "./bucket.h"
#include "./rdma.h"

namespace patronus::hash
{
template <size_t kSlotNr>
class CombinedBucket
{
public:
    CombinedBucket(void *addr, bool main_on_left)
        : addr_(addr), main_on_left_(main_on_left)
    {
    }

    void *buffer_addr() const
    {
        return addr_;
    }

    constexpr static size_t size_bytes()
    {
        return bucket_size_bytes() * 2;
    }
    constexpr static size_t bucket_size_bytes()
    {
        return Bucket<kSlotNr>::size_bytes();
    }

    Bucket<kSlotNr> main_bucket()
    {
        if (main_on_left_)
        {
            return Bucket<kSlotNr>(addr_);
        }
        return Bucket<kSlotNr>((char *) addr_ + bucket_size_bytes());
    }

    Bucket<kSlotNr> overflow_bucket()
    {
        if (main_on_left_)
        {
            return Bucket<kSlotNr>((char *) addr_ + bucket_size_bytes());
        }
        return Bucket<kSlotNr>(addr_);
    }

private:
    void *addr_{0};
    bool main_on_left_;
};

template <size_t kSlotNr>
class CombinedBucketHandle
{
public:
    CombinedBucketHandle(uint64_t addr, bool main_on_left)
        : addr_(addr), main_on_left_(main_on_left)
    {
    }
    CombinedBucketHandle(const CombinedBucketHandle &rhs) = delete;
    CombinedBucketHandle &operator=(const CombinedBucketHandle &) = delete;

    CombinedBucketHandle(CombinedBucketHandle &&) = default;
    CombinedBucketHandle &operator=(CombinedBucketHandle &&) = default;

    RetCode read(RaceHashingRdmaContext &rdma_ctx)
    {
        buffer_ = CHECK_NOTNULL(rdma_ctx.get_rdma_buffer(size_bytes()));
        return rdma_ctx.rdma_read(addr_, (char *) buffer_, size_bytes());
    }

    RetCode locate(uint8_t fp,
                   uint32_t ld,
                   uint32_t suffix,
                   std::unordered_set<SlotHandle> &ret,
                   HashContext *dctx)
    {
        auto mbh = main_bucket_handle();
        auto rc = mbh.locate(fp, ld, suffix, ret, dctx);
        if (rc != kOk)
        {
            return rc;
        }
        auto obh = overflow_bucket_handle();
        return obh.locate(fp, ld, suffix, ret, dctx);
    }

    uint64_t remote_addr() const
    {
        return addr_;
    }
    void *buffer_addr() const
    {
        return buffer_;
    }

    constexpr static size_t size_bytes()
    {
        return bucket_size_bytes() * 2;
    }
    constexpr static size_t bucket_size_bytes()
    {
        return Bucket<kSlotNr>::size_bytes();
    }

    BucketHandle<kSlotNr> main_bucket_handle() const
    {
        if (main_on_left_)
        {
            return BucketHandle<kSlotNr>(addr_,
                                         (char *) DCHECK_NOTNULL(buffer_));
        }
        return BucketHandle<kSlotNr>(
            addr_ + bucket_size_bytes(),
            (char *) DCHECK_NOTNULL(buffer_) + bucket_size_bytes());
    }

    BucketHandle<kSlotNr> overflow_bucket_handle() const
    {
        if (main_on_left_)
        {
            return BucketHandle<kSlotNr>(
                addr_ + bucket_size_bytes(),
                (char *) DCHECK_NOTNULL(buffer_) + bucket_size_bytes());
        }
        return BucketHandle<kSlotNr>(addr_, (char *) DCHECK_NOTNULL(buffer_));
    }

private:
    uint64_t addr_{0};
    void *buffer_{nullptr};
    bool main_on_left_;
};

template <size_t kSlotNr>
class TwoCombinedBucketHandle
{
public:
    TwoCombinedBucketHandle(uint32_t h1,
                            uint32_t h2,
                            CombinedBucketHandle<kSlotNr> &&cb1,
                            CombinedBucketHandle<kSlotNr> &&cb2,
                            RaceHashingRdmaContext &rdma_ctx)
        : h1_(h1), h2_(h2), cb1_(std::move(cb1)), cb2_(std::move(cb2))
    {
        CHECK_EQ(cb1_.read(rdma_ctx), kOk);
        CHECK_EQ(cb2_.read(rdma_ctx), kOk);
    }
    TwoCombinedBucketHandle(const TwoCombinedBucketHandle &) = delete;
    TwoCombinedBucketHandle &operator=(const TwoCombinedBucketHandle &) =
        delete;

    RetCode locate(uint8_t fp,
                   uint32_t ld,
                   uint32_t suffix,
                   std::unordered_set<SlotHandle> &ret,
                   HashContext *dctx)
    {
        auto rc = cb1_.locate(fp, ld, suffix, ret, dctx);
        if (rc != kOk)
        {
            return rc;
        }
        return cb2_.locate(fp, ld, suffix, ret, dctx);
    }

    RetCode get_bucket_handle(std::vector<BucketHandle<kSlotNr>> &buckets) const
    {
        buckets.push_back(cb1_.main_bucket_handle());
        buckets.push_back(cb2_.main_bucket_handle());
        buckets.push_back(cb1_.overflow_bucket_handle());
        buckets.push_back(cb2_.overflow_bucket_handle());
        return kOk;
    }

private:
    uint32_t h1_;
    uint32_t h2_;
    CombinedBucketHandle<kSlotNr> cb1_;
    CombinedBucketHandle<kSlotNr> cb2_;
};

// a bucket group is {MainBucket_1, OverflowBucket, MainBucket_2}
template <size_t kSlotNr>
class BucketGroup
{
public:
    BucketGroup(char *buffer_addr) : buffer_addr_(buffer_addr)
    {
    }
    Bucket<kSlotNr> bucket(size_t idx) const
    {
        DCHECK_GE(idx, 0);
        DCHECK_LT(idx, 3);
        constexpr size_t bucket_size_bytes = Bucket<kSlotNr>::size_bytes();
        size_t offset = idx * bucket_size_bytes;
        return Bucket<kSlotNr>(buffer_addr_ + offset);
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
    void setup_header(uint32_t ld, uint32_t hash_suffix)
    {
        main_bucket_0().setup_header(ld, hash_suffix);
        main_bucket_1().setup_header(ld, hash_suffix);
        overflow_bucket().setup_header(ld, hash_suffix);
    }

    constexpr static size_t max_item_nr()
    {
        return Bucket<kSlotNr>::max_item_nr() * 3;
    }
    void *buffer_addr() const
    {
        return buffer_addr_;
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

private:
    constexpr static size_t kItemSize = Bucket<kSlotNr>::size_bytes();
    char *buffer_addr_{nullptr};
};

// a bucket group is {MainBucket_1, OverflowBucket, MainBucket_2}
template <size_t kSlotNr>
class BucketGroupHandle
{
public:
    BucketGroupHandle(uint64_t addr) : addr_(addr)
    {
    }
    BucketHandle<kSlotNr> bucket(size_t idx) const
    {
        DCHECK_GE(idx, 0);
        DCHECK_LT(idx, 3);
        constexpr size_t bucket_size_bytes = Bucket<kSlotNr>::size_bytes();
        size_t offset = idx * bucket_size_bytes;
        return Bucket<kSlotNr>(addr_ + offset, nullptr);
    }
    BucketHandle<kSlotNr> main_bucket_0() const
    {
        return bucket(0);
    }
    BucketHandle<kSlotNr> main_bucket_1() const
    {
        return bucket(2);
    }
    BucketHandle<kSlotNr> overflow_bucket() const
    {
        return bucket(1);
    }
    void setup_header(uint32_t ld, uint32_t hash_suffix)
    {
        main_bucket_0().setup_header(ld, hash_suffix);
        main_bucket_1().setup_header(ld, hash_suffix);
        overflow_bucket().setup_header(ld, hash_suffix);
    }
    CombinedBucketHandle<kSlotNr> combined_bucket_0()
    {
        return CombinedBucketHandle<kSlotNr>((char *) addr_,
                                             (char *) addr_ + 1 * kItemSize);
    }
    CombinedBucketHandle<kSlotNr> &combined_bucket_1()
    {
        return CombinedBucketHandle<kSlotNr>((char *) addr_ + 2 * kItemSize,
                                             (char *) addr_ + 1 * kItemSize);
    }
    constexpr static size_t max_item_nr()
    {
        return Bucket<kSlotNr>::max_item_nr() * 3;
    }

    uint64_t remote_addr() const
    {
        return addr_;
    }

    constexpr static size_t size_bytes()
    {
        return Bucket<kSlotNr>::size_bytes() * 3;
    }

private:
    constexpr static size_t kItemSize = Bucket<kSlotNr>::size_bytes();
    uint64_t addr_{0};
};
}  // namespace patronus::hash
#endif