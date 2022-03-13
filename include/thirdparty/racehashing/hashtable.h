#pragma once
#ifndef PERTRONUS_RACEHASHING_HASHTABLE_H_
#define PERTRONUS_RACEHASHING_HASHTABLE_H_

#include <string>

#include "./utils.h"
#include "patronus/memory/allocator.h"
#include "util/PerformanceReporter.h"
#include "util/Rand.h"

namespace patronus::hash
{
struct KVBlock
{
    // TODO: add checksum to KVBlock
    // Paper sec 3.3, there will be a corner case where one client is reading
    // @key and @value from the KVBlock. Meanwhile, the KVBlock is freed,
    // re-allocated, and be filled with the same @key but different @value.
    // TO detect this inconsistency, add checksum to the KVBlock.
    uint32_t key_len;
    uint32_t value_len;
    char buf[0];
    static KVBlock *new_instance(const Key &key, const Value &value)
    {
        auto expect_size =
            sizeof(key_len) + sizeof(value_len) + key.size() + value.size();
        auto &ret = *(KVBlock *) malloc(expect_size);
        ret.key_len = key.size();
        ret.value_len = value.size();
        memcpy(ret.buf, key.data(), key.size());
        memcpy(ret.buf + key.size(), value.data(), value.size());
        return &ret;
    }
} __attribute__((packed));

class SlotView;
class SlotWithView;
class Slot
{
public:
    explicit Slot(uint8_t fp, uint8_t len, void *ptr);

    constexpr static size_t size_bytes()
    {
        return 8;
    }

    friend std::ostream &operator<<(std::ostream &, const Slot &);
    friend class SlotWithView;

    SlotView view() const;
    SlotWithView with_view() const;
    void *addr() const;

private:
    uint8_t fp() const;
    void set_fp(uint8_t fp);
    uint8_t len() const;
    void set_len(uint8_t len);
    void *ptr() const;
    void set_ptr(void *_ptr);
    uint64_t val() const;
    bool empty() const;
    bool cas(SlotView &expected, const SlotView &desired);

    bool match(uint8_t _fp) const;
    void clear();
    Slot(uint64_t val);

    // TODO: actually it is TaggedPtrImpl<KVBlock>
    TaggedPtr ptr_;
} __attribute__((packed));
static_assert(sizeof(Slot) == sizeof(TaggedPtr));
static_assert(sizeof(Slot) == 8);

inline std::ostream &operator<<(std::ostream &os, const Slot &slot)
{
    os << "{Slot: fp: " << pre_fp(slot.fp()) << ", len: " << (int) slot.len()
       << ", ptr: " << slot.ptr() << "}";
    return os;
}

/**
 * @brief A read-only view of the slot
 */
class SlotView
{
public:
    explicit SlotView(uint64_t val)
    {
        ptr_.set_val(val);
    }
    explicit SlotView(TaggedPtr ptr)
    {
        ptr_ = ptr;
    }
    explicit SlotView(uint8_t fp, uint8_t len, void *ptr)
    {
        ptr_.set_u8_h(fp);
        ptr_.set_u8_l(len);
        ptr_.set_ptr(ptr);
    }
    uint8_t fp() const;
    uint8_t len() const;
    void *ptr() const;
    uint64_t val() const;
    bool empty() const;

    bool match(uint8_t _fp) const;
    SlotView view_after_clear() const;

    constexpr static size_t size_bytes()
    {
        return 8;
    }

    friend std::ostream &operator<<(std::ostream &, const SlotView &);
    friend class Slot;

private:
    TaggedPtr ptr_;
} __attribute__((packed));
static_assert(sizeof(SlotView) == sizeof(TaggedPtr));
static_assert(sizeof(SlotView) == 8);

inline std::ostream &operator<<(std::ostream &os, const SlotView &slot_view)
{
    os << "{SlotView: " << std::hex << slot_view.ptr_ << "}";
    return os;
}

/**
 * @brief SlowView is a *snapshot* of a Slot, including its address and the
 * value when read.
 *
 */
class SlotWithView
{
public:
    explicit SlotWithView(Slot *slot, SlotView slot_view);
    explicit SlotWithView();
    Slot *slot() const;
    SlotView view() const;
    SlotView view_after_clear() const;
    bool operator<(const SlotWithView &rhs) const;

    // all the query goes to @view_
    uint8_t fp() const;
    uint8_t len() const;
    void *ptr() const;
    uint64_t val() const;
    bool empty() const;
    bool match(uint8_t fp) const;

    // all the modify goes to @slot_
    bool cas(SlotView &expected, const SlotView &desired);
    void set_fp(uint8_t fp);
    void set_len(uint8_t len);
    void set_ptr(void *_ptr);
    void clear();

    friend std::ostream &operator<<(std::ostream &os, const SlotWithView &view);

private:
    Slot *slot_;
    SlotView slot_view_;
};
inline std::ostream &operator<<(std::ostream &os, const SlotWithView &view)
{
    os << "{SlotWithView: " << view.slot_view_ << " at " << (void *) view.slot_
       << "}";
    return os;
}

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
    std::vector<SlotWithView> locate(uint8_t fp)
    {
        auto slots_main = main_.locate(fp);
        auto slots_overflow = overflow_.locate(fp);
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
        if constexpr (debug())
        {
            auto ec2 = overflow_.validate_staleness(expect_ld, suffix);
            CHECK_EQ(ec, ec2);
        }
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

    SubTable(size_t ld, void *addr, size_t size, uint64_t hash_suffix)
        : addr_((BucketGroup<kSlotNr> *) CHECK_NOTNULL(addr))
    {
        DVLOG(1) << "Subtable ctor called. addr_: " << addr_
                 << ", addr: " << addr << ", memset bytes: " << size_bytes();

        CHECK_GE(size, size_bytes());
        // NOTE:
        // the memset is required to fill the slot as empty
        memset(addr_, 0, size_bytes());

        // init each bucket
        for (size_t b = 0; b < kBucketGroupNr; ++b)
        {
            auto bg = bucket_group(b);
            bg.setup_header(ld, hash_suffix);
        }
    }

    BucketGroup<kSlotNr> bucket_group(size_t idx) const
    {
        DCHECK_LT(idx, kBucketGroupNr);
        return BucketGroup<kSlotNr>((char *) addr_ + idx * kItemSize);
    }
    CombinedBucketView<kSlotNr> combined_bucket(size_t idx)
    {
        idx = idx % kCombinedBucketNr;
        if (idx % 2 == 0)
        {
            // even
            idx /= 2;
            void *bucket_group_addr = (char *) addr_ + kItemSize * idx;
            void *main_bucket_addr = bucket_group_addr;
            void *overflow_bucket_addr =
                (char *) main_bucket_addr + Bucket<kSlotNr>::size_bytes();
            return CombinedBucketView<kSlotNr>(main_bucket_addr,
                                               overflow_bucket_addr);
        }
        else
        {
            // odd
            idx = (idx - 1) / 2;
            void *bucket_group_addr = (char *) addr_ + kItemSize * idx;
            void *main_bucket_addr =
                (char *) bucket_group_addr + Bucket<kSlotNr>::size_bytes() * 2;
            void *overflow_bucket_addr =
                (char *) bucket_group_addr + Bucket<kSlotNr>::size_bytes();
            return CombinedBucketView<kSlotNr>(main_bucket_addr,
                                               overflow_bucket_addr);
        }
    }

    void *addr() const
    {
        return addr_;
    }

    static constexpr size_t size_bytes()
    {
        return BucketGroup<kSlotNr>::size_bytes() * kBucketGroupNr;
    }
    RetCode get(const Key &key,
                Value &value,
                uint64_t hash,
                uint32_t expect_ld,
                HashContext *dctx)
    {
        // TODO: not enable dctx for get
        std::ignore = dctx;
        auto h1 = hash_1(hash);
        auto h2 = hash_2(hash);
        auto fp = hash_fp(hash);
        auto m = hash_m(hash);
        DVLOG(4) << "[race][subtable] GET hash: " << std::hex << hash
                 << " got h1: " << h1 << ", h2: " << h2
                 << ", fp: " << pre_fp(fp);

        // RDMA: read the whole combined_bucket
        // Batch together.
        // TODO(RDMA): should check header here
        auto cb1 = combined_bucket(h1);
        auto cb2 = combined_bucket(h2);
        auto ec = cb1.validate_staleness(expect_ld, m);
        if (ec == kCacheStale)
        {
            return kCacheStale;
        }
        DCHECK_EQ(ec, kOk);
        // RDMA: do the local search for any slot *may* match the key
        auto slot_views_1 = cb1.locate(fp);
        for (auto view : slot_views_1)
        {
            // RDMA: read here
            auto *kv_block = (KVBlock *) DCHECK_NOTNULL(view.ptr());
            auto rc = do_get_if_real_match(kv_block, key, value);
            if (rc == kOk)
            {
                return kOk;
            }
            DCHECK_EQ(rc, kNotFound);
        }
        ec = cb2.validate_staleness(expect_ld, m);
        if (ec == kCacheStale)
        {
            return kCacheStale;
        }
        DCHECK_EQ(ec, kOk);
        auto slot_views_2 = cb2.locate(fp);
        for (auto view : slot_views_2)
        {
            // RDMA: read here
            auto *kv_block = (KVBlock *) DCHECK_NOTNULL(view.ptr());

            auto rc = do_get_if_real_match(kv_block, key, value);
            if (rc == kOk)
            {
                return kOk;
            }
            DCHECK_EQ(rc, kNotFound);
        }
        return kNotFound;
    }

    RetCode put(const Key &key,
                const Value &value,
                uint64_t hash,
                uint32_t expect_ld,
                HashContext *dctx)
    {
        if constexpr (debug())
        {
            auto h1 = hash_1(hash);
            auto h2 = hash_2(hash);
            auto fp = hash_fp(hash);
            DVLOG(4) << "[race][subtable] PUT hash: " << std::hex << hash
                     << " got h1: " << h1 << ", h2: " << h2
                     << ", fp: " << pre_fp(fp);
        }

        auto rc = do_put_phase_one(key, value, hash, expect_ld, dctx);
        if (rc != kOk)
        {
            return rc;
        }
        // TODO: if do_put_phase_two_reread return kCacheStale
        // should update cache and re-execute do_put_phase_two_reread.
        return do_put_phase_two_reread(key, hash, expect_ld, dctx);
    }
    RetCode del(const Key &key,
                uint64_t hash,
                uint32_t expect_ld,
                HashContext *dctx)
    {
        auto h1 = hash_1(hash);
        auto h2 = hash_2(hash);
        auto fp = hash_fp(hash);
        auto m = hash_m(hash);
        DVLOG(4) << "[race][subtable] DEL hash: " << std::hex << hash
                 << " got h1: " << h1 << ", h2: " << h2
                 << ", fp: " << pre_fp(fp);

        // RDMA: read the whole combined_bucket
        // Batch together.
        // TODO(RDMA): should check header here
        auto cb1 = combined_bucket(h1);
        auto ec = cb1.validate_staleness(expect_ld, m);
        if (ec == kCacheStale)
        {
            return ec;
        }
        DCHECK_EQ(ec, kOk);
        auto cb2 = combined_bucket(h2);
        ec = cb2.validate_staleness(expect_ld, m);
        if (ec == kCacheStale)
        {
            return ec;
        }
        DCHECK_EQ(ec, kOk);
        // RDMA: do the local search for any slot *may* match the key
        auto slot_views_1 = cb1.locate(fp);
        for (auto view : slot_views_1)
        {
            // RDMA: read here
            auto *kv_block = (KVBlock *) DCHECK_NOTNULL(view.ptr());
            auto rc = do_del_if_real_match(view, kv_block, key, dctx);
            if (rc == kOk)
            {
                return kOk;
            }
            DCHECK_EQ(rc, kNotFound);
        }
        auto slot_views_2 = cb2.locate(fp);
        for (auto view : slot_views_2)
        {
            // RDMA: read here
            auto *kv_block = (KVBlock *) DCHECK_NOTNULL(view.ptr());
            auto rc = do_del_if_real_match(view, kv_block, key, dctx);
            if (rc == kOk)
            {
                return kOk;
            }
            DCHECK_EQ(rc, kNotFound);
        }
        return kNotFound;
    }
    static constexpr size_t max_capacity()
    {
        return kTotalBucketNr * Bucket<kSlotNr>::max_capacity();
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

private:
    /**
     * @brief do anything except for re-read
     *
     */
    RetCode do_put_phase_one(const Key &key,
                             const Value &value,
                             uint64_t hash,
                             uint32_t expect_ld,
                             HashContext *dctx)
    {
        // RDMA: read the whole combined_bucket
        // Batch together.
        // TODO(RDMA): should write KV block here, parallely
        auto h1 = hash_1(hash);
        auto h2 = hash_2(hash);
        auto fp = hash_fp(hash);
        auto m = hash_m(hash);

        auto cb1 = combined_bucket(h1);
        auto ec = cb1.validate_staleness(expect_ld, m);
        if (ec == kCacheStale)
        {
            return kCacheStale;
        }
        DCHECK_EQ(ec, kOk);

        auto cb2 = combined_bucket(h2);
        ec = cb2.validate_staleness(expect_ld, m);
        if (ec == kCacheStale)
        {
            return kCacheStale;
        }
        DCHECK_EQ(ec, kOk);

        auto *kv_block = KVBlock::new_instance(key, value);

        // RDMA: do the local search for any slot *may* match or empty
        auto slot_views_1 = cb1.locate(fp);
        auto slot_views_2 = cb2.locate(fp);

        auto len = (key.size() + value.size() + (kLenUnit - 1)) / kLenUnit;
        SlotView new_slot(fp, len, kv_block);

        for (auto view : slot_views_1)
        {
            auto rc = do_update_if_real_match(view, key, new_slot, dctx);
            if (rc == kOk)
            {
                return kOk;
            }
            DCHECK_EQ(rc, kNotFound);
        }
        for (auto view : slot_views_2)
        {
            auto rc = do_update_if_real_match(view, key, new_slot, dctx);
            if (rc == kOk)
            {
                return kOk;
            }
            DCHECK_EQ(rc, kNotFound);
        }

        // definitely miss
        // loop and try all the empty slots
        auto rc = do_find_empty_slot_to_insert(cb1, cb2, new_slot, dctx);
        if (rc == kOk)
        {
            return kOk;
        }
        hash_table_free(kv_block);
        return rc;
    }
    /**
     * @brief reread to delete any duplicated ones
     *
     */
    RetCode do_put_phase_two_reread(const Key &key,
                                    uint64_t hash,
                                    uint32_t expect_ld,
                                    HashContext *dctx)
    {
        auto h1 = hash_1(hash);
        auto h2 = hash_2(hash);
        auto fp = hash_fp(hash);
        auto m = hash_m(hash);

        DVLOG(4) << "[race][subtable] reread: " << pre(key)
                 << " to detect any duplicated ones";
        // RDMA: do the read again here
        auto cb1 = combined_bucket(h1);
        auto ec = cb1.validate_staleness(expect_ld, m);
        if (ec == kCacheStale)
        {
            return kCacheStale;
        }
        DCHECK_EQ(ec, kOk);
        auto cb2 = combined_bucket(h2);
        ec = cb2.validate_staleness(expect_ld, m);
        if (ec == kCacheStale)
        {
            return kCacheStale;
        }
        DCHECK_EQ(ec, kOk);
        // RDMA: do the local search for any slot *may* match or empty
        auto slot_views_1 = cb1.locate(fp);
        auto slot_views_2 = cb2.locate(fp);
        // Get the actual hit slots
        auto real_match_slot_views =
            get_real_match_slots(key, slot_views_1, slot_views_2);
        if (!real_match_slot_views.empty())
        {
            auto chosen_slot = deterministic_choose_slot(real_match_slot_views);
            for (auto view : real_match_slot_views)
            {
                if (view.slot() != chosen_slot.slot())
                {
                    DVLOG(4)
                        << "[race][subtable] reread: " << pre(key)
                        << " remove duplicated slot " << (void *) view.slot();
                    auto rc = do_remove(view, dctx);
                    CHECK(rc == kOk || rc == kRetry);
                }
            }
        }
        return kOk;
    }
    RetCode do_remove(SlotWithView view, HashContext *dctx)
    {
        auto expect_slot = view.view();
        auto desired_slot = view.view_after_clear();
        if (view.cas(expect_slot, desired_slot))
        {
            DVLOG(2) << "Clearing slot " << (void *) view.slot()
                     << (dctx == nullptr ? nulldctx : *dctx);
            hash_table_free(expect_slot.ptr());
            return kOk;
        }
        else
        {
            return kRetry;
        }
    }
    std::vector<SlotWithView> get_real_match_slots(
        const Key &key,
        const std::vector<SlotWithView> view_1,
        const std::vector<SlotWithView> view_2)
    {
        std::vector<SlotWithView> ret;
        for (auto view : view_1)
        {
            // TODO(RDMA): read here.
            // Possible to batch reads
            auto *kv_block = (KVBlock *) DCHECK_NOTNULL(view.ptr());
            if (slot_real_match(kv_block, key))
            {
                ret.push_back(view);
            }
        }
        for (auto view : view_2)
        {
            // TODO(RDMA): read here.
            // Possible to batch reads
            auto *kv_block = (KVBlock *) DCHECK_NOTNULL(view.ptr());
            if (slot_real_match(kv_block, key))
            {
                ret.push_back(view);
            }
        }
        return ret;
    }
    SlotWithView deterministic_choose_slot(
        const std::vector<SlotWithView> views)
    {
        if (views.empty())
        {
            return SlotWithView();
        }
        return *std::min_element(views.begin(), views.end());
    }
    RetCode do_find_empty_slot_to_insert(const CombinedBucketView<kSlotNr> &cb1,
                                         const CombinedBucketView<kSlotNr> &cb2,
                                         SlotView new_slot,
                                         HashContext *dctx)
    {
        DVLOG(4) << "[race][subtable] do_find_empty_slot_to_insert: new_slot "
                 << new_slot << ". Try to fetch empty slots.";
        auto empty_1 = cb1.fetch_empty(10);
        for (auto view : empty_1)
        {
            auto rc = do_update_if_empty(view, new_slot, dctx);
            if (rc == kOk)
            {
                DVLOG(4) << "[race][subtable] insert to empty slot SUCCEED "
                            "at h1. new_slot "
                         << new_slot << " at slot " << (void *) view.slot();
                return kOk;
            }
            CHECK(rc == kRetry || rc == kNotFound);
        }
        auto empty_2 = cb2.fetch_empty(10);
        for (auto view : empty_2)
        {
            auto rc = do_update_if_empty(view, new_slot, dctx);
            if (rc == kOk)
            {
                DVLOG(4) << "[race][subtable] insert to empty slot SUCCEED "
                            "at h2. new_slot "
                         << new_slot << " at slot " << (void *) view.slot();
                return kOk;
            }
            CHECK(rc == kRetry || rc == kNotFound);
        }
        DVLOG(4) << "[race][subtable] both bucket failed to insert slot "
                 << new_slot << " to any empty ones. Tried " << empty_1.size()
                 << " and " << empty_2.size() << " empty slots each.";
        return kNoMem;
    }
    RetCode do_update_if_real_match(SlotWithView view,
                                    const Key &key,
                                    SlotView new_slot,
                                    HashContext *dctx)
    {
        auto *kv_block = (KVBlock *) DCHECK_NOTNULL(view.ptr());

        if (!slot_real_match(kv_block, key))
        {
            return kNotFound;
        }
        return do_update(view, new_slot, dctx);
    }
    RetCode do_update_if_empty(SlotWithView view,
                               SlotView new_slot,
                               HashContext *dctx)
    {
        if (view.empty())
        {
            return do_update(view, new_slot, dctx);
        }
        return kNotFound;
    }
    RetCode do_update(SlotWithView view, SlotView new_slot, HashContext *dctx)
    {
        // TODO: the cas should free the old kv_block.
        auto expect_slot = view.view();
        if (view.cas(expect_slot, new_slot))
        {
            if (expect_slot.empty())
            {
                DVLOG(4)
                    << "[race][subtable] do_update SUCC: update into an empty "
                       "slot. New_slot "
                    << new_slot;
            }
            else
            {
                auto *kv_block = (KVBlock *) expect_slot.ptr();
                DVLOG(4) << "[race][subtable] do_update SUCC: for slot with "
                            "kv_block ("
                         << (void *) kv_block << ". New_slot " << new_slot;
                hash_table_free(kv_block);
            }
            DVLOG(2) << "[race][subtable] slot " << (void *) view.slot()
                     << " update to " << new_slot
                     << (dctx == nullptr ? nulldctx : *dctx);
            return kOk;
        }
        DVLOG(4) << "[race][subtable] do_update FAILED: new_slot " << new_slot;
        return kRetry;
    }

    bool slot_real_match(KVBlock *kv_block, const Key &key)
    {
        if (kv_block->key_len != key.size())
        {
            DVLOG(4) << "[race][subtable] slot_real_match FAILED: key "
                     << pre(key) << " miss: key len mismatch";

            return false;
        }
        if (memcmp(key.data(), kv_block->buf, key.size()) != 0)
        {
            DVLOG(4) << "[race][subtable] slot_real_match FAILED: key "
                     << pre(key) << " miss: key content mismatch";
            return false;
        }
        DVLOG(4) << "[race][subtable] slot_real_match SUCCEED: key "
                 << pre(key);
        return true;
    }
    RetCode do_get(KVBlock *kv_block, Value &value)
    {
        value.resize(kv_block->value_len);
        memcpy(value.data(),
               kv_block->buf + kv_block->key_len,
               kv_block->value_len);
        return kOk;
    }
    RetCode do_del_if_real_match(SlotWithView view,
                                 KVBlock *kv_block,
                                 const Key &key,
                                 HashContext *dctx)
    {
        if (!slot_real_match(kv_block, key))
        {
            return kNotFound;
        }
        return do_remove(view, dctx);
    }

    RetCode do_get_if_real_match(KVBlock *kv_block,
                                 const Key &key,
                                 Value &value)
    {
        if (!slot_real_match(kv_block, key))
        {
            return kNotFound;
        }
        return do_get(kv_block, value);
    }
    constexpr static size_t kItemSize = BucketGroup<kSlotNr>::size_bytes();
    BucketGroup<kSlotNr> *addr_{nullptr};
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
    os << m;
    return os;
}

template <size_t kDEntryNr, size_t kBucketGroupNr, size_t kSlotNr>
class RaceHashing
{
public:
    using SubTableT = SubTable<kBucketGroupNr, kSlotNr>;
    using pointer = std::shared_ptr<RaceHashing>;

    static_assert(is_power_of_two(kDEntryNr));

    RaceHashing(std::shared_ptr<patronus::mem::IAllocator> allocator,
                size_t initial_subtable_nr = 1,
                uint64_t seed = 5381)
        : allocator_(allocator), seed_(seed)
    {
        size_t dir_size = sizeof(SubTableT *) * kDEntryNr;
        CHECK_LE(initial_subtable_nr, kDEntryNr);
        entries_ = (SubTableT **) CHECK_NOTNULL(allocator_->alloc(dir_size));

        DVLOG(1) << "Allocated director " << (void *) entries_ << " with size "
                 << dir_size;

        for (size_t i = 0; i < kDEntryNr; ++i)
        {
            entries_[i] = nullptr;
        }
        CHECK_GE(initial_subtable_nr, 1);

        initial_subtable_nr = round_up_to_next_power_of_2(initial_subtable_nr);
        gd_ = log2(initial_subtable_nr);
        CHECK_LE(initial_subtable_nr, kDEntryNr);

        auto ld = gd();
        DVLOG(1) << "[race] initial_subtable_nr: " << initial_subtable_nr
                 << ", gd: " << gd_;
        for (size_t i = 0; i < initial_subtable_nr; ++i)
        {
            auto alloc_size = SubTableT::size_bytes();
            void *alloc_mem = CHECK_NOTNULL(allocator_->alloc(alloc_size));

            DVLOG(1) << "[race] allocating subtable " << i << " at "
                     << (void *) alloc_mem << " for size "
                     << SubTableT::size_bytes();

            entries_[i] = new SubTableT(ld, alloc_mem, alloc_size, i);
            cached_lds_[i] = ld;
        }
    }
    SubTableT &subtable(size_t idx)
    {
        DCHECK_LT(idx, subtable_nr_may_stale());
        return *entries_[idx];
    }
    const SubTableT &subtable(size_t idx) const
    {
        DCHECK_LT(idx, subtable_nr_may_stale());
        return *entries_[idx];
    }
    size_t subtable_nr_may_stale() const
    {
        return pow(2, gd());
    }

    ~RaceHashing()
    {
        for (size_t i = 0; i < subtable_nr_may_stale(); ++i)
        {
            // the address space is from allocator
            auto alloc_size = SubTableT::size_bytes();
            DVLOG(1) << "Freeing " << i
                     << " subtable: " << (void *) entries_[i]->addr()
                     << " with size " << alloc_size;
            allocator_->free(entries_[i]->addr(), alloc_size);
            // the object itself is new-ed
            delete entries_[i];
        }
        auto alloc_size = sizeof(SubTableT *) * kDEntryNr;
        DVLOG(1) << "Freeing directory: " << (void *) entries_ << " with size "
                 << alloc_size;
        allocator_->free(entries_, alloc_size);
    }

    RetCode put(const Key &key, const Value &value, HashContext *dctx = nullptr)
    {
        auto hash = hash_impl(key.data(), key.size(), seed_);
        auto m = hash_m(hash);
        auto fp = hash_fp(hash);
        auto rounded_m = round_hash_to_depth(m);
        DVLOG(3) << "[race] PUT key " << pre(key) << ", got hash " << std::hex
                 << hash << std::dec << ", m: " << m << ", rounded to "
                 << rounded_m << " (subtable) by gd(may stale): " << gd()
                 << ". fp: " << pre_fp(fp);
        auto *sub_table = entries_[rounded_m];
        auto rc =
            sub_table->put(key, value, hash, cached_lds_[rounded_m], dctx);
        if (rc == kOk)
        {
            DVLOG(2) << "[race] PUT key `" << key << "`, val `" << value << "`";
        }
        return rc;
    }
    RetCode del(const Key &key, HashContext *dctx = nullptr)
    {
        auto hash = hash_impl(key.data(), key.size(), seed_);
        auto m = hash_m(hash);
        auto fp = hash_fp(hash);
        auto rounded_m = round_hash_to_depth(m);
        DVLOG(3) << "[race] DEL key " << pre(key) << ", got hash " << std::hex
                 << hash << " hash, m: " << m << ", rounded to " << rounded_m
                 << " by gd(may stale): " << gd() << ". fp: " << pre_fp(fp);
        auto *sub_table = entries_[rounded_m];
        auto rc = sub_table->del(key, hash, cached_lds_[rounded_m], dctx);
        if (rc == kOk)
        {
            DVLOG(2) << "[race] DEL key " << key;
        }
        return rc;
    }
    RetCode get(const Key &key, Value &value, HashContext *dctx = nullptr)
    {
        auto hash = hash_impl(key.data(), key.size(), seed_);
        auto m = hash_m(hash);
        auto fp = hash_fp(hash);
        auto rounded_m = round_hash_to_depth(m);
        DVLOG(3) << "[race] GET key " << pre(key) << ", got hash " << std::hex
                 << hash << ", m: " << m << ", rounded to " << rounded_m
                 << " by gd(may stale): " << gd() << ". fp: " << pre_fp(fp);
        auto *sub_table = entries_[rounded_m];
        auto rc =
            sub_table->get(key, value, hash, cached_lds_[rounded_m], dctx);
        return rc;
    }
    static constexpr size_t max_capacity()
    {
        constexpr size_t kMaxSubTableNr = kDEntryNr;
        return kMaxSubTableNr * SubTableT::max_capacity();
    }
    double utilization() const
    {
        OnePassMonitor m;
        for (size_t i = 0; i < subtable_nr_may_stale(); ++i)
        {
            m.collect(subtable(i).utilization());
        }
        return m.average();
    }

    template <size_t A, size_t B, size_t C>
    friend std::ostream &operator<<(std::ostream &os,
                                    const RaceHashing<A, B, C> &rh);

private:
    uint32_t round_hash_to_depth(uint32_t h)
    {
        return h & ((1 << gd()) - 1);
    }
    size_t gd() const
    {
        return gd_.load(std::memory_order_relaxed);
    }

    std::shared_ptr<patronus::mem::IAllocator> allocator_;
    SubTableT **entries_{nullptr};
    std::array<uint32_t, kDEntryNr> cached_lds_;
    std::atomic<size_t> gd_{0};
    uint64_t seed_;
};

template <size_t kDEntryNr, size_t kBucketGroupNr, size_t kSlotNr>
inline std::ostream &operator<<(
    std::ostream &os, const RaceHashing<kDEntryNr, kBucketGroupNr, kSlotNr> &rh)
{
    os << "RaceHashTable with " << kDEntryNr << " dir entries, "
       << kBucketGroupNr << " bucket groups, " << kSlotNr
       << " slots each. Util: " << rh.utilization() << std::endl;
    for (size_t i = 0; i < pow(2, rh.gd()); ++i)
    {
        auto *sub_table = rh.entries_[i];
        os << "sub-table[" << i << "]: " << *sub_table << std::endl;
    }

    return os;
}

}  // namespace patronus::hash

#endif