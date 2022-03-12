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

class Slot
{
public:
    // so you have to catch the reference.
    Slot(uint8_t fp, uint8_t len, void *ptr)
    {
        set_ptr(ptr);
        set_fp(fp);
        set_len(len);
    }

    uint8_t fp() const
    {
        return ptr_.u8_h();
    }
    void set_fp(uint8_t fp)
    {
        ptr_.set_u8_h(fp);
    }
    uint8_t len() const
    {
        return ptr_.u8_l();
    }
    void set_len(uint8_t len)
    {
        ptr_.set_u8_l(len);
    }
    void *ptr() const
    {
        return ptr_.ptr();
    }
    void *addr() const
    {
        return (void *) &ptr_;
    }
    void set_ptr(void *_ptr)
    {
        ptr_.set_ptr(_ptr);
    }
    constexpr static size_t size_bytes()
    {
        return 8;
    }
    uint64_t val() const
    {
        return ptr_.val();
    }

    bool empty() const
    {
        return ptr_.ptr() == nullptr;
    }

    bool cas(Slot &expected, const Slot &desired)
    {
        return ptr_.cas(expected.ptr_, desired.ptr_);
    }

    bool match(uint8_t _fp) const
    {
        return !empty() && fp() == _fp;
    }
    friend std::ostream &operator<<(std::ostream &, const Slot &);

private:
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

struct BucketHeader
{
    uint32_t local_depth;
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
        return slot(0);
    }
    const BucketHeader &header() const
    {
        return slot(0);
    }
    void *addr() const
    {
        return addr_;
    }
    constexpr static size_t size_bytes()
    {
        return Slot::size_bytes() * kSlotNr;
    }
    std::vector<Slot *> locate(uint8_t fp)
    {
        std::vector<Slot *> ret;
        for (size_t i = 1; i < kSlotNr; ++i)
        {
            auto &get_slot = slot(i);
            if (get_slot.match(fp))
            {
                ret.push_back(&get_slot);
            }
        }
        return ret;
    }

    std::vector<Slot *> fetch_empty(size_t limit) const
    {
        std::vector<Slot *> ret;
        auto poll_slot_idx = fast_pseudo_rand_int(1, kSlotNr - 1);
        for (size_t i = 0; i < kDataSlotNr; ++i)
        {
            auto idx = (poll_slot_idx + i) % kDataSlotNr + 1;
            CHECK_GE(idx, 1);
            CHECK_LT(idx, kSlotNr);
            auto &get_slot = slot(idx);

            if (get_slot.empty())
            {
                ret.push_back((Slot *) &get_slot);
                if (ret.size() >= limit)
                {
                    return ret;
                }
            }
        }
        return ret;
    }

    std::pair<std::vector<Slot *>, Slot *> locate_or_empty(uint8_t fp)
    {
        std::vector<Slot *> ret;
        auto poll_slot_idx = fast_pseudo_rand_int(1, kSlotNr - 1);
        Slot *first_empty_slot = nullptr;
        for (size_t i = 0; i < kDataSlotNr; ++i)
        {
            auto idx = (poll_slot_idx + i) % kDataSlotNr + 1;
            CHECK_GE(idx, 1);
            CHECK_LT(idx, kSlotNr);
            auto &get_slot = slot(idx);

            if (get_slot.match(fp))
            {
                DVLOG(6) << "[bench][bucket] Search for fp " << pre_fp(fp)
                         << " got possible match slot at " << idx;
                ret.push_back(&get_slot);
            }

            if (get_slot.empty() && first_empty_slot == nullptr)
            {
                DVLOG(6) << "[bench][bucket] Search for fp " << pre_fp(fp)
                         << " got first empty slot at " << idx;
                first_empty_slot = &get_slot;
            }
        }

        return {ret, first_empty_slot};
    }
    static constexpr size_t max_capacity()
    {
        return kDataSlotNr;
    }
    double utilization() const
    {
        size_t full_nr = 0;
        for (size_t i = 0; i < kSlotNr; ++i)
        {
            if (!slot(i).empty())
            {
                full_nr++;
            }
        }
        DVLOG(1) << "[slot] utilization: full_nr: " << full_nr
                 << ", total: " << kSlotNr
                 << ", util: " << 1.0 * full_nr / kDataSlotNr;
        return 1.0 * full_nr / kDataSlotNr;
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
    std::vector<Slot *> fetch_empty(size_t limit) const
    {
        // NOTE:
        // fetch from main bucket first
        auto ret = main_.fetch_empty(limit);
        if (ret.size() > 0)
        {
            return ret;
        }
        return overflow_.fetch_empty(limit);
    }
    std::vector<Slot *> locate(uint8_t fp)
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
    std::pair<std::vector<Slot *>, Slot *> locate_or_empty(uint8_t fp)
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

    SubTable(size_t ld, void *addr, size_t size)
        : ld_(ld), addr_((BucketGroup<kSlotNr> *) CHECK_NOTNULL(addr))
    {
        LOG(INFO) << "Subtable ctor called. addr_: " << addr_
                  << ", addr: " << addr << ", memset bytes: " << size_bytes();

        CHECK_GE(size, size_bytes());
        // NOTE:
        // the memset is required to fill the slot as empty
        memset(addr_, 0, size_bytes());
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
    size_t ld() const
    {
        return ld_;
    }
    RetCode get(const Key &key, Value &value, uint64_t hash)
    {
        auto h1 = hash_1(hash);
        auto h2 = hash_2(hash);
        auto fp = hash_fp(hash);
        DVLOG(4) << "[race][subtable] GET hash: " << std::hex << hash
                 << " got h1: " << h1 << ", h2: " << h2
                 << ", fp: " << pre_fp(fp);

        // RDMA: read the whole combined_bucket
        // Batch together.
        // TODO(RDMA): should check header here
        auto cb1 = combined_bucket(h1);
        auto cb2 = combined_bucket(h2);
        // RDMA: do the local search for any slot *may* match the key
        auto slots_1 = cb1.locate(fp);
        for (auto *slot : slots_1)
        {
            // RDMA: read here
            auto *kv_block = (KVBlock *) slot->ptr();
            auto rc = do_get_if_real_match(kv_block, key, value);
            if (rc == kOk)
            {
                return kOk;
            }
            DCHECK_EQ(rc, kNotFound);
        }
        auto slots_2 = cb2.locate(fp);
        for (auto *slot : slots_2)
        {
            // RDMA: read here
            auto *kv_block = (KVBlock *) slot->ptr();
            auto rc = do_get_if_real_match(kv_block, key, value);
            if (rc == kOk)
            {
                return kOk;
            }
            DCHECK_EQ(rc, kNotFound);
        }
        return kNotFound;
    }

    RetCode put(const Key &key, const Value &value, uint64_t hash)
    {
        auto h1 = hash_1(hash);
        auto h2 = hash_2(hash);
        auto fp = hash_fp(hash);
        DVLOG(4) << "[race][subtable] PUT hash: " << std::hex << hash
                 << " got h1: " << h1 << ", h2: " << h2
                 << ", fp: " << pre_fp(fp);

        auto rc = do_put_phase_one(key, value, h1, h2, fp);
        if (rc != kOk)
        {
            return rc;
        }
        return do_put_phase_two_reread(key, h1, h2, fp);
    }
    RetCode del(const Key &key, uint64_t hash)
    {
        CHECK(false) << "TODO:" << key << ", " << hash;
    }
    static constexpr size_t max_capacity()
    {
        return kTotalBucketNr * Bucket<kSlotNr>::max_capacity();
    }

private:
    /**
     * @brief do anything except for re-read
     *
     */
    RetCode do_put_phase_one(const Key &key,
                             const Value &value,
                             uint64_t h1,
                             uint64_t h2,
                             uint8_t fp)
    {
        // RDMA: read the whole combined_bucket
        // Batch together.
        // TODO(RDMA): should check header here
        // TODO(RDMA): should write KV block here, parallely
        auto cb1 = combined_bucket(h1);
        auto cb2 = combined_bucket(h2);
        auto *kv_block = KVBlock::new_instance(key, value);

        // RDMA: do the local search for any slot *may* match or empty
        auto slots_1 = cb1.locate(fp);
        auto slots_2 = cb2.locate(fp);

        auto len = (key.size() + value.size() + (kLenUnit - 1)) / kLenUnit;
        Slot new_slot(fp, len, kv_block);

        for (auto *slot : slots_1)
        {
            auto rc = do_update_if_real_match(slot, key, new_slot);
            if (rc == kOk)
            {
                return kOk;
            }
            DCHECK_EQ(rc, kNotFound);
        }
        for (auto *slot : slots_2)
        {
            auto rc = do_update_if_real_match(slot, key, new_slot);
            if (rc == kOk)
            {
                return kOk;
            }
            DCHECK_EQ(rc, kNotFound);
        }

        // definitely miss
        // loop and try all the empty slots
        auto rc = do_find_empty_slot_to_insert(cb1, cb2, new_slot);
        if (rc == kOk)
        {
            return kOk;
        }
        free(kv_block);
        return rc;
    }
    /**
     * @brief reread to delete any duplicated ones
     *
     */
    RetCode do_put_phase_two_reread(const Key &key,
                                    uint64_t h1,
                                    uint64_t h2,
                                    uint8_t fp)
    {
        // RDMA: do the read again here
        auto cb1 = combined_bucket(h1);
        auto cb2 = combined_bucket(h2);
        // RDMA: do the local search for any slot *may* match or empty
        auto slot_1 = cb1.locate(fp);
        auto slot_2 = cb2.locate(fp);
        // Get the actual hit slots
        auto real_match_slots = get_real_match_slots(key, slot_1, slot_2);
        if (!real_match_slots.empty())
        {
            auto *chosen_slot = deterministic_choose_slot(real_match_slots);
            for (auto *slot : real_match_slots)
            {
                if (slot != chosen_slot)
                {
                    auto rc = do_remove(slot);
                    CHECK(rc == kOk || rc == kRetry);
                }
            }
        }
        return kOk;
    }
    RetCode do_remove(Slot *slot)
    {
        CHECK(false) << "TODO: slot: " << (void *) slot;
        return kInvalid;
    }
    std::vector<Slot *> get_real_match_slots(const Key &key,
                                             const std::vector<Slot *> slot_1,
                                             const std::vector<Slot *> slot_2)
    {
        std::vector<Slot *> ret;
        for (auto *slot : slot_1)
        {
            // TODO(RDMA): read here.
            // Possible to batch reads
            auto *kv_block = (KVBlock *) slot->ptr();
            if (slot_real_match(kv_block, key))
            {
                ret.push_back(slot);
            }
        }
        for (auto *slot : slot_2)
        {
            // TODO(RDMA): read here.
            // Possible to batch reads
            auto *kv_block = (KVBlock *) slot->ptr();
            if (slot_real_match(kv_block, key))
            {
                ret.push_back(slot);
            }
        }
        return ret;
    }
    Slot *deterministic_choose_slot(const std::vector<Slot *> slots)
    {
        if (slots.empty())
        {
            return nullptr;
        }
        auto *ret = slots[0];
        for (auto *slot : slots)
        {
            if (slot < ret)
            {
                ret = slot;
            }
        }
        return ret;
    }
    RetCode do_find_empty_slot_to_insert(const CombinedBucketView<kSlotNr> &cb1,
                                         const CombinedBucketView<kSlotNr> &cb2,
                                         Slot &new_slot)
    {
        DVLOG(4) << "[race][subtable] do_find_empty_slot_to_insert: new_slot "
                 << new_slot << ". Try to fetch empty slots.";
        auto empty_1 = cb1.fetch_empty(10);
        for (auto *slot : empty_1)
        {
            auto rc = do_update_if_empty(slot, new_slot);
            if (rc == kOk)
            {
                DVLOG(4) << "[race][subtable] insert to empty slot SUCCEED "
                            "at h1. new_slot "
                         << new_slot << " at slot " << (void *) slot;
                return kOk;
            }
            CHECK(rc == kRetry || rc == kNotFound);
        }
        auto empty_2 = cb2.fetch_empty(10);
        for (auto *slot : empty_2)
        {
            auto rc = do_update_if_empty(slot, new_slot);
            if (rc == kOk)
            {
                DVLOG(4) << "[race][subtable] insert to empty slot SUCCEED "
                            "at h2. new_slot "
                         << new_slot << " at slot " << (void *) slot;
                return kOk;
            }
            CHECK(rc == kRetry || rc == kNotFound);
        }
        DVLOG(4) << "[race][subtable] both bucket failed to insert slot "
                 << new_slot << " at empty";
        return kNoMem;
    }
    RetCode do_update_if_real_match(Slot *slot, const Key &key, Slot &new_slot)
    {
        auto *kv_block = (KVBlock *) slot->ptr();
        if (!slot_real_match(kv_block, key))
        {
            return kNotFound;
        }
        return do_update(slot, new_slot);
    }
    RetCode do_update_if_empty(Slot *slot, Slot &new_slot)
    {
        if (slot->empty())
        {
            return do_update(slot, new_slot);
        }
        return kNotFound;
    }
    RetCode do_update(Slot *slot, Slot &new_slot)
    {
        // TODO: the cas should free the old kv_block.
        Slot expect_slot = *slot;
        if (slot->cas(expect_slot, new_slot))
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
                free(kv_block);
            }
            return kOk;
        }
        DVLOG(4) << "[race][subtable] do_update FAILED: new_slot " << new_slot;
        return kRetry;
    }

    bool slot_real_match(KVBlock *kv_block, const Key &key)
    {
        if (kv_block->key_len != key.size())
        {
            DVLOG(4) << "[race][subtable] slot_real_match FAILED: GET key "
                     << pre(key) << " miss: key len mismatch";

            return false;
        }
        if (memcmp(key.data(), kv_block->buf, key.size()) != 0)
        {
            DVLOG(4) << "[race][subtable] slot_real_match FAILED: GET key "
                     << pre(key) << " miss: key content mismatch";
            return false;
        }
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
    size_t ld_{0};
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
    static_assert(is_power_of_two(kDEntryNr));
    RaceHashing(std::shared_ptr<patronus::mem::IAllocator> allocator,
                size_t initial_subtable_nr = 1,
                uint64_t seed = 5381)
        : allocator_(allocator), seed_(seed)
    {
        entries_ = (SubTableT **) CHECK_NOTNULL(
            allocator_->alloc(sizeof(SubTableT *) * kDEntryNr));
        for (size_t i = 0; i < kDEntryNr; ++i)
        {
            entries_[i] = nullptr;
        }
        CHECK_GE(initial_subtable_nr, 1);

        initial_subtable_nr = round_up_to_next_power_of_2(initial_subtable_nr);
        gd_ = log2(initial_subtable_nr);
        auto ld = gd();
        DVLOG(1) << "[race] initial_subtable_nr: " << initial_subtable_nr
                 << ", gd: " << gd_;
        for (size_t i = 0; i < initial_subtable_nr; ++i)
        {
            DVLOG(3) << "[race] allocating subtable for size "
                     << SubTableT::size_bytes();
            auto alloc_size = SubTableT::size_bytes();
            void *alloc_mem = CHECK_NOTNULL(allocator_->alloc(alloc_size));

            entries_[i] = new SubTableT(ld, alloc_mem, alloc_size);
        }
    }

    ~RaceHashing()
    {
        for (size_t i = 0; i < pow(2, gd()); ++i)
        {
            // the address space is from allocator
            auto alloc_size = SubTableT::size_bytes();
            allocator_->free(entries_[i]->addr(), alloc_size);
            // the object itself is new-ed
            delete entries_[i];
        }
        auto alloc_size = sizeof(SubTableT *) * kDEntryNr;
        allocator_->free(entries_, alloc_size);
    }

    RetCode put(const Key &key, const Value &value)
    {
        auto hash = hash_impl(key.data(), key.size(), seed_);
        auto m = hash_m(hash);
        auto rounded_m = round_hash_to_depth(m);
        DVLOG(3) << "[race] PUT key " << pre(key) << ", got hash " << std::hex
                 << hash << ", m: " << m << ", rounded to " << rounded_m
                 << " (subtable) by gd(may stale): " << gd();
        auto *sub_table = entries_[rounded_m];
        return sub_table->put(key, value, hash);
    }
    RetCode del(const Key &key)
    {
        auto hash = hash_impl(key.data(), key.size(), seed_);
        auto m = hash_m(hash);
        auto rounded_m = round_hash_to_depth(m);
        DVLOG(3) << "[race] DEL key " << pre(key) << ", got hash " << std::hex
                 << hash << " hash, m: " << m << ", rounded to " << rounded_m
                 << " by gd(may stale): " << gd();
        auto *sub_table = entries_[rounded_m];
        return sub_table->del(key, hash);
    }
    RetCode get(const Key &key, Value &value)
    {
        auto hash = hash_impl(key.data(), key.size(), seed_);
        auto m = hash_m(hash);
        auto rounded_m = round_hash_to_depth(m);
        DVLOG(3) << "[race] GET key " << pre(key) << ", got hash " << std::hex
                 << hash << ", m: " << m << ", rounded to " << rounded_m
                 << " by gd(may stale): " << gd();
        auto *sub_table = entries_[rounded_m];
        return sub_table->get(key, value, hash);
    }
    static constexpr size_t max_capacity()
    {
        constexpr size_t kMaxSubTableNr = kDEntryNr;
        return kMaxSubTableNr * SubTableT::max_capacity();
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

    using SubTableT = SubTable<kBucketGroupNr, kSlotNr>;
    std::shared_ptr<patronus::mem::IAllocator> allocator_;
    SubTableT **entries_{nullptr};
    std::atomic<size_t> gd_{0};
    uint64_t seed_;
};

template <size_t kDEntryNr, size_t kBucketGroupNr, size_t kSlotNr>
inline std::ostream &operator<<(
    std::ostream &os, const RaceHashing<kDEntryNr, kBucketGroupNr, kSlotNr> &rh)
{
    os << "RaceHashTable with " << kDEntryNr << " dir entries, "
       << kBucketGroupNr << " bucket groups, " << kSlotNr << " slots each"
       << std::endl;
    for (size_t i = 0; i < pow(2, rh.gd()); ++i)
    {
        auto *sub_table = rh.entries_[i];
        os << "sub-table[" << i << "]: " << *sub_table << std::endl;
    }

    return os;
}

}  // namespace patronus::hash

#endif