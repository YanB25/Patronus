#pragma once
#ifndef PERTRONUS_RACEHASHING_HASHTABLE_H_
#define PERTRONUS_RACEHASHING_HASHTABLE_H_

#include <string>

#include "./bucket.h"
#include "./bucket_group.h"
#include "./slot.h"
#include "./subtable.h"
#include "./utils.h"
#include "patronus/memory/allocator.h"
#include "util/PerformanceReporter.h"
#include "util/Rand.h"

namespace patronus::hash
{
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