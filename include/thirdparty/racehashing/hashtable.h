#pragma once
#ifndef PERTRONUS_RACEHASHING_HASHTABLE_H_
#define PERTRONUS_RACEHASHING_HASHTABLE_H_

#include <string>

#include "./bucket.h"
#include "./bucket_group.h"
#include "./kv_block.h"
#include "./slot.h"
#include "./subtable.h"
#include "./utils.h"
#include "patronus/memory/allocator.h"
#include "util/PerformanceReporter.h"
#include "util/Rand.h"

namespace patronus::hash
{
struct RaceHashingConfig
{
    size_t initial_subtable{1};
};

template <size_t kDEntryNr, size_t kBucketGroupNr, size_t kSlotNr>
struct RaceHashingMeta
{
    static_assert(is_power_of_two(kDEntryNr));
    using SubTableT = SubTable<kBucketGroupNr, kSlotNr>;

    // Take care before chaning the type
    // client need the sizeof(T) to work right
    std::array<SubTableT *, kDEntryNr> entries;
    std::array<uint32_t, kDEntryNr> lds;
    std::array<std::atomic<uint64_t>, kDEntryNr> expanding;
    std::atomic<uint64_t> gd;
    // template <size_t kA, size_t kB, size_t kC>
    // std::ostream &operator<<(std::ostream &,
    //                          const RaceHashingMeta<kA, kB, kC> &);
};
template <size_t kA, size_t kB, size_t kC>
inline std::ostream &operator<<(std::ostream &os,
                                const RaceHashingMeta<kA, kB, kC> &meta)
{
    os << "{RaceHashingMeta: gd: " << meta.gd << std::endl;
    for (size_t i = 0; i < pow(size_t(2), size_t(meta.gd)); ++i)
    {
        if (meta.entries[i] != nullptr)
        {
            os << "subtable[" << i << "] ld: " << meta.lds[i]
               << ", lock: " << meta.expanding[i] << " at "
               << (void *) meta.entries[i] << std::endl;
        }
        else
        {
            os << "subtable[" << i << "] NULL" << std::endl;
        }
    }
    return os;
}

template <size_t kA, size_t kB, size_t kC>
class RaceHashingHandleImpl;

template <size_t kDEntryNr, size_t kBucketGroupNr, size_t kSlotNr>
class RaceHashing
{
public:
    using SubTableT = SubTable<kBucketGroupNr, kSlotNr>;
    using pointer = std::shared_ptr<RaceHashing>;
    using MetaT = RaceHashingMeta<kDEntryNr, kBucketGroupNr, kSlotNr>;
    using Handle = RaceHashingHandleImpl<kDEntryNr, kBucketGroupNr, kSlotNr>;

    static_assert(is_power_of_two(kDEntryNr));

    constexpr static size_t meta_size()
    {
        return sizeof(RaceHashingMeta<kDEntryNr, kBucketGroupNr, kSlotNr>);
    }

    RaceHashing(std::shared_ptr<patronus::mem::IAllocator> allocator,
                const RaceHashingConfig &conf)
        : conf_(conf), allocator_(allocator)
    {
        auto initial_subtable_nr = conf_.initial_subtable;
        size_t alloc_size = meta_size();
        CHECK_LE(initial_subtable_nr, kDEntryNr);
        void *alloc_addr = CHECK_NOTNULL(allocator_->alloc(alloc_size));
        memset(alloc_addr, 0, alloc_size);
        meta_ = (MetaT *) alloc_addr;

        DVLOG(1) << "Allocated meta region " << (void *) meta_ << " with size "
                 << alloc_size;

        CHECK_GE(initial_subtable_nr, 1);

        initial_subtable_nr = round_up_to_next_power_of_2(initial_subtable_nr);
        set_gd(log2(initial_subtable_nr));
        CHECK_LE(initial_subtable_nr, kDEntryNr);

        auto ld = gd();
        DVLOG(1) << "[race] initial_subtable_nr: " << initial_subtable_nr
                 << ", gd: " << gd();
        for (size_t i = 0; i < initial_subtable_nr; ++i)
        {
            auto alloc_size = SubTableT::size_bytes();
            void *alloc_mem = CHECK_NOTNULL(allocator_->alloc(alloc_size));
            memset(alloc_mem, 0, alloc_size);

            DVLOG(1) << "[race] allocating subtable " << i << " at "
                     << (void *) alloc_mem << " for size "
                     << SubTableT::size_bytes();

            subtables_[i] = std::make_shared<SubTableT>(alloc_mem, alloc_size);
            subtables_[i]->update_header(ld, i);
            meta_->lds[i] = ld;
            meta_->entries[i] = (SubTableT *) alloc_mem;
        }
        DLOG(INFO) << "[race] meta addr: " << (void *) meta_addr()
                   << ", content: " << *meta_;
    }
    void refresh_subtables()
    {
        for (size_t i = 0; i < kDEntryNr; ++i)
        {
            auto subtable = subtables_[i];
            auto *raw_addr = meta_->entries[i];
            if (raw_addr != nullptr && subtable == nullptr)
            {
                subtables_[i] = std::make_shared<SubTableT>(
                    raw_addr, SubTableT::size_bytes());
            }
        }
    }
    std::shared_ptr<SubTableT> subtable(size_t idx) const
    {
        DCHECK_LT(idx, subtable_nr_may_stale());
        return subtables_[idx];
    }
    size_t subtable_nr_may_stale() const
    {
        return pow(2, gd());
    }

    size_t idx_to_depth(size_t idx)
    {
        if (idx == 0)
        {
            return 0;
        }
        return log2(idx) + 1;
    }

    ~RaceHashing()
    {
        std::set<void *> freed_entries_addr;
        for (size_t i = 0; i < kDEntryNr; ++i)
        {
            // the address space is from allocator
            auto alloc_size = SubTableT::size_bytes();
            auto *entry_addr = meta_->entries[i];
            if (entry_addr == nullptr ||
                freed_entries_addr.count(entry_addr) == 1)
            {
                DVLOG(1) << "Skip freeing already freed (or nullptr) entries_["
                         << i << "] entry_addr " << (void *) entry_addr;
                continue;
            }
            freed_entries_addr.insert(entry_addr);
            DVLOG(1) << "Freeing subtable[" << i
                     << "].addr(): " << (void *) entry_addr << " with size "
                     << alloc_size;
            allocator_->free(entry_addr, alloc_size);
        }
        auto size = meta_size();
        DVLOG(1) << "Freeing meta region: " << (void *) meta_ << " with size "
                 << size;
        allocator_->free(meta_, size);
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
            auto t = subtable(i);
            if (t != nullptr)
            {
                m.collect(t->utilization());
            }
        }
        return m.average();
    }

    template <size_t A, size_t B, size_t C>
    friend std::ostream &operator<<(std::ostream &os, RaceHashing<A, B, C> &rh);
    uint64_t meta_addr() const
    {
        return (uint64_t) meta_;
    }

private:
    RaceHashingConfig conf_;
    // TODO: this expanding variable should be RDMA globally-visable
    std::shared_ptr<patronus::mem::IAllocator> allocator_;
    uint64_t seed_;

    RaceHashingMeta<kDEntryNr, kBucketGroupNr, kSlotNr> *meta_;
    std::array<std::shared_ptr<SubTableT>, kDEntryNr> subtables_;

    uint32_t round_hash_to_depth(uint32_t h)
    {
        return h & ((1 << gd()) - 1);
    }
    size_t gd() const
    {
        return meta_->gd.load(std::memory_order_relaxed);
    }
    void set_gd(size_t new_gd)
    {
        meta_->gd = new_gd;
    }
};

template <size_t kDEntryNr, size_t kBucketGroupNr, size_t kSlotNr>
inline std::ostream &operator<<(
    std::ostream &os, RaceHashing<kDEntryNr, kBucketGroupNr, kSlotNr> &rh)
{
    rh.refresh_subtables();
    os << "RaceHashTable util: " << rh.utilization() << " with " << kDEntryNr
       << " dir entries, " << kBucketGroupNr << " bucket groups, " << kSlotNr
       << " slots each. " << std::endl;
    for (size_t i = 0; i < pow(2, rh.gd()); ++i)
    {
        // auto *sub_table = rh.meta_->entries[i];
        auto subtable = rh.subtable(i);
        auto *subtable_addr = rh.meta_->entries[i];
        auto ld = subtable->ld();

        if (subtable != nullptr)
        {
            os << "subtable[" << i << "] ld: " << ld << " at "
               << (void *) subtable_addr << ". " << *subtable << std::endl;
        }
        else
        {
            os << "subtable[" << i << "]: NULL " << std::endl;
        }
    }

    return os;
}

}  // namespace patronus::hash

#endif