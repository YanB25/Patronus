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
    size_t try_empty_nr{10};
    size_t initial_subtable{1};
    bool auto_expand{false};
    bool auto_update_dir{false};
    uint64_t seed = 5381;
};
template <size_t kDEntryNr, size_t kBucketGroupNr, size_t kSlotNr>
class RaceHashing
{
public:
    using SubTableT = SubTable<kBucketGroupNr, kSlotNr>;
    using pointer = std::shared_ptr<RaceHashing>;

    static_assert(is_power_of_two(kDEntryNr));

    RaceHashing(std::shared_ptr<patronus::mem::IAllocator> allocator,
                const RaceHashingConfig &conf)
        : conf_(conf), allocator_(allocator)
    {
        auto initial_subtable_nr = conf_.initial_subtable;
        size_t dir_size = sizeof(SubTableT *) * kDEntryNr;
        CHECK_LE(initial_subtable_nr, kDEntryNr);
        entries_ = (SubTableT **) CHECK_NOTNULL(allocator_->alloc(dir_size));
        memset(entries_, 0, sizeof(dir_size));

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
    SubTableT *subtable(size_t idx)
    {
        DCHECK_LT(idx, subtable_nr_may_stale());
        return entries_[idx];
    }
    const SubTableT *subtable(size_t idx) const
    {
        DCHECK_LT(idx, subtable_nr_may_stale());
        return entries_[idx];
    }
    size_t subtable_nr_may_stale() const
    {
        return pow(2, gd());
    }
    RetCode expand(size_t subtable_idx, HashContext *dctx = nullptr)
    {
        CHECK_LT(subtable_idx, kDEntryNr);
        bool expect = false;
        if (!expandings_[subtable_idx].compare_exchange_strong(expect, true))
        {
            return kRetry;
        }
        // okay, entered critical section
        // auto depth = idx_to_depth(subtable_idx);
        auto *src_subtable = CHECK_NOTNULL(entries_[subtable_idx]);
        auto depth = src_subtable->ld();
        auto next_depth = depth + 1;
        auto expect_gd = gd();
        if (pow(2, next_depth) > kDEntryNr)
        {
            DLOG(WARNING) << "[race][expand] trying to expand to gd "
                          << next_depth << " with entries "
                          << pow(2, next_depth) << ". Out of directory entry.";
            return kNoMem;
        }
        if (next_depth >= expect_gd)
        {
            // insert all the entries into the directory
            // before updating gd
            for (size_t i = 0; i < pow(2, next_depth); ++i)
            {
                if (entries_[i] == nullptr)
                {
                    auto from_subtable_idx = i & (~(1 << depth));
                    DVLOG(1)
                        << "[race][expand] (0) Update gd and directory: "
                           "setting subtable["
                        << i << "] to subtale[" << from_subtable_idx << "]";
                    entries_[i] = entries_[from_subtable_idx];
                }
            }

            DVLOG(1)
                << "[race][expand] (0) Update gd and directory: setting gd to "
                << next_depth;
            CHECK(gd_.compare_exchange_strong(expect_gd, next_depth))
                << "Should not cas failed, because we are in the critical "
                   "section";
        }
        auto next_subtable_idx = subtable_idx | (1 << depth);

        CHECK_LT(next_subtable_idx, kDEntryNr);

        if (entries_[next_subtable_idx] != nullptr &&
            entries_[next_subtable_idx] != entries_[subtable_idx])
        {
            CHECK(false) << "Failed to extend: already have subtables here. "
                            "Trying to expand subtable["
                         << subtable_idx << "] to subtable["
                         << next_subtable_idx
                         << "]. But already exists subtable at "
                         << (void *) entries_[next_subtable_idx]
                         << ", which is not nullptr or "
                         << (void *) entries_[subtable_idx] << " from subtable["
                         << subtable_idx << "]"
                         << ". depth: " << depth
                         << ", (1<<depth): " << (1 << depth);
        }

        // 1) allocate subtable here
        auto alloc_size = SubTableT::size_bytes();
        void *alloc_mem = CHECK_NOTNULL(allocator_->alloc(alloc_size));
        DVLOG(1) << "[race][expand] (1) expanding subtable[" << subtable_idx
                 << "] to next subtable[" << next_subtable_idx
                 << "]. Allocated " << alloc_size << " at "
                 << (void *) alloc_mem;

        // 2) init subtable: setup the bucket header
        auto ld = next_depth;
        auto *new_subtable =
            new SubTableT(ld, alloc_mem, alloc_size, next_subtable_idx);
        DVLOG(1) << "[race][expand] (2) set up header for subtable["
                 << next_subtable_idx << "]. ld: " << ld
                 << ", suffix: " << next_subtable_idx;

        // 3) insert the subtable into the directory AND lock the subtable.
        expect = false;
        CHECK(expandings_[next_subtable_idx].compare_exchange_strong(expect,
                                                                     true));
        entries_[next_subtable_idx] = new_subtable;
        cached_lds_[next_subtable_idx] = ld;
        cached_lds_[subtable_idx] = ld;
        DVLOG(1) << "[race][expand] (3) Lock subtable[" << next_subtable_idx
                 << "] at directory. Insert subtable " << (void *) new_subtable
                 << " into directory. Update ld to " << ld << " for subtable["
                 << subtable_idx << "] and subtable[" << next_subtable_idx
                 << "]";

        // 4) move data.
        // 4.1) update bucket suffix
        DVLOG(1) << "[race][expand] (4.1) update bucket header for subtable["
                 << subtable_idx << "]. ld: " << ld
                 << ", suffix: " << subtable_idx;
        auto *origin_subtable = entries_[subtable_idx];
        origin_subtable->update_header(ld, subtable_idx);
        // Before 4.1) Iterate all the entries in @entries_, check for any
        // recursive updates to the entries.
        // For example, when
        // subtable_idx in {1, 5, 9, 13} pointing to the same LD = 2, suffix =
        // 0b01, when expanding subtable 1 to 5, should also set 9 pointing to 1
        // (not changed) and 13 pointing to 5 (changed)
        DVLOG(1) << "[race][expand] (4.2) recursively checks all the entries "
                    "for further updates";
        for (size_t i = 0; i < kDEntryNr; ++i)
        {
            auto *sub_table = entries_[i];
            if (sub_table != nullptr)
            {
                auto ld = sub_table->ld();
                auto rounded_table_id = round_hash_to_bit(i, ld);
                auto *new_sub_table = entries_[rounded_table_id];
                if (new_sub_table != sub_table)
                {
                    DVLOG(1)
                        << "[race][expand] (4.2) cascadely update subtable["
                        << i << "] to subtable[" << rounded_table_id
                        << "]. i.e. from " << (void *) sub_table << " to "
                        << (void *) new_sub_table;
                    entries_[i] = entries_[rounded_table_id];
                }
            }
        }

        // 4.3) insert all items from the old bucket to the new
        DVLOG(1) << "[race][expand] (4.3) migrate slots from subtable["
                 << subtable_idx << "] to subtable[" << next_subtable_idx
                 << "] with the " << ld << "-th bit == 1";
        auto bits = ld;
        CHECK_GE(bits, 1) << "Test the " << bits
                          << "-th bits, which index from one.";
        auto migrate_views = origin_subtable->locate_migratable(bits - 1, dctx);
        // 4.4) delete all items from the old bucket
        DVLOG(1) << "[race][expand] (4.4) do migrate: will migrate "
                 << migrate_views.size() << " items from subtable["
                 << subtable_idx << "] to subtable[" << next_subtable_idx
                 << "]";
        for (auto m : migrate_views)
        {
            CHECK_EQ(do_migrate(m, *origin_subtable, *new_subtable, dctx), kOk);
        }

        // 5) unlock
        DVLOG(1) << "[race][expand] (5) Unlock subtable[" << next_subtable_idx
                 << "] and subtable[" << subtable_idx << "]";
        expandings_[next_subtable_idx] = false;
        expandings_[subtable_idx] = false;
        DVLOG(1) << "[race][expand] Finished. " << *this;
        return kOk;
    }
    size_t idx_to_depth(size_t idx)
    {
        if (idx == 0)
        {
            return 0;
        }
        return log2(idx) + 1;
    }
    RetCode do_migrate(MigrateView mig_view,
                       SubTableT &src_table,
                       SubTableT &dst_table,
                       HashContext *dctx)
    {
        auto hash = mig_view.hash();
        auto h1 = hash_1(hash);
        auto h2 = hash_2(hash);
        auto fp = hash_fp(hash);
        if constexpr (debug())
        {
            auto *block = (KVBlock *) mig_view.view().ptr();
            if (block != nullptr)
            {
                dctx->key = std::string(block->buf, block->key_len);
                dctx->value =
                    std::string(block->buf + block->key_len, block->value_len);
            }

            DVLOG(4) << "[race][subtable] MIG hash: " << std::hex << hash
                     << " got h1: " << h1 << ", h2: " << h2
                     << ", fp: " << pre_fp(fp);
        }

        // RDMA: read the whole combined_bucket
        // Batch together.
        // TODO(RDMA): should check header here
        auto cb1 = dst_table.combined_bucket(h1);
        // cb1 & cb2 will not be stale, because they are locked
        auto cb2 = dst_table.combined_bucket(h2);
        // RDMA: do the local search for any slot *may* match the key
        // auto slot_views_1 = cb1.locate(fp);
        auto rc = dst_table.do_find_empty_slot_to_insert(
            cb1, cb2, mig_view.view(), dctx);
        if (rc == kOk)
        {
            auto with_view = mig_view.with_view();
            CHECK_EQ(src_table.do_remove(with_view, dctx), kOk)
                << "TODO: impl when remove failed. slot: "
                << (void *) with_view.slot() << ", expect: " << with_view.view()
                << ", desired: " << with_view.view_after_clear()
                << ", CAS failed. actual: " << with_view.slot()->val();
            return kOk;
        }
        return rc;
    }

    ~RaceHashing()
    {
        std::set<void *> freed_entries_addr;
        for (size_t i = 0; i < kDEntryNr; ++i)
        {
            // the address space is from allocator
            auto alloc_size = SubTableT::size_bytes();
            auto *entry_addr = entries_[i];
            if (entry_addr == nullptr ||
                freed_entries_addr.count(entry_addr) == 1)
            {
                DVLOG(1) << "Skip freeing already freed (or nullptr) entries_["
                         << i << "] entry_addr " << (void *) entry_addr;
                continue;
            }
            freed_entries_addr.insert(entry_addr);
            auto *sub_table_addr = entry_addr->addr();
            DVLOG(1) << "Freeing subtable[" << i
                     << "].addr(): " << (void *) sub_table_addr << " with size "
                     << alloc_size << " at entry: " << (void *) entry_addr;
            allocator_->free(sub_table_addr, alloc_size);
            // the object itself is new-ed
            delete entry_addr;
        }
        auto alloc_size = sizeof(SubTableT *) * kDEntryNr;
        DVLOG(1) << "Freeing directory: " << (void *) entries_ << " with size "
                 << alloc_size;
        allocator_->free(entries_, alloc_size);
    }

    RetCode update_directory_cache(HashContext *dctx = nullptr)
    {
        // RDMA read the whole directory.
        // TODO: the cached_lds_ should be thread local
        // but just skip this since does not matter
        for (size_t i = 0; i < kDEntryNr; ++i)
        {
            auto *sub_table = entries_[i];
            if (sub_table)
            {
                cached_lds_[i] = sub_table->ld();
                DLOG_IF(INFO, kEnableDebug && dctx != nullptr)
                    << "[race] update_directory_cache: update cached_ld to "
                    << cached_lds_[i] << " for subtable[" << i << "]" << *dctx;
            }
        }
        return kOk;
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
            DLOG_IF(INFO, kEnableDebug && dctx != nullptr)
                << "[race] PUT SUCC at subtable[" << rounded_m << "] at "
                << (void *) sub_table << ". " << *dctx;

            DVLOG(2) << "[race] PUT key `" << key << "`, val `" << value << "`";
        }
        if (rc == kNoMem && conf_.auto_expand)
        {
            auto ld = sub_table->ld();
            auto overflow_subtable_idx = round_hash_to_bit(rounded_m, ld);
            DLOG_IF(INFO, kEnableDebug && dctx != nullptr)
                << "[race][auto-expand] put: put failed by kNoMem. "
                   "Automatically expand subtable["
                << overflow_subtable_idx << "] from rounde_m: " << rounded_m
                << " by ld: " << ld << ". Current table: " << *this;
            auto rc = expand(overflow_subtable_idx, dctx);
            if (rc == kOk)
            {
                return put(key, value, dctx);
            }
            return rc;
        }
        if (rc == kCacheStale && conf_.auto_update_dir)
        {
            CHECK_EQ(update_directory_cache(dctx), kOk);
            // retry
            return put(key, value, dctx);
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
        DLOG_IF(INFO, kEnableDebug && dctx != nullptr)
            << "[race] GET subtable[" << rounded_m << "] at "
            << (void *) sub_table << ". Got result " << rc << ". " << *dctx;
        if (rc == kCacheStale && conf_.auto_update_dir)
        {
            CHECK_EQ(update_directory_cache(), kOk);
            return get(key, value, dctx);
        }

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
            auto *t = subtable(i);
            if (t != nullptr)
            {
                m.collect(t->utilization());
            }
        }
        return m.average();
    }

    template <size_t A, size_t B, size_t C>
    friend std::ostream &operator<<(std::ostream &os,
                                    const RaceHashing<A, B, C> &rh);

private:
    RaceHashingConfig conf_;
    uint32_t round_hash_to_depth(uint32_t h)
    {
        return h & ((1 << gd()) - 1);
    }
    size_t gd() const
    {
        return gd_.load(std::memory_order_relaxed);
    }

    // TODO: this expanding variable should be RDMA globally-visable
    std::array<std::atomic<bool>, kDEntryNr> expandings_{false};
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
        if (sub_table)
        {
            auto [ld, suffix] = sub_table->ld_suffix();
            os << "sub-table[" << i << "]: ld: " << ld << ", suf: " << suffix
               << ", util: " << sub_table->utilization() << ", at "
               << (void *) sub_table << ". " << *sub_table << std::endl;
        }
        else
        {
            os << "sub-table[" << i << "]: NULL " << std::endl;
        }
    }

    return os;
}

}  // namespace patronus::hash

#endif