#pragma once
#ifndef PERTRONUS_RACEHASHING_SUBTABLE_H_
#define PERTRONUS_RACEHASHING_SUBTABLE_H_

#include <cstddef>
#include <cstdint>

#include "./bucket_group.h"
#include "./kv_block.h"
#include "util/PerformanceReporter.h"

namespace patronus::hash
{
constexpr static bool kEnableDebug = config::kEnableRaceHashingDebug;
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
        update_header(ld, hash_suffix);
    }
    uint32_t ld() const
    {
        return bucket_group(0).bucket(0).header().ld;
    }
    uint32_t suffix() const
    {
        return bucket_group(0).bucket(0).header().suffix;
    }
    std::pair<uint32_t, uint32_t> ld_suffix() const
    {
        const auto &h = bucket_group(0).bucket(0).header();
        return {h.ld, h.suffix};
    }
    void update_header(uint32_t ld, uint32_t suffix)
    {
        for (size_t b = 0; b < kBucketGroupNr; ++b)
        {
            auto bg = bucket_group(b);
            bg.setup_header(ld, suffix);
        }
    }
    constexpr static size_t max_item_nr()
    {
        return kBucketGroupNr * BucketGroup<kSlotNr>::max_item_nr();
    }
    std::vector<MigrateView> locate_migratable(size_t test_bit,
                                               HashContext *dctx)
    {
        std::vector<MigrateView> ret;
        // the memory is contineous by design.
        for (size_t bgid = 0; bgid < kBucketGroupNr; bgid++)
        {
            auto bg = bucket_group(bgid);
            auto m0 = bg.main_bucket_0();
            auto m1 = bg.main_bucket_1();
            auto o = bg.overflow_bucket();
            for (auto buck : {m0, m1, o})
            {
                // sid == 0 is the header
                for (size_t sid = 1; sid < kSlotNr; ++sid)
                {
                    auto &slot = buck.slot(sid);
                    auto *kv_block = (KVBlock *) slot.view().ptr();
                    if (kv_block != nullptr)
                    {
                        auto hash = kv_block->hash;
                        bool hit = (hash & (1ull << test_bit));
                        // DLOG_IF(INFO, kEnableDebug && dctx != nullptr)
                        //     << "[debug][race][expand] bgid: " << bgid
                        //     << ", sid: " << sid
                        //     << ", at slot: " << (void *) &slot << ". key: `"
                        //     << std::string(kv_block->buf, kv_block->key_len)
                        //     << "`. hash: " << std::hex << kv_block->hash
                        //     << std::dec << ". test-int: " << (1ull <<
                        //     test_bit)
                        //     << ", hit: " << hit << ". " << *dctx;
                        std::ignore = dctx;
                        if (hit)
                        {
                            DVLOG(2) << "[race][expand] locate_migratable: the "
                                     << sid << "-th slot got hash " << std::hex
                                     << hash << std::dec << ", with "
                                     << test_bit << " set. Should be migrated";
                            // ret.push_back(MigrateView(slot_addr, view));
                            ret.emplace_back(
                                &slot, slot.view(), kv_block->hash);
                        }
                    }
                }
            }
        }
        return ret;
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
        auto slot_views_1 = cb1.locate(fp, dctx);
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
        auto slot_views_2 = cb2.locate(fp, dctx);
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

    RetCode do_find_empty_slot_to_insert(const CombinedBucketView<kSlotNr> &cb1,
                                         const CombinedBucketView<kSlotNr> &cb2,
                                         SlotView new_slot,
                                         HashContext *dctx)
    {
        DVLOG(4) << "[race][subtable] do_find_empty_slot_to_insert: new_slot "
                 << new_slot << ". Try to fetch empty slots.";
        auto empty_1 = cb1.fetch_empty(10);
        DLOG_IF(INFO, kEnableDebug && dctx != nullptr && empty_1.empty())
            << "[race][trace] do_find_empty_slot_to_insert WARN: "
               "cb1.fetch_empty() got no empty slot. "
            << *dctx;
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
        DLOG_IF(INFO, kEnableDebug && dctx != nullptr && empty_2.empty())
            << "[race][trace] do_find_empty_slot_to_insert WARN: "
               "cb2.fetch_empty() got no empty slot. "
            << *dctx;
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
    RetCode do_remove(SlotWithView view, HashContext *dctx)
    {
        auto expect_slot = view.view();
        auto desired_slot = view.view_after_clear();
        if (view.cas(expect_slot, desired_slot))
        {
            DLOG_IF(INFO, kEnableDebug && dctx != nullptr)
                << "[race][trace] do_remove: clearing slot "
                << (void *) view.slot() << ". " << *dctx;
            hash_table_free(expect_slot.ptr());
            return kOk;
        }
        else
        {
            return kRetry;
        }
    }

private:
    constexpr static size_t kItemSize = BucketGroup<kSlotNr>::size_bytes();
    BucketGroup<kSlotNr> *addr_{nullptr};

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

        auto *kv_block = KVBlock::new_instance(key, value, hash);

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
        DLOG_IF(INFO, kEnableDebug && dctx != nullptr)
            << "[race][trace][subtable] do_update_if_empty: FAILED. slot not "
               "empty. "
               "slot_with_view: "
            << view << *dctx;
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
            DLOG_IF(INFO, kEnableDebug && dctx != nullptr)
                << "[race][subtable] slot " << (void *) view.slot()
                << " update to " << new_slot << *dctx;
            return kOk;
        }
        DVLOG(4) << "[race][subtable] do_update FAILED: new_slot " << new_slot;
        DLOG_IF(INFO, kEnableDebug && dctx != nullptr)
            << "[race][trace][subtable] do_update FAILED: cas failed. slot "
            << view << *dctx;
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
}  // namespace patronus::hash

#endif