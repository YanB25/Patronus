#pragma once
#ifndef PERTRONUS_RACEHASHING_HASHTABLE_HANDLE_H_
#define PERTRONUS_RACEHASHING_HASHTABLE_HANDLE_H_

#include <cinttypes>
#include <cstddef>
#include <cstring>

#include "./hashtable.h"
#include "./rdma.h"
#include "./utils.h"

namespace patronus::hash
{
struct RaceHashingHandleConfig
{
    bool auto_expand{false};
    bool auto_update_dir{false};
};
/**
 * @brief HashTableHandle is a handler for each client. Containing the cache of
 * GD and directory, and how to find the server-side hash table.
 *
 */
template <size_t kDEntryNr, size_t kBucketGroupNr, size_t kSlotNr>
class RaceHashingHandleImpl
{
public:
    using SubTableHandleT = SubTableHandle<kBucketGroupNr, kSlotNr>;
    using RaceHashingT = RaceHashing<kDEntryNr, kBucketGroupNr, kSlotNr>;
    using pointer = std::shared_ptr<RaceHashingHandleImpl>;

    using MetaT = RaceHashingMeta<kDEntryNr, kBucketGroupNr, kSlotNr>;

    RaceHashingHandleImpl(uint64_t table_meta_addr,
                          const RaceHashingHandleConfig &conf,
                          RaceHashingRdmaContext::pointer rdma_ctx)
        : table_meta_addr_(table_meta_addr),
          conf_(conf),
          rdma_ctx_(std::move(rdma_ctx))
    {
        update_directory_cache(nullptr);
    }
    constexpr static size_t meta_size()
    {
        return RaceHashingT::meta_size();
    }

    RetCode update_directory_cache(HashContext *dctx)
    {
        std::ignore = dctx;
        // TODO(race): this have performance problem because of additional
        // memcpy.
        auto *rdma_buf = (char *) rdma_ctx_->get_rdma_buffer(meta_size());
        CHECK_EQ(rdma_ctx_->rdma_read(table_meta_addr_, rdma_buf, meta_size()),
                 kOk);
        CHECK_EQ(rdma_ctx_->commit(), kOk);
        memcpy(&cached_meta_, rdma_buf, meta_size());
        LOG_IF(INFO, config::kEnableDebug)
            << "[race][trace] update_directory_cache: update cache to "
            << cached_meta_;
        return kOk;
    }

    template <size_t kA, size_t kB, size_t kC>
    friend std::ostream &operator<<(
        std::ostream &os, const RaceHashingHandleImpl<kA, kB, kC> &rhh);

    size_t gd() const
    {
        return cached_meta_.gd;
    }

    RetCode del(const Key &key, HashContext *dctx = nullptr)
    {
        auto hash = hash_impl(key.data(), key.size());
        auto m = hash_m(hash);
        auto fp = hash_fp(hash);
        auto rounded_m = round_to_bits(m, gd());
        auto h1 = hash_1(hash);
        auto h2 = hash_2(hash);

        DVLOG(3) << "[race] DEL key " << pre(key) << ", got hash "
                 << pre_hash(hash) << ", m: " << m << ", rounded to "
                 << rounded_m << " by cached_gd: " << gd()
                 << ". fp: " << pre_fp(fp);
        auto st = subtable_handle(rounded_m);

        // get the two combined buckets the same time
        auto cbs = st.get_two_combined_bucket_handle(h1, h2, *rdma_ctx_);
        CHECK_EQ(rdma_ctx_->commit(), kOk);

        std::unordered_set<SlotHandle> slot_handles;
        auto rc = cbs.locate(fp, cached_ld(rounded_m), m, slot_handles, dctx);
        if (rc == kCacheStale && conf_.auto_update_dir)
        {
            // update cache and retry
            CHECK_EQ(update_directory_cache(dctx), kOk);
            return del(key, dctx);
        }

        return remove_if_exists(slot_handles, key, dctx);
    }
    RetCode remove_if_exists(const std::unordered_set<SlotHandle> slot_handles,
                             const Key &key,
                             HashContext *dctx)
    {
        auto f = [this](const Key &key,
                        SlotHandle slot_handles,
                        KVBlockHandle kvblock_handle,
                        HashContext *dctx) {
            std::ignore = key;
            std::ignore = kvblock_handle;
            return do_remove(slot_handles, dctx);
        };
        return for_the_real_match_do(slot_handles, key, f, dctx);
    }

    RetCode put(const Key &key, const Value &value, HashContext *dctx = nullptr)
    {
        auto hash = hash_impl(key.data(), key.size());
        auto h1 = hash_1(hash);
        auto h2 = hash_2(hash);
        auto fp = hash_fp(hash);
        auto m = hash_m(hash);
        auto cached_gd = gd();
        auto rounded_m = round_to_bits(m, cached_gd);
        auto ld = cached_ld(rounded_m);

        auto rc = put_phase_one(key, value, hash, dctx);
        if (rc == kCacheStale && conf_.auto_update_dir)
        {
            CHECK_EQ(update_directory_cache(dctx), kOk);
            return put(key, value, dctx);
        }
        if (rc == kNoMem && conf_.auto_expand)
        {
            auto overflow_subtable_idx = round_to_bits(rounded_m, ld);
            CHECK_EQ(expand(overflow_subtable_idx, dctx), kOk);
            return put(key, value, dctx);
        }
        if (rc != kOk)
        {
            return rc;
        }
        rc = phase_two_deduplicate(key, hash, ld, dctx);
        CHECK_NE(rc, kNoMem);
        if constexpr (debug())
        {
            LOG_IF(INFO, kEnableDebug && dctx != nullptr && rc == kOk)
                << "[race][trace] PUT succeed: hash: " << pre_hash(hash)
                << ", h1: " << pre_hash(h1) << ", h2: " << pre_hash(h2)
                << ", fp: " << pre_hash(fp) << ", m: " << pre_hash(m)
                << ", put to subtable[" << rounded_m << "] by gd " << cached_gd
                << ". cached_ld: " << ld << ". " << *dctx;
        }
        return rc;
    }
    RetCode expand(size_t subtable_id, HashContext *dctx)
    {
        CHECK(false) << "TODO: " << subtable_id << ", " << dctx;
        return kInvalid;
    }
    RetCode phase_two_deduplicate(const Key &key,
                                  uint64_t hash,
                                  uint32_t cached_ld,
                                  HashContext *dctx)
    {
        auto m = hash_m(hash);
        auto rounded_m = round_to_bits(m, gd());
        auto h1 = hash_1(hash);
        auto h2 = hash_2(hash);
        auto fp = hash_fp(hash);

        auto st = subtable_handle(rounded_m);

        // get the two combined buckets the same time
        // validate staleness at the same time
        auto cbs = st.get_two_combined_bucket_handle(h1, h2, *rdma_ctx_);
        CHECK_EQ(rdma_ctx_->commit(), kOk);

        std::unordered_set<SlotHandle> slot_handles;
        auto rc = cbs.locate(fp, cached_ld, m, slot_handles, dctx);
        if (rc == kCacheStale && conf_.auto_update_dir)
        {
            // update cache and retry
            CHECK_EQ(update_directory_cache(dctx), kOk);
            return phase_two_deduplicate(key, hash, cached_ld, dctx);
        }

        if (slot_handles.size() >= 2)
        {
            std::unordered_set<SlotHandle> real_match_slot_handles;
            CHECK_EQ(get_real_match_slots(
                         slot_handles, key, real_match_slot_handles, dctx),
                     kOk);
            if (real_match_slot_handles.size() <= 1)
            {
                return kOk;
            }
            auto chosen_slot =
                deterministic_choose_slot(real_match_slot_handles);
            for (auto view : real_match_slot_handles)
            {
                if (view.remote_addr() != chosen_slot.remote_addr())
                {
                    auto rc = do_remove(view, dctx);
                    CHECK(rc == kOk || rc == kRetry);
                }
            }
        }
        return kOk;
    }

    RetCode do_remove(SlotHandle slot_handle, HashContext *dctx)
    {
        auto expect_slot = slot_handle.slot_view();
        uint64_t expect_val = expect_slot.val();
        auto desired_slot = slot_handle.view_after_clear();
        auto *rdma_buf = DCHECK_NOTNULL(rdma_ctx_->get_rdma_buffer(8));
        CHECK_EQ(rdma_ctx_->rdma_cas(slot_handle.remote_addr(),
                                     expect_val,
                                     desired_slot.val(),
                                     rdma_buf),
                 kOk);
        CHECK_EQ(rdma_ctx_->commit(), kOk);
        bool success = memcmp(rdma_buf, &expect_val, 8) == 0;
        if (success)
        {
            DLOG_IF(INFO, kEnableDebug && dctx != nullptr)
                << "[race][trace] do_remove SUCC: clearing slot " << slot_handle
                << ". kOk: " << *dctx;
            rdma_ctx_->remote_free(expect_slot.ptr());
            return kOk;
        }
        else
        {
            DLOG_IF(INFO, kEnableDebug && dctx != nullptr)
                << "[race][trace] do_remove FAILED: clearing slot "
                << slot_handle << ". kRetry: " << *dctx;
            return kRetry;
        }
    }

    SlotHandle deterministic_choose_slot(
        const std::unordered_set<SlotHandle> &views)
    {
        CHECK(!views.empty());
        return *std::min_element(views.begin(), views.end());
    }
    RetCode get_real_match_slots(
        const std::unordered_set<SlotHandle> &slot_handles,
        const Key &key,
        std::unordered_set<SlotHandle> real_matches,
        HashContext *dctx)
    {
        auto f = [&real_matches](const Key &key,
                                 SlotHandle slot,
                                 KVBlockHandle kvblock_handle,
                                 HashContext *dctx) {
            std::ignore = key;
            std::ignore = kvblock_handle;
            std::ignore = dctx;
            real_matches.insert(slot);
            return kOk;
        };
        return for_the_real_match_do(slot_handles, key, f, dctx);
    }
    RetCode put_phase_one(const Key &key,
                          const Value &value,
                          uint64_t hash,
                          HashContext *dctx)
    {
        RetCode rc = kOk;
        auto m = hash_m(hash);
        auto fp = hash_fp(hash);
        auto h1 = hash_1(hash);
        auto h2 = hash_2(hash);
        auto rounded_m = round_to_bits(m, gd());
        auto ld = cached_ld(rounded_m);
        DLOG_IF(INFO, config::kEnableDebug && dctx != nullptr)
            << "[race][trace] put_phase_one: got hash " << pre_hash(hash)
            << ", m: " << m << ", rounded to " << rounded_m
            << " (subtable) by gd(may stale): " << gd()
            << ". fp: " << pre_fp(fp) << ", cached_ld: " << ld;
        auto st = subtable_handle(rounded_m);

        uint64_t kv_block = 0;
        size_t len = 0;
        CHECK_EQ(parallel_write_kv(
                     key, value, hash, *rdma_ctx_, &kv_block, &len, dctx),
                 kOk);

        auto cbs = st.get_two_combined_bucket_handle(h1, h2, *rdma_ctx_);
        CHECK_EQ(rdma_ctx_->commit(), kOk);

        SlotView new_slot(fp, len, (char *) kv_block);
        DLOG_IF(INFO, config::kEnableMemoryDebug)
            << "[race][mem] new remote kv_block addr: " << (void *) kv_block
            << " with len: " << len
            << ". actual len: " << new_slot.actual_len_bytes();

        std::unordered_set<SlotHandle> slot_handles;
        rc = cbs.locate(fp, ld, m, slot_handles, dctx);
        DCHECK_NE(rc, kNotFound)
            << "Even not found should not response not found";
        if (rc != kOk)
        {
            goto handle_err;
        }

        rc = update_if_exists(slot_handles, key, new_slot, dctx);
        if (rc == kOk)
        {
            return rc;
        }
        else
        {
            DCHECK_EQ(rc, kNotFound);
            rc = insert_if_exist_empty_slot(
                cbs, cached_ld(rounded_m), m, new_slot, dctx);
        }
        if (rc == kOk)
        {
            return rc;
        }

    handle_err:
        DCHECK_NE(rc, kOk);
        rdma_ctx_->remote_free((char *) kv_block);
        if (rc == kCacheStale && conf_.auto_update_dir)
        {
            // update cache and retry
            CHECK_EQ(update_directory_cache(dctx), kOk);
            return put(key, value, dctx);
        }
        return rc;
    }
    RetCode insert_if_exist_empty_slot(
        const TwoCombinedBucketHandle<kSlotNr> &cb,
        uint32_t ld,
        uint32_t suffix,
        SlotView new_slot,
        HashContext *dctx)
    {
        std::vector<BucketHandle<kSlotNr>> buckets;
        buckets.reserve(4);
        CHECK_EQ(cb.get_bucket_handle(buckets), kOk);
        for (const auto &bucket : buckets)
        {
            RetCode rc;
            if ((rc = bucket.validate_staleness(ld, suffix, dctx)) != kOk)
            {
                DCHECK_EQ(rc, kCacheStale);
                return rc;
            }
            auto poll_slot_idx = fast_pseudo_rand_int(1, kSlotNr - 1);
            constexpr auto kDataSlotNr = Bucket<kSlotNr>::kDataSlotNr;
            for (size_t i = 0; i < kDataSlotNr; ++i)
            {
                auto idx = (poll_slot_idx + i) % kDataSlotNr + 1;
                DCHECK_GE(idx, 1);
                DCHECK_LT(idx, kSlotNr);
                auto view = bucket.slot_view(idx);
                if (view.empty())
                {
                    auto rc =
                        do_insert(bucket.slot_handle(idx), new_slot, dctx);
                    if (rc == kOk)
                    {
                        return kOk;
                    }
                    DCHECK_EQ(rc, kRetry);
                }
            }
        }
        return kNoMem;
    }
    RetCode do_insert(SlotHandle slot_handle,
                      SlotView new_slot,
                      HashContext *dctx)
    {
        uint64_t expect_val = slot_handle.val();
        auto *rdma_buf = DCHECK_NOTNULL(rdma_ctx_->get_rdma_buffer(8));
        CHECK_EQ(rdma_ctx_->rdma_cas(slot_handle.remote_addr(),
                                     expect_val,
                                     new_slot.val(),
                                     rdma_buf),
                 kOk);
        CHECK_EQ(rdma_ctx_->commit(), kOk);
        bool success = memcmp(rdma_buf, &expect_val, 8) == 0;

        SlotView expect_slot(expect_val);
        if (success)
        {
            CHECK(expect_slot.empty())
                << "[trace][handle] the succeess of insert should happen on an "
                   "empty slot";
            DLOG_IF(INFO, kEnableDebug && dctx != nullptr)
                << "[race][handle] slot " << slot_handle << " update to "
                << new_slot << *dctx;
            return kOk;
        }
        DVLOG(4) << "[race][handle] do_update FAILED: new_slot " << new_slot;
        DLOG_IF(INFO, kEnableDebug && dctx != nullptr)
            << "[race][trace][handle] do_update FAILED: cas failed. slot "
            << slot_handle << *dctx;
        return kRetry;
    }
    void *patronus_alloc(size_t size)
    {
        return malloc(size);
    }
    RetCode parallel_write_kv(const Key &key,
                              const Value &value,
                              uint64_t hash,
                              RaceHashingRdmaContext &rdma_ctx,
                              uint64_t *remote_kvblock_addr,
                              size_t *len,
                              HashContext *dctx)
    {
        size_t kvblock_size = sizeof(KVBlock) + key.size() + value.size();
        size_t tag_ptr_len = len_to_ptr_len(kvblock_size);
        kvblock_size = ptr_len_to_len(tag_ptr_len);

        auto *rdma_buf = rdma_ctx.get_rdma_buffer(kvblock_size);
        auto &kv_block = *(KVBlock *) rdma_buf;
        kv_block.hash = hash;
        kv_block.key_len = key.size();
        kv_block.value_len = value.size();
        memcpy(kv_block.buf, key.data(), key.size());
        memcpy(kv_block.buf + key.size(), value.data(), value.size());

        auto *remote_buf = rdma_ctx.remote_alloc(kvblock_size);
        CHECK_EQ(rdma_ctx.rdma_write(
                     (uint64_t) remote_buf, (char *) rdma_buf, kvblock_size),
                 kOk);
        (*remote_kvblock_addr) = (uint64_t) remote_buf;
        (*len) = tag_ptr_len;

        DLOG_IF(INFO, kEnableDebug && dctx != nullptr)
            << "[race][trace] parallel_write_kv: allocate kv_block: "
            << (void *) remote_kvblock_addr
            << ", allocated_size: " << kvblock_size
            << ", ptr.len: " << tag_ptr_len << ". " << *dctx;

        return kOk;
    }
    using ApplyF = std::function<RetCode(
        const Key &, SlotHandle, KVBlockHandle, HashContext *)>;

    RetCode for_the_real_match_do(
        const std::unordered_set<SlotHandle> &slot_handles,
        const Key &key,
        const ApplyF &func,
        HashContext *dctx)
    {
        std::map<SlotHandle, KVBlockHandle> slots_rdma_buffers;
        for (auto slot_handle : slot_handles)
        {
            size_t actual_size = slot_handle.slot_view().actual_len_bytes();
            DCHECK_GT(actual_size, 0)
                << "make no sense to have actual size == 0. slot_handle: "
                << slot_handle;
            auto *rdma_buffer = rdma_ctx_->get_rdma_buffer(actual_size);
            // TODO(race): the high 16 bits is zero.
            // If using GlobalAddress, remember to fill the nodeID here.
            auto remote_kvblock_addr = (uint64_t) slot_handle.ptr();
            DLOG_IF(INFO, config::kEnableMemoryDebug)
                << "[race][mem] Reading remote kvblock addr: "
                << (void *) remote_kvblock_addr << " with size " << actual_size
                << ", fp: " << pre_fp(slot_handle.fp());
            CHECK_EQ(
                rdma_ctx_->rdma_read(
                    remote_kvblock_addr, (char *) rdma_buffer, actual_size),
                kOk);
            slots_rdma_buffers.emplace(
                slot_handle,
                KVBlockHandle(remote_kvblock_addr, (KVBlock *) rdma_buffer));
        }
        CHECK_EQ(rdma_ctx_->commit(), kOk);

        RetCode rc = kNotFound;
        for (const auto &[slot_handle, kvblock_handle] : slots_rdma_buffers)
        {
            if (is_real_match(kvblock_handle.buffer_addr(), key, dctx) != kOk)
            {
                continue;
            }
            // okay, it is the real match
            rc = func(key, slot_handle, kvblock_handle, dctx);
            CHECK_NE(rc, kNotFound)
                << "Make no sense to return kNotFound: already found for you.";
        }
        return rc;
    }

    RetCode get(const Key &key, Value &value, HashContext *dctx = nullptr)
    {
        auto hash = hash_impl(key.data(), key.size());
        auto m = hash_m(hash);
        auto fp = hash_fp(hash);
        auto cached_gd = gd();
        auto rounded_m = round_to_bits(m, cached_gd);
        auto h1 = hash_1(hash);
        auto h2 = hash_2(hash);

        DVLOG(3) << "[race] GET key " << pre(key) << ", got hash "
                 << pre_hash(hash) << ", m: " << m << ", rounded to "
                 << rounded_m << " by cached_gd: " << cached_gd
                 << ". fp: " << pre_fp(fp);
        auto st = subtable_handle(rounded_m);

        // get the two combined buckets the same time
        // validate staleness at the same time
        auto cbs = st.get_two_combined_bucket_handle(h1, h2, *rdma_ctx_);
        CHECK_EQ(rdma_ctx_->commit(), kOk);

        std::unordered_set<SlotHandle> slot_handles;
        auto rc = cbs.locate(fp, cached_ld(rounded_m), m, slot_handles, dctx);
        if (rc == kCacheStale && conf_.auto_update_dir)
        {
            // update cache and retry
            CHECK_EQ(update_directory_cache(dctx), kOk);
            return get(key, value, dctx);
        }
        DLOG_IF(INFO, kEnableDebug && dctx != nullptr)
            << "[race][trace] GET from subtable[" << rounded_m << "] from hash "
            << pre_hash(hash) << " and gd " << cached_gd
            << ". Possible match nr: " << slot_handles.size() << ". " << *dctx;

        return get_from_slot_views(slot_handles, key, value, dctx);
    }
    RetCode get_from_slot_views(
        const std::unordered_set<SlotHandle> &slot_handles,
        const Key &key,
        Value &value,
        HashContext *dctx)
    {
        auto f = [&value](const Key &key,
                          SlotHandle slot_handle,
                          KVBlockHandle kvblock_handle,
                          HashContext *dctx) {
            std::ignore = key;
            std::ignore = slot_handle;

            DLOG_IF(INFO, kEnableDebug && dctx != nullptr)
                << "[race][trace] get_from_slot_views SUCC: slot_handle "
                << slot_handle << ". " << *dctx;
            value.resize(kvblock_handle.value_len());
            memcpy(value.data(),
                   (char *) kvblock_handle.buf() + kvblock_handle.key_len(),
                   kvblock_handle.value_len());
            return kOk;
        };

        return for_the_real_match_do(slot_handles, key, f, dctx);
    }

    RetCode update_if_exists(const std::unordered_set<SlotHandle> &slot_handles,
                             const Key &key,
                             SlotView new_slot,
                             HashContext *dctx)
    {
        auto f = [&rdma_ctx = *rdma_ctx_.get(), new_slot](
                     const Key &key,
                     SlotHandle slot_handle,
                     KVBlockHandle kvblock_handle,
                     HashContext *dctx) {
            std::ignore = key;

            uint64_t expect_val = slot_handle.val();
            auto *rdma_buf = DCHECK_NOTNULL(rdma_ctx.get_rdma_buffer(8));
            CHECK_EQ(rdma_ctx.rdma_cas(slot_handle.remote_addr(),
                                       expect_val,
                                       new_slot.val(),
                                       rdma_buf),
                     kOk);
            CHECK_EQ(rdma_ctx.commit(), kOk);
            bool success = memcmp(rdma_buf, (char *) &expect_val, 8) == 0;
            expect_val = *(uint64_t *) rdma_buf;
            SlotView expect_slot(expect_val);
            if (success)
            {
                if (expect_slot.empty())
                {
                    DVLOG(4) << "[race][subtable] do_update SUCC: update into "
                                "an empty slot. New_slot "
                             << new_slot;
                }
                else
                {
                    DVLOG(4) << "[race][subtable] do_update SUCC: for slot "
                                "with kvblock_handle:"
                             << kvblock_handle << ". New_slot " << new_slot;
                    rdma_ctx.remote_free(expect_slot.ptr());
                }
                DLOG_IF(INFO, kEnableDebug && dctx != nullptr)
                    << "[race][subtable] slot " << slot_handle << " update to "
                    << new_slot << *dctx;
                return kOk;
            }
            DVLOG(4) << "[race][subtable] do_update FAILED: new_slot "
                     << new_slot;
            DLOG_IF(INFO, kEnableDebug && dctx != nullptr)
                << "[race][trace][subtable] do_update FAILED: cas failed. slot "
                << slot_handle << *dctx;
            return kRetry;
        };

        return for_the_real_match_do(slot_handles, key, f, dctx);
    }

    RetCode is_real_match(KVBlock *kvblock, const Key &key, HashContext *dctx)
    {
        std::ignore = dctx;
        if (kvblock->key_len != key.size())
        {
            // DVLOG(4) << "[race][subtable] slot_real_match FAILED: key "
            //          << pre(key) << " miss: key len mismatch";
            DLOG_IF(INFO, kEnableDebug && dctx != nullptr)
                << "[race][stable] is_real_match FAILED: key len " << key.size()
                << " mismatch with block->key_len: " << kvblock->key_len << ". "
                << *dctx;
            return kNotFound;
        }
        if (memcmp(key.data(), kvblock->buf, key.size()) != 0)
        {
            // DVLOG(4) << "[race][subtable] slot_real_match FAILED: key "
            //          << pre(key) << " miss: key content mismatch";
            DLOG_IF(INFO, kEnableDebug && dctx != nullptr)
                << "[race][stable] is_real_match FAILED: key content mismatch. "
                << *dctx;
            return kNotFound;
        }
        // DVLOG(4) << "[race][subtable] slot_real_match SUCCEED: key "
        //          << pre(key);
        DLOG_IF(INFO, kEnableDebug && dctx != nullptr)
            << "[race][stable] is_real_match SUCC. " << *dctx;
        return kOk;
    }

private:
    // the address of hash table at remote side
    uint64_t table_meta_addr_;
    MetaT cached_meta_;
    RaceHashingHandleConfig conf_;
    RaceHashingRdmaContext::pointer rdma_ctx_;

    size_t cached_gd() const
    {
        return cached_meta_.gd;
    }
    size_t cached_subtable_nr() const
    {
        return pow(2, cached_gd());
    }
    SubTableHandleT subtable_handle(size_t idx)
    {
        SubTableHandleT st(cached_ld(idx),
                           subtable_addr(idx),
                           SubTableHandleT::size_bytes(),
                           idx);
        return st;
    }
    uint32_t cached_ld(size_t idx)
    {
        return cached_meta_.lds[idx];
    }
    uint64_t subtable_addr(size_t idx)
    {
        return (uint64_t) cached_meta_.entries[idx];
    }
};

template <size_t kDEntryNr, size_t kBucketGroupNr, size_t kSlotNr>
inline std::ostream &operator<<(
    std::ostream &os,
    const RaceHashingHandleImpl<kDEntryNr, kBucketGroupNr, kSlotNr> &rhh)
{
    os << "RaceHashingHandleImpl gd: " << rhh.cached_gd() << std::endl;
    for (size_t i = 0; i < rhh.cached_subtable_nr(); ++i)
    {
        os << "sub-table[" << i << "]: ld: " << rhh.cached_meta_.lds[i]
           << ", at " << rhh.cached_meta_.entries[i] << std::endl;
    }
    return os;
}

}  // namespace patronus::hash

#endif