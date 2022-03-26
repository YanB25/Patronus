#pragma once
#ifndef PERTRONUS_RACEHASHING_HASHTABLE_HANDLE_H_
#define PERTRONUS_RACEHASHING_HASHTABLE_HANDLE_H_

#include <cinttypes>
#include <cstddef>
#include <cstring>

#include "./hashtable.h"
#include "./mock_rdma_adaptor.h"
#include "./utils.h"
#include "Common.h"

using namespace define::literals;
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
    using SubTableT = SubTable<kBucketGroupNr, kSlotNr>;
    using RaceHashingT = RaceHashing<kDEntryNr, kBucketGroupNr, kSlotNr>;
    using pointer = std::shared_ptr<RaceHashingHandleImpl>;

    using MetaT = RaceHashingMeta<kDEntryNr, kBucketGroupNr, kSlotNr>;

    RaceHashingHandleImpl(uint16_t node_id,
                          GlobalAddress table_meta_addr,
                          const RaceHashingHandleConfig &conf,
                          IRdmaAdaptor::pointer rdma_ctx,
                          CoroContext *coro_ctx)
        : node_id_(node_id),
          table_meta_addr_(table_meta_addr),
          conf_(conf),
          rdma_ctx_(rdma_ctx),
          coro_ctx_(coro_ctx)
    {
    }
    constexpr static size_t meta_size()
    {
        return RaceHashingT::meta_size();
    }
    void init(HashContext *dctx = nullptr)
    {
        init_directory_mem_handle();
        update_directory_cache(dctx);
        // init_kvblock_mem_handle AFTER getting the directory cache
        // because we need to know where server places the kvblock pool
        init_kvblock_mem_handle();
        inited_ = true;
    }

    RetCode update_directory_cache(HashContext *dctx)
    {
        // TODO(race): this have performance problem because of additional
        // memcpy.
        auto *rdma_buf = (char *) rdma_ctx_->get_rdma_buffer(meta_size());
        auto &dir_mem_handle = get_directory_mem_handle();
        auto rc = rdma_ctx_->rdma_read(
            rdma_buf, table_meta_addr_, meta_size(), dir_mem_handle, coro_ctx_);
        CHECK_EQ(rc, kOk);

        CHECK_EQ(rdma_ctx_->commit(coro_ctx_), kOk);
        memcpy(&cached_meta_, rdma_buf, meta_size());

        LOG_IF(INFO, config::kEnableDebug)
            << "[race][trace] update_directory_cache: update cache to "
            << cached_meta_ << ". ctx: " << pre_coro_ctx(coro_ctx_) << ". "
            << pre_dctx(dctx);
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
        DCHECK(inited_);
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
        auto subtable_idx = rounded_m;
        auto st = subtable_handle(subtable_idx);

        // get the two combined buckets the same time
        auto cbs = st.get_two_combined_bucket_handle(h1, h2, *rdma_ctx_);
        CHECK_EQ(rdma_ctx_->commit(), kOk);

        std::unordered_set<SlotHandle> slot_handles;
        auto rc =
            cbs.locate(fp, cached_ld(subtable_idx), m, slot_handles, dctx);
        if (rc == kCacheStale && conf_.auto_update_dir)
        {
            // update cache and retry
            CHECK_EQ(update_directory_cache(dctx), kOk);
            return del(key, dctx);
        }

        rc = remove_if_exists(subtable_idx, slot_handles, key, dctx);
        if (rc == kOk)
        {
            DLOG_IF(INFO, config::kEnableDebug && dctx != nullptr)
                << "[race][trace] del: rm at subtable[" << subtable_idx << "]. "
                << *dctx;
        }
        else
        {
            DLOG_IF(INFO, config::kEnableDebug && dctx != nullptr)
                << "[race][trace] del: not found at subtable[" << subtable_idx
                << "]. " << *dctx;
        }
        return rc;
    }
    RetCode remove_if_exists(size_t subtable_idx,
                             const std::unordered_set<SlotHandle> slot_handles,
                             const Key &key,
                             HashContext *dctx)
    {
        auto f = [this, subtable_idx](const Key &key,
                                      SlotHandle slot_handles,
                                      KVBlockHandle kvblock_handle,
                                      HashContext *dctx) {
            std::ignore = key;
            std::ignore = kvblock_handle;
            return do_remove(subtable_idx, slot_handles, dctx);
        };
        return for_the_real_match_do(slot_handles, key, f, dctx);
    }

    RetCode put(const Key &key, const Value &value, HashContext *dctx = nullptr)
    {
        DCHECK(inited_);

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
            auto rc = expand(overflow_subtable_idx, dctx);
            if (rc == kNoMem || rc == kRetry)
            {
                return rc;
            }
            CHECK_EQ(rc, kOk);
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
            LOG_IF(INFO, config::kEnableDebug && dctx != nullptr && rc == kOk)
                << "[race][trace] PUT succeed: hash: " << pre_hash(hash)
                << ", h1: " << pre_hash(h1) << ", h2: " << pre_hash(h2)
                << ", fp: " << pre_hash(fp) << ", m: " << pre_hash(m)
                << ", put to subtable[" << rounded_m << "] by gd " << cached_gd
                << ". cached_ld: " << ld << ". " << *dctx;
        }
        return rc;
    }
    RetCode expand_try_lock_subtable_drain(size_t subtable_idx)
    {
        auto *rdma_buf = DCHECK_NOTNULL(rdma_ctx_->get_rdma_buffer(8));
        auto remote = lock_remote_addr(subtable_idx);
        uint64_t expect = 0;   // no lock
        uint64_t desired = 1;  // lock
        auto &dir_mem_handle = get_directory_mem_handle();
        CHECK_EQ(rdma_ctx_->rdma_cas(
                     remote, expect, desired, rdma_buf, dir_mem_handle),
                 kOk);
        CHECK_EQ(rdma_ctx_->commit(), kOk);
        uint64_t r = *(uint64_t *) rdma_buf;
        CHECK(r == 0 || r == 1)
            << "Unexpected value read from lock of subtable[" << subtable_idx
            << "]. Expect 0 or 1, got " << r;
        if (r == 0)
        {
            DCHECK_EQ(desired, 1);
            cached_meta_.expanding[subtable_idx] = desired;
            return kOk;
        }
        return kRetry;
    }
    GlobalAddress lock_remote_addr(size_t subtable_idx) const
    {
        DCHECK_LT(subtable_idx, kDEntryNr);
        auto offset = offsetof(MetaT, expanding);
        offset += subtable_idx * sizeof(uint64_t);
        return table_meta_addr_ + offset;
    }
    RetCode expand_unlock_subtable_nodrain(size_t subtable_idx)
    {
        auto *rdma_buf = DCHECK_NOTNULL(rdma_ctx_->get_rdma_buffer(8));
        auto remote = lock_remote_addr(subtable_idx);
        *(uint64_t *) rdma_buf = 0;  // no lock
        auto &dir_mem_handle = get_directory_mem_handle();
        return rdma_ctx_->rdma_write(
            remote, (char *) rdma_buf, 8, dir_mem_handle);
    }
    RetCode expand_write_entry_nodrain(size_t subtable_idx,
                                       GlobalAddress subtable_remote_addr)
    {
        auto entry_size = sizeof(subtable_remote_addr);
        auto *rdma_buf = DCHECK_NOTNULL(rdma_ctx_->get_rdma_buffer(entry_size));
        *(uint64_t *) rdma_buf = subtable_remote_addr.val;
        auto remote = entries_remote_addr(subtable_idx);
        auto &dir_mem_handle = get_directory_mem_handle();
        return rdma_ctx_->rdma_write(
            remote, (char *) rdma_buf, entry_size, dir_mem_handle);
    }
    GlobalAddress entries_remote_addr(size_t subtable_idx) const
    {
        DCHECK_LT(subtable_idx, kDEntryNr)
            << "make no sense to ask for overflowed addr";
        auto offset = offsetof(MetaT, entries);
        offset += subtable_idx * sizeof(SubTableT *);
        return table_meta_addr_ + offset;
    }
    GlobalAddress ld_remote_addr(size_t subtable_idx) const
    {
        DCHECK_LT(subtable_idx, kDEntryNr);
        auto offset = offsetof(MetaT, lds);
        offset += subtable_idx * sizeof(uint32_t);
        return table_meta_addr_ + offset;
    }
    RetCode expand_update_ld_nodrain(size_t subtable_idx, uint32_t ld)
    {
        // just uint32_t
        // I am afraid that I will change the type of ld and ruins everything
        DCHECK_LT(subtable_idx, kDEntryNr);
        using ld_t =
            typename std::remove_reference<decltype(cached_meta_.lds[0])>::type;
        auto entry_size = sizeof(ld_t);
        auto *rdma_buf = DCHECK_NOTNULL(rdma_ctx_->get_rdma_buffer(entry_size));
        *(ld_t *) rdma_buf = ld;
        auto remote = ld_remote_addr(subtable_idx);
        auto &dir_mem_handle = get_directory_mem_handle();
        return rdma_ctx_->rdma_write(
            remote, (char *) rdma_buf, entry_size, dir_mem_handle);
    }
    GlobalAddress gd_remote_addr() const
    {
        return table_meta_addr_ + offsetof(MetaT, gd);
    }
    RetCode expand_cas_gd_drain(uint64_t expect, uint64_t desired)
    {
        auto *rdma_buf = DCHECK_NOTNULL(rdma_ctx_->get_rdma_buffer(8));
        auto remote = gd_remote_addr();
        auto &dir_mem_handle = get_directory_mem_handle();
        CHECK_EQ(rdma_ctx_->rdma_cas(
                     remote, expect, desired, rdma_buf, dir_mem_handle),
                 kOk);
        CHECK_EQ(rdma_ctx_->commit(), kOk);
        uint64_t r = *(uint64_t *) rdma_buf;
        DCHECK_LE(r, log2(kDEntryNr))
            << "The old gd should not larger than log2(entries_nr)";
        bool success = r == expect;
        if (success)
        {
            DCHECK_EQ(cached_meta_.gd, expect);
            cached_meta_.gd = desired;
            return kOk;
        }
        return kRetry;
    }
    RetCode expand_update_remote_bucket_header_drain(
        SubTableHandleT &subtable_handle,
        uint32_t ld,
        uint32_t suffix,
        HashContext *dctx)
    {
        auto rc = subtable_handle.update_bucket_header_nodrain(
            ld, suffix, *rdma_ctx_, dctx);
        if (rc != kOk)
        {
            return rc;
        }
        return rdma_ctx_->commit();
    }
    RetCode expand_init_and_update_remote_bucket_header_drain(
        SubTableHandleT &subtable_handle,
        uint32_t ld,
        uint32_t suffix,
        HashContext *dctx)
    {
        // just a wrapper, this parameters to the ctor is not used.
        auto rc = subtable_handle.init_and_update_bucket_header_drain(
            ld, suffix, *rdma_ctx_, dctx);
        if (rc != kOk)
        {
            return rc;
        }
        return rdma_ctx_->commit();
    }
    RetCode expand_install_subtable_nodrain(size_t subtable_idx,
                                            GlobalAddress new_remote_subtable)
    {
        auto size = sizeof(new_remote_subtable.val);
        auto *rdma_buf = rdma_ctx_->get_rdma_buffer(size);
        *(uint64_t *) rdma_buf = new_remote_subtable.val;
        auto remote = entries_remote_addr(subtable_idx);
        auto &dir_mem_handle = get_directory_mem_handle();
        return rdma_ctx_->rdma_write(
            remote, (char *) rdma_buf, size, dir_mem_handle);
    }
    /**
     * pre-condition: the subtable of dst_staddr is all empty.
     */
    RetCode expand_migrate_subtable(SubTableHandleT &src_st_handle,
                                    SubTableHandleT &dst_st_handle,
                                    size_t bit,
                                    HashContext *dctx)
    {
        std::unordered_set<SlotMigrateHandle> should_migrate;
        auto st_size = SubTableT::size_bytes();
        auto *rdma_buf = DCHECK_NOTNULL(rdma_ctx_->get_rdma_buffer(st_size));
        CHECK_EQ(rdma_ctx_->rdma_read(rdma_buf,
                                      src_st_handle.gaddr(),
                                      st_size,
                                      src_st_handle.mem_handle()),
                 kOk);
        CHECK_EQ(rdma_ctx_->commit(), kOk);

        auto &kvblock_mem_handle = get_kvblock_mem_handle();
        for (size_t i = 0; i < SubTableHandleT::kTotalBucketNr; ++i)
        {
            auto remote_bucket_addr = GlobalAddress(
                src_st_handle.gaddr() + i * Bucket<kSlotNr>::size_bytes());
            void *bucket_buffer_addr =
                (char *) rdma_buf + i * Bucket<kSlotNr>::size_bytes();
            BucketHandle<kSlotNr> b(remote_bucket_addr,
                                    (char *) bucket_buffer_addr);
            CHECK_EQ(
                b.should_migrate(
                    bit, *rdma_ctx_, kvblock_mem_handle, should_migrate, dctx),
                kOk);
        }

        // TODO: rethink about how batching can boost performance
        for (auto slot_handle : should_migrate)
        {
            SlotHandle ret_slot(nullgaddr, SlotView(0));
            CHECK_EQ(dst_st_handle.put_slot(
                         slot_handle, *rdma_ctx_, &ret_slot, dctx),
                     kOk);
            auto rc = src_st_handle.try_del_slot(slot_handle.slot_handle(),
                                                 *rdma_ctx_);
            if (rc != kOk)
            {
                DCHECK(!ret_slot.ptr().is_null());
                dst_st_handle.del_slot(ret_slot, *rdma_ctx_);
            }
        }

        return kOk;
    }
    // TODO: expand still has lots of problem
    // When trying to expand to dst subtable, the concurrent clients can insert
    // lots of items to make it full. Therefore cascaded expansion is required.
    // The client can always find out cache stale and update their directory
    // cache.
    RetCode expand(size_t subtable_idx, HashContext *dctx)
    {
        DCHECK(inited_);
        CHECK_LT(subtable_idx, kDEntryNr);
        // 0) try lock
        auto rc = expand_try_lock_subtable_drain(subtable_idx);
        if (rc != kOk)
        {
            return rc;
        }
        // 1) I think updating the directory cache is definitely necessary
        // while expanding.
        update_directory_cache(dctx);
        auto depth = cached_meta_.lds[subtable_idx];
        auto next_subtable_idx = subtable_idx | (1 << depth);
        // DLOG_IF(INFO, config::kEnableExpandDebug && dctx != nullptr)
        //     << "[race][expand] Expanding subtable[" << subtable_idx
        //     << "] to subtable[" << next_subtable_idx
        //     << "]. meta: " << cached_meta_;
        auto subtable_remote_addr = cached_meta_.entries[subtable_idx];

        // okay, entered critical section
        auto next_depth = depth + 1;
        auto expect_gd = gd();
        if (pow(2, next_depth) > kDEntryNr)
        {
            DLOG(WARNING) << "[race][expand] (1) trying to expand to gd "
                          << next_depth << " with entries "
                          << pow(2, next_depth) << ". Out of directory entry.";
            CHECK_EQ(expand_unlock_subtable_nodrain(subtable_idx), kOk);
            CHECK_EQ(rdma_ctx_->commit(), kOk);
            DCHECK_EQ(cached_meta_.expanding[subtable_idx], 1);
            cached_meta_.expanding[subtable_idx] = 0;
            return kNoMem;
        }
        // 2) Expand the directory first
        if (next_depth >= expect_gd)
        {
            // insert all the entries into the directory
            // before updating gd
            for (size_t i = 0; i < pow(2, next_depth); ++i)
            {
                if (cached_meta_.entries[i].is_null())
                {
                    auto from_subtable_idx = i & (~(1 << depth));
                    DLOG_IF(INFO, config::kEnableExpandDebug && dctx != nullptr)
                        << "[race][expand] (2) Update gd and directory: "
                           "setting subtable["
                        << i << "] to subtale[" << from_subtable_idx << "]. "
                        << *dctx;
                    auto subtable_remote_addr =
                        cached_meta_.entries[from_subtable_idx];
                    auto ld = cached_ld(from_subtable_idx);
                    CHECK_EQ(
                        expand_write_entry_nodrain(i, subtable_remote_addr),
                        kOk);
                    CHECK_EQ(expand_update_ld_nodrain(i, ld), kOk);
                    // TODO(race): update cache here. Not sure if it is right
                    cached_meta_.entries[i] = subtable_remote_addr;
                    cached_meta_.lds[i] = ld;
                }
            }
            CHECK_EQ(rdma_ctx_->commit(), kOk);

            DLOG_IF(INFO, config::kEnableExpandDebug && dctx != nullptr)
                << "[race][expand] (2) Update gd and directory: setting gd to "
                << next_depth;
            CHECK_EQ(expand_cas_gd_drain(expect_gd, next_depth), kOk);
        }

        CHECK_LT(next_subtable_idx, kDEntryNr);

        // do some debug checks
        auto cached_meta_next_subtable =
            cached_meta_.entries[next_subtable_idx];
        DCHECK_EQ(cached_meta_next_subtable.nodeID, 0);
        auto cached_meta_subtable = cached_meta_.entries[subtable_idx];
        DCHECK_EQ(cached_meta_subtable.nodeID, 0);
        if (!cached_meta_next_subtable.is_null() &&
            cached_meta_next_subtable != cached_meta_subtable)
        {
            CHECK(false) << "Failed to extend: already have subtables here. "
                            "Trying to expand subtable["
                         << subtable_idx << "] to subtable["
                         << next_subtable_idx
                         << "]. But already exists subtable at "
                         << cached_meta_next_subtable
                         << ", which is not nullptr or " << cached_meta_subtable
                         << " from subtable[" << subtable_idx << "]"
                         << ". depth: " << depth
                         << ", (1<<depth): " << (1 << depth);
        }

        // 3) allocate subtable here
        auto alloc_size = SubTableT::size_bytes();
        subtable_mem_handles_[next_subtable_idx] =
            rdma_ctx_->remote_alloc_acquire_perm(alloc_size,
                                                 config::kAllocHintSubtable);
        auto new_remote_subtable =
            subtable_mem_handles_[next_subtable_idx].gaddr();
        LOG_IF(INFO, config::kEnableExpandDebug && dctx != nullptr)
            << "[race][expand] (3) expanding subtable[" << subtable_idx
            << "] to next subtable[" << next_subtable_idx << "]. Allocated "
            << alloc_size << " at " << new_remote_subtable;

        // 4) init subtable: setup the bucket header
        auto ld = next_depth;
        auto suffix = next_subtable_idx;
        LOG_IF(INFO, config::kEnableExpandDebug && dctx != nullptr)
            << "[race][expand] (4) set up header for subtable["
            << next_subtable_idx << "]. ld: " << ld
            << ", suffix: " << next_subtable_idx;
        // should remotely memset the buffer to zero
        // then set up the header.
        SubTableHandleT new_remote_st_handle(
            new_remote_subtable, get_subtable_mem_handle(next_subtable_idx));
        rc = expand_init_and_update_remote_bucket_header_drain(
            new_remote_st_handle, ld, suffix, dctx);
        CHECK_EQ(rc, kOk);
        cached_meta_.lds[next_subtable_idx] = ld;

        // 5) insert the subtable into the directory AND lock the subtable.
        DLOG_IF(INFO, config::kEnableExpandDebug && dctx != nullptr)
            << "[race][expand] (5) Lock subtable[" << next_subtable_idx
            << "] at directory. Insert subtable " << new_remote_subtable
            << " into directory. Update ld to " << ld << " for subtable["
            << subtable_idx << "] and subtable[" << next_subtable_idx << "]";
        CHECK_EQ(expand_try_lock_subtable_drain(next_subtable_idx), kOk)
            << "Should not be conflict with concurrent expand.";
        CHECK_EQ(expand_install_subtable_nodrain(next_subtable_idx,
                                                 new_remote_subtable),
                 kOk);
        CHECK_EQ(expand_update_ld_nodrain(subtable_idx, ld), kOk);
        CHECK_EQ(expand_update_ld_nodrain(next_subtable_idx, ld), kOk);
        CHECK_EQ(rdma_ctx_->commit(), kOk);
        // update local cache
        cached_meta_.entries[next_subtable_idx] = new_remote_subtable;
        cached_meta_.lds[subtable_idx] = ld;
        cached_meta_.lds[next_subtable_idx] = ld;

        // 6) move data.
        // 6.1) update bucket suffix
        DLOG_IF(INFO, config::kEnableExpandDebug)
            << "[race][expand] (6.1) update bucket header for subtable["
            << subtable_idx << "]. ld: " << ld << ", suffix: " << subtable_idx;
        // auto *origin_subtable = entries_[subtable_idx];
        // origin_subtable->update_header(ld, subtable_idx);
        auto origin_subtable_remote_addr = cached_meta_.entries[subtable_idx];
        auto &org_st_mem_handle = get_subtable_mem_handle(subtable_idx);
        SubTableHandleT origin_subtable_handle(origin_subtable_remote_addr,
                                               org_st_mem_handle);
        CHECK_EQ(expand_update_remote_bucket_header_drain(
                     origin_subtable_handle, ld, subtable_idx, dctx),
                 kOk);
        cached_meta_.lds[subtable_idx] = ld;
        // Before 6.1) Iterate all the entries in @entries_, check for any
        // recursive updates to the entries.
        // For example, when
        // subtable_idx in {1, 5, 9, 13} pointing to the same LD = 2, suffix =
        // 0b01, when expanding subtable 1 to 5, should also set 9 pointing to 1
        // (not changed) and 13 pointing to 5 (changed)
        DLOG_IF(INFO, config::kEnableExpandDebug && dctx != nullptr)
            << "[race][expand] (6.1) recursively checks all the entries "
               "for further updates. "
            << *dctx;
        auto &dir_mem_handle = get_directory_mem_handle();
        CHECK_EQ(expand_cascade_update_entries_drain(
                     new_remote_subtable,
                     subtable_remote_addr,
                     ld,
                     round_to_bits(next_subtable_idx, ld),
                     *rdma_ctx_,
                     dir_mem_handle,
                     dctx),
                 kOk);

        // 6.3) insert all items from the old bucket to the new
        DLOG_IF(INFO, config::kEnableExpandDebug && dctx != nullptr)
            << "[race][expand] (6.3) migrate slots from subtable["
            << subtable_idx << "] to subtable[" << next_subtable_idx
            << "] with the " << ld << "-th bit == 1. " << *dctx;
        auto bits = ld;
        CHECK_GE(bits, 1) << "Test the " << bits
                          << "-th bits, which index from one.";
        auto &src_st_mem_handle = get_subtable_mem_handle(subtable_idx);
        auto &dst_st_mem_handle = get_subtable_mem_handle(next_subtable_idx);
        SubTableHandleT org_st(origin_subtable_remote_addr, src_st_mem_handle);
        SubTableHandleT dst_st(new_remote_subtable, dst_st_mem_handle);
        CHECK_EQ(expand_migrate_subtable(org_st, dst_st, bits - 1, dctx), kOk);

        // 7) unlock
        DLOG_IF(INFO, config::kEnableExpandDebug && dctx != nullptr)
            << "[race][expand] (7) Unlock subtable[" << next_subtable_idx
            << "] and subtable[" << subtable_idx << "]. " << *dctx;
        CHECK_EQ(expand_unlock_subtable_nodrain(next_subtable_idx), kOk);
        CHECK_EQ(expand_unlock_subtable_nodrain(subtable_idx), kOk);
        CHECK_EQ(rdma_ctx_->commit(), kOk);
        cached_meta_.expanding[next_subtable_idx] = 0;
        cached_meta_.expanding[subtable_idx] = 0;

        if constexpr (debug())
        {
            print_latest_meta_image(dctx);
        }

        return kOk;
    }
    void print_latest_meta_image(HashContext *dctx)
    {
        auto *rdma_buf = rdma_ctx_->get_rdma_buffer(sizeof(MetaT));
        auto &dir_mem_handle = get_directory_mem_handle();
        CHECK_EQ(rdma_ctx_->rdma_read(
                     rdma_buf, table_meta_addr_, sizeof(MetaT), dir_mem_handle),
                 kOk);
        CHECK_EQ(rdma_ctx_->commit(), kOk);
        auto &meta = *(MetaT *) rdma_buf;
        DLOG_IF(INFO, config::kEnableExpandDebug && dctx != nullptr)
            << "[race][expand][result][debug] The latest remote meta: " << meta
            << ". dctx: " << *dctx;
        if constexpr (debug())
        {
            for (size_t i = 0; i < pow((size_t) 2, (size_t) meta.gd); ++i)
            {
                auto ld = meta.lds[i];
                auto addr = meta.entries[i];
                size_t ptr_nr = 0;
                for (size_t t = 0; t < pow((size_t) 2, (size_t) meta.gd); ++t)
                {
                    if (meta.entries[t] == addr)
                    {
                        ptr_nr++;
                    }
                }
                CHECK_EQ(ptr_nr, pow((ssize_t) 2, (ssize_t) meta.gd - ld))
                    << "** If multithread, plz comment out me. subtable[" << i
                    << "]: ld " << ld << ", gd " << meta.gd
                    << ", number of ptr wrong.";
            }
        }
    }
    RetCode expand_cascade_update_entries_drain(
        GlobalAddress next_subtable_addr,
        GlobalAddress origin_subtable_addr,
        uint32_t ld,
        uint32_t suffix,
        IRdmaAdaptor &rdma_ctx,
        RemoteMemHandle &dir_mem_handle,
        HashContext *dctx)
    {
        DCHECK_EQ(next_subtable_addr.nodeID, 0);
        DCHECK_EQ(origin_subtable_addr.nodeID, 0);
        for (size_t i = 0; i < pow((size_t) 2, (size_t) cached_meta_.gd); ++i)
        {
            auto subtable_addr = GlobalAddress(cached_meta_.entries[i]);
            auto rounded_i = round_to_bits(i, ld);
            // DLOG_IF(INFO, config::kEnableExpandDebug && dctx != nullptr)
            //     << "[race][trace] expand_cascade_update_entries_drain: "
            //        "subtable["
            //     << i << "] at " << (void *) subtable_addr
            //     << ", test match with " << (void *) origin_subtable_addr
            //     << ", rounded_i: " << rounded_i << ", test match with "
            //     << suffix;
            if (subtable_addr == origin_subtable_addr)
            {
                // if addr match, should update ld.
                auto ld_remote = ld_remote_addr(i);
                auto *ld_rdma_buf = rdma_ctx.get_rdma_buffer(sizeof(uint32_t));
                *(uint32_t *) ld_rdma_buf = ld;
                CHECK_EQ(rdma_ctx.rdma_write(ld_remote,
                                             ld_rdma_buf,
                                             sizeof(uint32_t),
                                             dir_mem_handle),
                         kOk);
                cached_meta_.lds[i] = ld;
                DLOG_IF(INFO, config::kEnableExpandDebug && dctx != nullptr)
                    << "[race][trace] expand_cascade_update_entries_drain: "
                       "UPDATE LD "
                       "subtable["
                    << i << "] update LD to " << ld << ". " << *dctx;

                // if suffix match, further update entries
                if (rounded_i == suffix)
                {
                    // for entry
                    auto *entry_rdma_buf =
                        rdma_ctx.get_rdma_buffer(sizeof(next_subtable_addr));
                    *(uint64_t *) entry_rdma_buf = next_subtable_addr.val;
                    auto entry_remote = entries_remote_addr(i);
                    CHECK_EQ(rdma_ctx.rdma_write(entry_remote,
                                                 entry_rdma_buf,
                                                 sizeof(next_subtable_addr),
                                                 dir_mem_handle),
                             kOk);
                    DLOG_IF(INFO, config::kEnableExpandDebug && dctx != nullptr)
                        << "[race][trace] expand_cascade_update_entries_drain: "
                           "UPDATE ENTRY "
                           "subtable["
                        << i << "] update entry from " << origin_subtable_addr
                        << " to " << next_subtable_addr << ". " << *dctx;
                }
            }
        }
        CHECK_EQ(rdma_ctx_->commit(), kOk);
        return kOk;
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

        auto subtable_idx = rounded_m;

        auto st = subtable_handle(subtable_idx);

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
            auto rc = get_real_match_slots(
                slot_handles, key, real_match_slot_handles, dctx);
            CHECK(rc == kOk || rc == kNotFound) << "Unexpected rc: " << rc;
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
                    auto rc = do_remove(subtable_idx, view, dctx);
                    CHECK(rc == kOk || rc == kRetry);
                }
            }
        }
        return kOk;
    }

    RetCode do_remove(size_t subtable_idx,
                      SlotHandle slot_handle,
                      HashContext *dctx)
    {
        auto expect_slot = slot_handle.slot_view();
        uint64_t expect_val = expect_slot.val();
        auto desired_slot = slot_handle.view_after_clear();
        auto *rdma_buf = DCHECK_NOTNULL(rdma_ctx_->get_rdma_buffer(8));
        auto &subtable_mem_handle = get_subtable_mem_handle(subtable_idx);
        CHECK_EQ(rdma_ctx_->rdma_cas(slot_handle.remote_addr(),
                                     expect_val,
                                     desired_slot.val(),
                                     rdma_buf,
                                     subtable_mem_handle),
                 kOk);
        CHECK_EQ(rdma_ctx_->commit(), kOk);
        bool success = memcmp(rdma_buf, &expect_val, 8) == 0;
        if (success)
        {
            DLOG_IF(INFO, config::kEnableDebug && dctx != nullptr)
                << "[race][trace] do_remove SUCC: clearing slot " << slot_handle
                << ". kOk: " << *dctx;
            remote_free_kvblock(expect_slot.ptr());
            return kOk;
        }
        else
        {
            DLOG_IF(INFO, config::kEnableDebug && dctx != nullptr)
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

        GlobalAddress kv_block;
        size_t len = 0;
        CHECK_EQ(parallel_write_kv(
                     key, value, hash, *rdma_ctx_, &kv_block, &len, dctx),
                 kOk);

        DCHECK_EQ(kv_block.nodeID, 0)
            << "The gaddr we got here should have been transformed: the upper "
               "bits should be zeros";
        SlotView new_slot(fp, len, kv_block);

        auto cbs = st.get_two_combined_bucket_handle(h1, h2, *rdma_ctx_);
        CHECK_EQ(rdma_ctx_->commit(), kOk);

        DLOG_IF(INFO, config::kEnableMemoryDebug)
            << "[race][mem] new remote kv_block gaddr: " << kv_block
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

        rc = update_if_exists(rounded_m, slot_handles, key, new_slot, dctx);
        if (rc == kOk)
        {
            return rc;
        }
        else
        {
            DCHECK_EQ(rc, kNotFound);
            rc = insert_if_exist_empty_slot(
                rounded_m, cbs, cached_ld(rounded_m), m, new_slot, dctx);
        }
        if (rc == kOk)
        {
            return rc;
        }

    handle_err:
        DCHECK_NE(rc, kOk);
        // TODO(patronus): the management of kvblock
        remote_free_kvblock(kv_block);
        if (rc == kCacheStale && conf_.auto_update_dir)
        {
            // update cache and retry
            CHECK_EQ(update_directory_cache(dctx), kOk);
            return put(key, value, dctx);
        }
        return rc;
    }
    RetCode insert_if_exist_empty_slot(
        size_t subtable_idx,
        const TwoCombinedBucketHandle<kSlotNr> &cb,
        uint32_t ld,
        uint32_t suffix,
        SlotView new_slot,
        HashContext *dctx)
    {
        std::vector<BucketHandle<kSlotNr>> buckets;
        buckets.reserve(4);
        CHECK_EQ(cb.get_bucket_handle(buckets), kOk);
        for (auto &bucket : buckets)
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
                    auto &st_mem_handle = get_subtable_mem_handle(subtable_idx);
                    auto rc = bucket.do_insert(bucket.slot_handle(idx),
                                               new_slot,
                                               *rdma_ctx_,
                                               st_mem_handle,
                                               nullptr,
                                               dctx);
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

    RetCode parallel_write_kv(const Key &key,
                              const Value &value,
                              uint64_t hash,
                              IRdmaAdaptor &rdma_ctx,
                              GlobalAddress *remote_kvblock_addr,
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

        auto remote_buf = remote_alloc_kvblock(kvblock_size);
        auto &kvblock_mem_handle = get_kvblock_mem_handle();
        CHECK_EQ(rdma_ctx.rdma_write(remote_buf,
                                     (char *) rdma_buf,
                                     kvblock_size,
                                     kvblock_mem_handle),
                 kOk);
        (*remote_kvblock_addr) = remote_buf;
        (*len) = tag_ptr_len;

        DLOG_IF(INFO, config::kEnableDebug && dctx != nullptr)
            << "[race][trace] parallel_write_kv: allocate kv_block: "
            << remote_kvblock_addr << ", allocated_size: " << kvblock_size
            << ", ptr.len: " << tag_ptr_len << ". " << *dctx;
        // TODO(patronus): about the management of kv block
        // Now all the kv blocks shares the same handle in the huge memory pool
        // So, no communication is neede during the whole kv block memory
        // handling.
        // Consider to change me when doing the following experiments

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

            auto remote_kvblock_addr = GlobalAddress(slot_handle.ptr());
            DLOG_IF(INFO, config::kEnableMemoryDebug)
                << "[race][mem] Reading remote kvblock addr: "
                << remote_kvblock_addr << " with size " << actual_size
                << ", fp: " << pre_fp(slot_handle.fp());
            auto &kvblock_mem_handle = get_kvblock_mem_handle();
            CHECK_EQ(rdma_ctx_->rdma_read((char *) rdma_buffer,
                                          remote_kvblock_addr,
                                          actual_size,
                                          kvblock_mem_handle),
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
        DCHECK(inited_);
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
        DLOG_IF(INFO, config::kEnableDebug && dctx != nullptr)
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

            DLOG_IF(INFO, config::kEnableDebug && dctx != nullptr)
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

    RetCode update_if_exists(size_t subtable_idx,
                             const std::unordered_set<SlotHandle> &slot_handles,
                             const Key &key,
                             SlotView new_slot,
                             HashContext *dctx)
    {
        auto &st_mem_handle = get_subtable_mem_handle(subtable_idx);
        auto f = [&rdma_ctx = *rdma_ctx_.get(), &st_mem_handle, new_slot, this](
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
                                       rdma_buf,
                                       st_mem_handle),
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
                    auto kvblock_remote_mem = expect_slot.ptr();
                    DVLOG(4) << "[race][subtable] do_update SUCC: for slot "
                                "with kvblock_handle:"
                             << kvblock_handle << ". New_slot " << new_slot;
                    remote_free_kvblock(kvblock_remote_mem);
                }
                DLOG_IF(INFO, config::kEnableDebug && dctx != nullptr)
                    << "[race][subtable] slot " << slot_handle << " update to "
                    << new_slot << *dctx;
                return kOk;
            }
            DVLOG(4) << "[race][subtable] do_update FAILED: new_slot "
                     << new_slot;
            DLOG_IF(INFO, config::kEnableDebug && dctx != nullptr)
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
            DLOG_IF(INFO, config::kEnableLocateDebug && dctx != nullptr)
                << "[race][stable] is_real_match FAILED: key len " << key.size()
                << " mismatch with block->key_len: " << kvblock->key_len << ". "
                << *dctx;
            return kNotFound;
        }
        if (memcmp(key.data(), kvblock->buf, key.size()) != 0)
        {
            // DVLOG(4) << "[race][subtable] slot_real_match FAILED: key "
            //          << pre(key) << " miss: key content mismatch";
            DLOG_IF(INFO, config::kEnableLocateDebug && dctx != nullptr)
                << "[race][stable] is_real_match FAILED: key content mismatch. "
                << *dctx;
            return kNotFound;
        }
        // DVLOG(4) << "[race][subtable] slot_real_match SUCCEED: key "
        //          << pre(key);
        DLOG_IF(INFO, config::kEnableLocateDebug && dctx != nullptr)
            << "[race][stable] is_real_match SUCC. " << *dctx;
        return kOk;
    }
    constexpr static size_t subtable_nr()
    {
        return kDEntryNr;
    }

private:
    GlobalAddress remote_alloc_kvblock(size_t size)
    {
        return GlobalAddress(kvblock_allocator_->alloc(size));
    }
    void remote_free_kvblock(GlobalAddress addr)
    {
        // kvblock_allocator_->free((void *) addr.val);
        LOG_FIRST_N(WARNING, 1)
            << "TODO: do not actual impl remote_free_kvblock. Can not free it, "
               "because the kv block may be allocated by otehr clients. The "
               "size of the blocks may even vary. Unable to keep track and "
               "reuse those memory. "
            << addr;
    }
    // the address of hash table at remote side
    uint16_t node_id_;
    GlobalAddress table_meta_addr_;
    MetaT cached_meta_;
    RaceHashingHandleConfig conf_;
    IRdmaAdaptor::pointer rdma_ctx_;
    CoroContext *coro_ctx_{nullptr};
    std::array<RemoteMemHandle, subtable_nr()> subtable_mem_handles_;
    RemoteMemHandle kvblock_mem_handle_;
    RemoteMemHandle directory_mem_handle_;

    bool inited_{false};

    // for client private kvblock memory
    GlobalAddress kvblock_pool_gaddr_;
    std::shared_ptr<patronus::mem::SlabAllocator> kvblock_allocator_;

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
        auto &st_mem_handle = get_subtable_mem_handle(idx);
        auto mem_handle_start_gaddr = st_mem_handle.gaddr();
        auto st_start_gaddr = subtable_addr(idx);
        if (unlikely(mem_handle_start_gaddr != st_start_gaddr))
        {
            // the handle gets stale because of expansion
            build_subtable_mem_handle(idx);
            // do again
            return subtable_handle(idx);
        }
        SubTableHandleT st(st_start_gaddr, st_mem_handle);
        return st;
    }
    RemoteMemHandle &get_subtable_mem_handle(size_t idx)
    {
        DCHECK_LT(idx, subtable_mem_handles_.size());
        auto &ret = subtable_mem_handles_[idx];
        if (unlikely(!ret.valid()))
        {
            build_subtable_mem_handle(idx);
            auto &new_ret = subtable_mem_handles_[idx];
            DCHECK(new_ret.valid());
            return new_ret;
        }
        return ret;
    }
    void build_subtable_mem_handle(size_t idx)
    {
        subtable_mem_handles_[idx] = rdma_ctx_->acquire_perm(
            cached_meta_.entries[idx], SubTableT::size_bytes(), coro_ctx_);
    }
    RemoteMemHandle &get_kvblock_mem_handle()
    {
        return kvblock_mem_handle_;
    }
    RemoteMemHandle &get_directory_mem_handle()
    {
        return directory_mem_handle_;
    }
    uint32_t cached_ld(size_t idx)
    {
        return cached_meta_.lds[idx];
    }
    GlobalAddress subtable_addr(size_t idx)
    {
        return (GlobalAddress) cached_meta_.entries[idx];
    }

    void init_kvblock_mem_handle()
    {
        // init the global covering handle of kvblock
        auto g_kvblock_pool_gaddr = cached_meta_.kvblock_pool_gaddr;
        auto g_kvblock_pool_size = cached_meta_.kvblock_pool_size;
        CHECK(!g_kvblock_pool_gaddr.is_null());
        CHECK_GT(g_kvblock_pool_size, 0);

        kvblock_mem_handle_ = rdma_ctx_->acquire_perm(
            g_kvblock_pool_gaddr, g_kvblock_pool_size, coro_ctx_);

        // init the second-level allocator to the kvblocks
        auto cur_kvblock_mem_handle =
            rdma_ctx_->remote_alloc_acquire_perm(config::kKVBlockAllocBatchSize,
                                                 config::kAllocHintKVBlock,
                                                 coro_ctx_);
        // each client got a piece of region WITHIN the global region of kvblock
        if constexpr (debug())
        {
            auto g_kvblock_gaddr = kvblock_mem_handle_.gaddr();
            auto cur_kvblock_gaddr = cur_kvblock_mem_handle.gaddr();
            CHECK_EQ(g_kvblock_gaddr.nodeID, cur_kvblock_gaddr.nodeID);
            CHECK_GE(cur_kvblock_gaddr.offset, g_kvblock_gaddr.offset);
            CHECK_LE(cur_kvblock_gaddr.offset + config::kKVBlockAllocBatchSize,
                     g_kvblock_gaddr.offset + kvblock_mem_handle_.size());
        }

        kvblock_pool_gaddr_ = cur_kvblock_mem_handle.gaddr();
        patronus::mem::SlabAllocatorConfig slab_conf;
        slab_conf.block_class = {config::kKVBlockExpectSize};
        slab_conf.block_ratio = {1.0};
        kvblock_allocator_ = std::make_shared<patronus::mem::SlabAllocator>(
            (void *) kvblock_pool_gaddr_.val,
            config::kKVBlockAllocBatchSize,
            slab_conf);
    }

    void init_directory_mem_handle()
    {
        directory_mem_handle_ =
            rdma_ctx_->acquire_perm(table_meta_addr_, sizeof(MetaT), coro_ctx_);
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