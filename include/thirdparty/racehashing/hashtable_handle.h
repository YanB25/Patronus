#pragma once
#ifndef PERTRONUS_RACEHASHING_HASHTABLE_HANDLE_H_
#define PERTRONUS_RACEHASHING_HASHTABLE_HANDLE_H_

#include <cinttypes>
#include <cstddef>
#include <cstring>

#include "./hashtable.h"
#include "./mock_rdma_adaptor.h"
#include "./rhh_conf.h"
#include "./utils.h"
#include "Common.h"
#include "patronus/RdmaAdaptor.h"
#include "patronus/memory/patronus_wrapper_allocator.h"
#include "util/TimeConv.h"

using namespace util::literals;
namespace patronus::hash
{
class RdmaAdaptorKVBlockWrapperAllocator : public mem::IAllocator
{
public:
    using pointer = std::shared_ptr<RdmaAdaptorKVBlockWrapperAllocator>;
    RdmaAdaptorKVBlockWrapperAllocator(IRdmaAdaptor::pointer rdma_adpt,
                                       uint64_t hint)
        : rdma_adpt_(rdma_adpt), hint_(hint)
    {
    }
    static pointer new_instance(IRdmaAdaptor::pointer rdma_adpt, uint64_t hint)
    {
        return std::make_shared<RdmaAdaptorKVBlockWrapperAllocator>(rdma_adpt,
                                                                    hint);
    }
    void *alloc(size_t size, CoroContext *ctx = nullptr) override
    {
        std::ignore = ctx;
        auto gaddr = rdma_adpt_->remote_alloc(size, hint_);
        gaddr.nodeID = 0;
        return (void *) gaddr.val;
    }
    void free(void *addr, size_t size, CoroContext *ctx = nullptr) override
    {
        std::ignore = ctx;
        GlobalAddress gaddr(0, (uint64_t) addr);
        rdma_adpt_->remote_free(gaddr, size, hint_);
    }
    void free(void *addr, CoroContext *ctx = nullptr) override
    {
        CHECK(false) << "Unsupported free without size. addr: " << addr
                     << ", coro: " << pre_coro_ctx(ctx);
    }

private:
    IRdmaAdaptor::pointer rdma_adpt_;
    uint64_t hint_;
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

    constexpr static size_t kOngoingHandleSize = 32;

    RaceHashingHandleImpl(uint16_t node_id,
                          GlobalAddress table_meta_addr,
                          const RaceHashingHandleConfig &conf,
                          bool auto_expand,
                          IRdmaAdaptor::pointer rdma_adpt)
        : node_id_(node_id),
          table_meta_addr_(table_meta_addr),
          conf_(conf),
          auto_expand_(auto_expand),
          auto_update_dir_(auto_expand),
          rdma_adpt_(rdma_adpt)
    {
        read_kvblock_handles_.reserve(kOngoingHandleSize);
    }
    ~RaceHashingHandleImpl()
    {
        // LOG(INFO) << "[debug] !! debug_fp_conflict_m: " <<
        // debug_fp_conflict_m_;
        // LOG(INFO) << "[debug] !! debug_fp_miss_m: " << debug_fp_miss_m_;

        const auto &c = conf_.meta.d;
        if (directory_mem_handle_.valid())
        {
            rdma_adpt_->relinquish_perm(
                directory_mem_handle_, c.alloc_hint, c.relinquish_flag);
        }
        for (auto &handle : subtable_mem_handles_)
        {
            if (handle.valid())
            {
                rdma_adpt_->relinquish_perm(
                    handle, c.alloc_hint, c.relinquish_flag);
            }
        }
        if (fake_handle_.valid())
        {
            relinquish_fake_handle(fake_handle_);
        }
        if (kvblock_region_handle_.valid())
        {
            const auto &c = conf_.kvblock_region.value();
            rdma_adpt_->relinquish_perm(
                kvblock_region_handle_, c.alloc_hint, c.relinquish_flag);
        }

        end_read_kvblock();
        if (to_insert_block.handle_.valid())
        {
            const auto &c = conf_.alloc_kvblock;
            rdma_adpt_->relinquish_perm(
                to_insert_block.handle_, c.alloc_hint, c.relinquish_flag);
        }
    }
    constexpr static size_t meta_size()
    {
        return RaceHashingT::meta_size();
    }
    void debug_out(std::ostream &os) const
    {
        os << "begin_insert: " << begin_insert_nr_
           << ", end_insert: " << end_insert_nr_
           << ", free_insert: " << free_insert_nr_;
    }
    void init(util::TraceView trace = util::nulltrace)
    {
        init_directory_mem_handle();
        trace.pin("init directory mem handle");

        update_directory_cache(trace).expect(RC::kOk);

        // init_kvblock_mem_handle AFTER getting the directory cache
        // because we need to know where server places the kvblock pool

        if (conf_.meta.eager_bind_subtable)
        {
            eager_init_subtable_mem_handle();
            trace.pin("eager init subtable mem handle");
        }

        inited_ = true;
        trace.pin("Finished");
    }
    void eager_init_subtable_mem_handle()
    {
        for (size_t i = 0; i < cached_subtable_nr(); ++i)
        {
            // trigger binding
            std::ignore = get_subtable_mem_handle(i);
        }
    }

    void hack_trigger_rdma_protection_error()
    {
        auto rdma_buffer = rdma_adpt_->get_rdma_buffer(64);
        auto &handle = get_directory_mem_handle();
        auto ret = rdma_adpt_->rdma_write(
            GlobalAddress(node_id_, 8_GB), rdma_buffer.buffer, 64, handle);
        CHECK_EQ(ret, kOk);
        ret = rdma_adpt_->commit();
        LOG_IF(ERROR, ret != kRdmaProtectionErr)
            << "** Expect RdmaProtectionError. got: " << ret;

        rdma_adpt_->put_all_rdma_buffer();
    }

    RetCode update_directory_cache(util::TraceView trace)
    {
        // TODO(race): this have performance problem because of additional
        // memcpy.
        auto rdma_buf = rdma_adpt_->get_rdma_buffer(meta_size());
        DCHECK_GE(rdma_buf.size, meta_size());
        RetCode rc;
        auto &dir_mem_handle = get_directory_mem_handle();
        rc = rdma_adpt_->rdma_read(rdma_buf.buffer,
                                   table_meta_addr_,
                                   meta_size(),
                                   0 /* flag */,
                                   dir_mem_handle);
        CHECK_EQ(rc, kOk);

        rdma_adpt_->commit().expect(RC::kOk);

        memcpy(&cached_meta_, rdma_buf.buffer, meta_size());

        LOG_IF(INFO, config::kEnableDebug)
            << "[race][trace] update_directory_cache: update cache to "
            << cached_meta_ << ". " << util::pre_map(trace.kv());

        trace.pin("update_diretory_cache");
        return kOk;
    }

    static size_t max_capacity()
    {
        return RaceHashingT::max_capacity();
    }

    template <size_t kA, size_t kB, size_t kC>
    friend std::ostream &operator<<(
        std::ostream &os, const RaceHashingHandleImpl<kA, kB, kC> &rhh);

    size_t gd() const
    {
        return cached_meta_.gd;
    }

    RetCode del(const Key &key, util::TraceView trace = util::nulltrace)
    {
        if (unlikely(!inited_))
        {
            init(trace.child("init"));
        }
        DCHECK(inited_);
        auto hash = hash_impl(key.data(), key.size());
        auto m = hash_m(hash);
        auto fp = hash_fp(hash);
        auto rounded_m = round_to_bits(m, gd());
        auto [h1, h2] = hash_h1_h2(hash);

        DVLOG(3) << "[race] DEL key " << pre(key) << ", got hash "
                 << pre_hash(hash) << ", m: " << m << ", rounded to "
                 << rounded_m << " by cached_gd: " << gd()
                 << ". fp: " << pre_fp(fp);
        auto subtable_idx = rounded_m;
        auto st = subtable_handle(subtable_idx);

        // get the two combined buckets the same time
        auto cbs = st.get_two_combined_bucket_handle(h1, h2, *rdma_adpt_);
        auto rc = rdma_adpt_->commit();
        if (unlikely(rc == kRdmaProtectionErr))
        {
            return rc;
        }
        CHECK_EQ(rc, kOk);

        std::unordered_set<SlotHandle> slot_handles;
        rc = cbs.locate(fp, cached_ld(subtable_idx), m, slot_handles, trace);
        if (rc == kCacheStale && auto_update_dir_)
        {
            // update cache and retry
            auto ret = update_directory_cache(trace);
            if (ret == kRdmaProtectionErr)
            {
                return ret;
            }
            CHECK_EQ(ret, kOk);
            return del(key, trace);
        }

        rc = remove_if_exists(subtable_idx, slot_handles, key, trace);
        if (rc == kOk)
        {
            DLOG_IF(INFO, config::kEnableDebug)
                << "[race][trace] del: rm at subtable[" << subtable_idx << "]. "
                << util::pre_map(trace.kv());
        }
        else
        {
            DLOG_IF(INFO, config::kEnableDebug)
                << "[race][trace] del: not found at subtable[" << subtable_idx
                << "]. " << util::pre_map(trace.kv());
        }
        return rc;
    }
    RetCode remove_if_exists(size_t subtable_idx,
                             const std::unordered_set<SlotHandle> &slot_handles,
                             const Key &key,
                             util::TraceView trace)
    {
        auto f = [this, subtable_idx](const Key &key,
                                      SlotHandle slot_handles,
                                      KVBlockHandle kvblock_handle,
                                      util::TraceView trace) {
            std::ignore = key;
            std::ignore = kvblock_handle;
            return do_remove(subtable_idx, slot_handles, trace);
        };
        return for_the_real_match_do(slot_handles, key, f, trace);
    }
    GlobalAddress to_insert_block_gaddr() const
    {
        DCHECK(!to_insert_block.gaddrs_.empty());
        return to_insert_block.gaddrs_.front();
    }
    RemoteMemHandle &to_insert_block_handle()
    {
        DCHECK(to_insert_block.handle_.valid());
        return to_insert_block.handle_;
    }
    size_t to_insert_block_kvblock_size()
    {
        DCHECK_GT(to_insert_block.cur_kvblock_size_, 0);
        return to_insert_block.cur_kvblock_size_;
    }
    size_t to_insert_block_tagged_len()
    {
        DCHECK_GT(to_insert_block.cur_tagged_len_, 0);
        return to_insert_block.cur_tagged_len_;
    }

    RetCode do_put_once(const Key &key,
                        uint64_t hash,
                        util::TraceView trace = util::nulltrace)
    {
        DCHECK(inited_);

        auto [h1, h2] = hash_h1_h2(hash);
        auto fp = hash_fp(hash);
        auto m = hash_m(hash);
        auto cached_gd = gd();
        auto rounded_m = round_to_bits(m, cached_gd);
        auto ld = cached_ld(rounded_m);

        DCHECK(has_to_insert_block());
        auto rc = put_phase_one(key,
                                to_insert_block_gaddr(),
                                to_insert_block_tagged_len(),
                                hash,
                                trace.child("put_phase_one"));
        if (rc != kOk)
        {
            return rc;
        }
        rc = phase_two_deduplicate(key,
                                   hash,
                                   ld,
                                   10 /* retry nr */,
                                   trace.child("phase_two_deduplicate"));
        CHECK_NE(rc, kNoMem);
        CHECK_NE(rc, kRetry) << "** why dedup will require retry?";
        if constexpr (debug())
        {
            LOG_IF(INFO, config::kEnableDebug && rc == kOk)
                << "[race][trace] PUT succeed: hash: " << pre_hash(hash)
                << ", h1: " << pre_hash(h1) << ", h2: " << pre_hash(h2)
                << ", fp: " << pre_hash(fp) << ", m: " << pre_hash(m)
                << ", put to subtable[" << rounded_m << "] by gd " << cached_gd
                << ". cached_ld: " << ld << ". " << util::pre_map(trace.kv());
        }
        return rc;
    }

    RetCode do_put_loop(const Key &key, uint64_t hash, util::TraceView trace)
    {
        RetCode rc = kOk;
        size_t retry_nr{0};

        while (true)
        {
            retry_nr++;
            CHECK_LT(retry_nr, 102400) << "** Failed to many times";
            rdma_adpt_->put_all_rdma_buffer();

            if (rc == RC::kCacheStale)
            {
                // not my deal to handle this
                if (!auto_update_dir_)
                {
                    trace.pin("kCacheStale");
                    return rc;
                }

                rc = update_directory_cache(trace);
                CHECK_EQ(rc, kOk);
                // retry
                continue;
            }
            if (rc == kNoMem)
            {
                // not my deal to handle this
                if (!auto_expand_)
                {
                    trace.pin("kNoMem");
                    return rc;
                }

                auto m = hash_m(hash);
                auto cached_gd = gd();
                auto rounded_m = round_to_bits(m, cached_gd);
                auto ld = cached_ld(rounded_m);
                auto overflow_subtable_idx = round_to_bits(rounded_m, ld);
                rc = expand(overflow_subtable_idx, trace);
                // got kNoMem AGAIN from expand, which means unable to
                // expand dont retry more. exit here.
                if (rc == kNoMem)
                {
                    trace.pin("kNoMem");
                    return rc;
                }
                // special case, just ignore me.
                if (unlikely(rc == RC::kMockCrashed))
                {
                    trace.pin("kMockCrashed");
                    return rc;
                }
                // otherwise, keep the retry-ing.
                CHECK(rc == kRetry || rc == kOk || rc == kCacheStale)
                    << "** rc can be only retry or ok or cache-stale. got: "
                    << rc;
                continue;
            }

            // insert here
            CHECK(rc == kOk || rc == kRetry) << "** Unexpected rc: " << rc;
            rc = do_put_once(key, hash, trace);
            if (rc == kOk)
            {
                trace.pin("OK");
                return rc;
            }

            CHECK(rc == kNoMem || rc == kRetry || rc == kCacheStale)
                << "** Only allow kNoMem, kRetry, kCacheStale. got: " << rc;
        }
    }

    RetCode put(const Key &key,
                const Value &value,
                util::TraceView trace = util::nulltrace)
    {
        if (unlikely(!inited_))
        {
            init(trace.child("init"));
        }
        DCHECK(inited_);

        // NOTE: the prepare_alloc_kv is optional (if already exist allocated
        // one)
        if (unlikely(!has_to_insert_block()))
        {
            // need allocation
            prepare_alloc_kv(key, value).expect(RC::kOk);
        }
        DCHECK(has_to_insert_block());
        // NOTE: the prepare_write_kv is doing once for each put request
        auto hash = hash_impl(key.data(), key.size());
        prepare_write_kv(key,
                         value,
                         hash,
                         to_insert_block_gaddr(),
                         to_insert_block_kvblock_size(),
                         to_insert_block_handle(),
                         *rdma_adpt_,
                         trace)
            .expect(RC::kOk);

        // Here we will loop until insertion succeeded (best-efford)
        trace.pin("allocate");
        auto rc = do_put_loop(key, hash, trace);

        if (rc == RC::kOk)
        {
            consume_kv_block();
            return rc;
        }
        // retry soo many times but still failed by do_put_loop
        // will not retry anymore.
        if (rc == RC::kRetry || rc == RC::kNoMem || rc == RC::kMockCrashed)
        {
            return rc;
        }

        CHECK(false) << "** Unexpected and unhandled rc here. rc: " << rc;
        return RC::kInvalid;
    }
    constexpr static size_t subtable_nr()
    {
        return kDEntryNr;
    }
    std::array<RemoteMemHandle, subtable_nr()> expand_lock_handles_{};
    std::chrono::time_point<std::chrono::steady_clock> lock_handle_last_extend_;
    bool maybe_expand_try_extend_lock_lease(
        size_t subtable_idx, [[maybe_unused]] util::TraceView trace)
    {
        const auto &c = conf_.expand;
        if (unlikely(!c.use_patronus_lock))
        {
            return true;
        }
        auto now = std::chrono::steady_clock::now();
        uint64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                          now - lock_handle_last_extend_)
                          .count();
        bool should_extend = ns >= util::time::to_ns(c.lock_time_ns) / 2;
        if (unlikely(should_extend))
        {
            auto rc = rdma_adpt_->extend(expand_lock_handles_[subtable_idx],
                                         c.lock_time_ns);
            CHECK_EQ(rc, kOk) << "Failed to extend.";
            lock_handle_last_extend_ = now;
            if (rc == kOk)
            {
                DLOG_IF(INFO, config::kEnableExpandDebug)
                    << "[race][expand] maybe_expand_try_extend_lock_lease: "
                       "extend SUCCEEDED. ";
                return true;
            }
            else
            {
                DLOG_IF(INFO, config::kEnableExpandDebug)
                    << "[race][expand] maybe_expand_try_extend_lock_lease: "
                       "extend FAILED ";
                return false;
            }
        }
        else
        {
            DLOG_IF(INFO, config::kEnableExpandDebug)
                << "[race][expand] maybe_expand_try_extend_lock_lease: "
                   "extend SKIP. elapsed "
                << ns << " ns, not close to expire time.";
        }
        return true;
    }
    RetCode expand_try_lock_subtable_drain(size_t subtable_idx)
    {
        const auto &c = conf_.expand;
        if (unlikely(c.use_patronus_lock))
        {
            // a little bit hack
            // get another lease to utilize the lock & fault tolerant semantics
            expand_lock_handles_[subtable_idx] =
                rdma_adpt_->acquire_perm(lock_remote_addr(subtable_idx),
                                         0,
                                         8,
                                         c.lock_time_ns,
                                         c.patronus_lock_flag);
            if (!expand_lock_handles_[subtable_idx].valid())
            {
                CHECK_EQ(expand_lock_handles_[subtable_idx].ec(),
                         AcquireRequestStatus::kLockedErr);
                return kRetry;
            }

            lock_handle_last_extend_ = std::chrono::steady_clock::now();
            return kOk;
        }
        else
        {
            auto rdma_buf = rdma_adpt_->get_rdma_buffer(8);
            DCHECK_GE(rdma_buf.size, 8);
            auto remote = lock_remote_addr(subtable_idx);
            uint64_t expect = 0;   // no lock
            uint64_t desired = 1;  // lock
            auto &dir_mem_handle = get_directory_mem_handle();
            rdma_adpt_
                ->rdma_cas(remote,
                           expect,
                           desired,
                           rdma_buf.buffer,
                           0 /* flag */,
                           dir_mem_handle)
                .expect(RC::kOk);
            auto rc = rdma_adpt_->commit();
            if (unlikely(rc == kRdmaProtectionErr))
            {
                return rc;
            }
            CHECK_EQ(rc, kOk);
            uint64_t r = *(uint64_t *) rdma_buf.buffer;
            CHECK(r == 0 || r == 1)
                << "Unexpected value read from lock of subtable["
                << subtable_idx << "]. Expect 0 or 1, got " << r;
            if (r == 0)
            {
                DCHECK_EQ(desired, 1);
                cached_meta_.expanding[subtable_idx] = desired;
                return kOk;
            }
            return kRetry;
        }
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
        const auto &c = conf_.expand;
        if (c.use_patronus_lock)
        {
            rdma_adpt_->relinquish_perm(
                expand_lock_handles_[subtable_idx], 0, c.patronus_unlock_flag);
            return kOk;
        }
        else
        {
            auto rdma_buf = rdma_adpt_->get_rdma_buffer(8);
            DCHECK_GE(rdma_buf.size, 8);
            auto remote = lock_remote_addr(subtable_idx);
            *(uint64_t *) DCHECK_NOTNULL(rdma_buf.buffer) = 0;  // no lock
            auto &dir_mem_handle = get_directory_mem_handle();
            return rdma_adpt_->rdma_write(remote,
                                          (char *) rdma_buf.buffer,
                                          8,
                                          0 /* flag */,
                                          dir_mem_handle);
        }
    }
    RetCode expand_write_entry_nodrain(size_t subtable_idx,
                                       GlobalAddress subtable_remote_addr)
    {
        auto entry_size = sizeof(subtable_remote_addr);
        auto rdma_buf = rdma_adpt_->get_rdma_buffer(entry_size);
        DCHECK_GE(rdma_buf.size, entry_size);

        *(uint64_t *) rdma_buf.buffer = subtable_remote_addr.val;
        auto remote = entries_remote_addr(subtable_idx);
        auto &dir_mem_handle = get_directory_mem_handle();
        return rdma_adpt_->rdma_write(remote,
                                      (char *) rdma_buf.buffer,
                                      entry_size,
                                      0 /* flag */,
                                      dir_mem_handle);
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
        auto rdma_buf = rdma_adpt_->get_rdma_buffer(entry_size);
        DCHECK_GE(rdma_buf.size, entry_size);
        *(ld_t *) rdma_buf.buffer = ld;
        auto remote = ld_remote_addr(subtable_idx);
        auto &dir_mem_handle = get_directory_mem_handle();
        return rdma_adpt_->rdma_write(remote,
                                      (char *) rdma_buf.buffer,
                                      entry_size,
                                      0 /* flag */,
                                      dir_mem_handle);
    }
    GlobalAddress gd_remote_addr() const
    {
        return table_meta_addr_ + offsetof(MetaT, gd);
    }
    RetCode expand_cas_gd_drain(uint64_t expect, uint64_t desired)
    {
        auto rdma_buf = rdma_adpt_->get_rdma_buffer(8);
        DCHECK_GE(rdma_buf.size, 8);
        auto remote = gd_remote_addr();
        auto &dir_mem_handle = get_directory_mem_handle();
        rdma_adpt_
            ->rdma_cas(remote,
                       expect,
                       desired,
                       rdma_buf.buffer,
                       0 /* flag */,
                       dir_mem_handle)
            .expect(RC::kOk);
        auto rc = rdma_adpt_->commit();
        if (unlikely(rc == kRdmaProtectionErr))
        {
            return rc;
        }
        CHECK_EQ(rc, kOk);

        uint64_t r = *(uint64_t *) rdma_buf.buffer;
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
        util::TraceView trace)
    {
        auto rc = subtable_handle.update_bucket_header_nodrain(
            ld, suffix, *rdma_adpt_, trace);
        if (rc != kOk)
        {
            return rc;
        }
        return rdma_adpt_->commit();
    }
    RetCode expand_init_and_update_remote_bucket_header_drain(
        SubTableHandleT &subtable_handle,
        uint32_t ld,
        uint32_t suffix,
        util::TraceView trace)
    {
        // just a wrapper, this parameters to the ctor is not used.
        auto rc = subtable_handle.init_and_update_bucket_header_drain(
            ld, suffix, *rdma_adpt_, trace);
        if (rc != kOk)
        {
            return rc;
        }
        return rdma_adpt_->commit();
    }
    RetCode expand_install_subtable_nodrain(size_t subtable_idx,
                                            GlobalAddress new_remote_subtable)
    {
        auto size = sizeof(new_remote_subtable.val);
        auto rdma_buf = rdma_adpt_->get_rdma_buffer(size);
        DCHECK_GE(rdma_buf.size, size);
        *(uint64_t *) rdma_buf.buffer = new_remote_subtable.val;
        auto remote = entries_remote_addr(subtable_idx);
        auto &dir_mem_handle = get_directory_mem_handle();
        return rdma_adpt_->rdma_write(remote,
                                      (char *) rdma_buf.buffer,
                                      size,
                                      0 /* flag */,
                                      dir_mem_handle);
    }
    /**
     * pre-condition: the subtable of dst_staddr is all empty.
     */
    RetCode expand_migrate_subtable(SubTableHandleT &src_st_handle,
                                    SubTableHandleT &dst_st_handle,
                                    size_t bit,
                                    util::TraceView trace)
    {
        std::unordered_set<SlotMigrateHandle> should_migrate;
        auto st_size = SubTableT::size_bytes();
        auto rdma_buf = rdma_adpt_->get_rdma_buffer(st_size);
        DCHECK_GE(rdma_buf.size, st_size);
        rdma_adpt_
            ->rdma_read(rdma_buf.buffer,
                        src_st_handle.gaddr(),
                        st_size,
                        0 /* flag */,
                        src_st_handle.mem_handle())
            .expect(RC::kOk);
        auto rc = rdma_adpt_->commit();
        if (unlikely(rc == kRdmaProtectionErr))
        {
            return rc;
        }
        CHECK_EQ(rc, kOk);

        if (unlikely(!fake_handle_.valid()))
        {
            fake_handle_ = alloc_fake_handle();
        }

        for (size_t i = 0; i < SubTableHandleT::kTotalBucketNr; ++i)
        {
            auto remote_bucket_addr = GlobalAddress(
                src_st_handle.gaddr() + i * Bucket<kSlotNr>::size_bytes());
            void *bucket_buffer_addr =
                (char *) rdma_buf.buffer + i * Bucket<kSlotNr>::size_bytes();
            BucketHandle<kSlotNr> b(remote_bucket_addr,
                                    (char *) bucket_buffer_addr);
            CHECK_EQ(b.should_migrate(
                         bit, *rdma_adpt_, fake_handle_, should_migrate, trace),
                     kOk);
        }

        // TODO: rethink about how batching can boost performance
        auto capacity = SubTableT::max_capacity();
        DLOG_IF(INFO, config::kEnableExpandDebug)
            << "[race][expand] should migrate " << should_migrate.size()
            << " entries. subtable capacity: " << capacity << ". "
            << util::pre_map(trace.kv());
        CHECK_LE(should_migrate.size(), capacity)
            << "Should migrate got " << should_migrate.size()
            << ", exceeded subtable capacity " << capacity;

        if constexpr (debug())
        {
            LOG_FIRST_N(WARNING, 1) << "TODO: expand_migrate_subtable: "
                                       "put_slot will read cb each time "
                                       "for one entry migration.";
        }

        for (auto slot_handle : should_migrate)
        {
            SlotHandle ret_slot(nullgaddr, SlotView(0));
            CHECK_EQ(dst_st_handle.put_slot(
                         slot_handle, *rdma_adpt_, &ret_slot, trace),
                     kOk);
            auto rc = src_st_handle.try_del_slot(slot_handle.slot_handle(),
                                                 *rdma_adpt_);
            if (rc != kOk)
            {
                DCHECK(!ret_slot.ptr().is_null());
                dst_st_handle.del_slot(ret_slot, *rdma_adpt_);
            }
            rdma_adpt_->put_all_rdma_buffer();
        }

        return kOk;
    }
    // TODO: expand still has lots of problem
    // When trying to expand to dst subtable, the concurrent clients can insert
    // lots of items to make it full. Therefore cascaded expansion is required.
    // The client can always find out cache stale and update their directory
    // cache.
    RetCode expand(size_t subtable_idx, util::TraceView trace)
    {
        if constexpr (::config::kEnableRdmaTrace)
        {
            if (likely(!rdma_adpt_->trace_enabled()))
            {
                if (true_with_prob(::config::kRdmaTraceRateExpand))
                {
                    rdma_adpt_->enable_trace("put-expand");
                }
            }
        }

        DCHECK(inited_);
        CHECK_LT(subtable_idx, kDEntryNr);
        // 0) try lock
        auto rc = expand_try_lock_subtable_drain(subtable_idx);
        trace.pin("expand_try_lock_subtable_drain");
        if (rc != kOk)
        {
            // failed migration, don't trace me.
            return rc;
        }

        // NOTE:
        // Here, right after successfully locking the subtable
        // we do the fault tolerance test of Patronus
        // let the client crashes here.
        if (unlikely(conf_.expand.mock_crash_nr > 0))
        {
            LOG(WARNING) << "[rhh] MOCK: client crashed. ";
            conf_.expand.mock_crash_nr--;

            return kMockCrashed;
        }

        // 1) I think updating the directory cache is definitely necessary
        // while expanding.
        auto ret = update_directory_cache(trace);
        if (ret == kRdmaProtectionErr)
        {
            return ret;
        }
        CHECK_EQ(ret, kOk);
        auto depth = cached_meta_.lds[subtable_idx];
        auto next_subtable_idx = subtable_idx | (1 << depth);
        DLOG_IF(INFO, config::kEnableExpandDebug)
            << "[race][expand] Expanding subtable[" << subtable_idx
            << "] to subtable[" << next_subtable_idx
            << "]. meta: " << cached_meta_
            << ". trace: " << util::pre_map(trace.kv());
        auto subtable_remote_addr = cached_meta_.entries[subtable_idx];
        maybe_expand_try_extend_lock_lease(subtable_idx, trace);

        // okay, entered critical section
        auto next_depth = depth + 1;
        auto expect_gd = gd();
        if (pow(2, next_depth) > kDEntryNr)
        {
            // DLOG(WARNING) << "[race][expand] (1) trying to expand to gd "
            //               << next_depth << " with entries "
            //               << pow(2, next_depth) << ". Out of directory
            //               entry.";
            auto rc = expand_unlock_subtable_nodrain(subtable_idx);
            if (unlikely(rc == kRdmaProtectionErr))
            {
                return rc;
            }
            CHECK_EQ(rc, kOk);
            rc = rdma_adpt_->commit();
            if (unlikely(rc == kRdmaProtectionErr))
            {
                return rc;
            }
            CHECK_EQ(rc, kOk);
            if constexpr (debug())
            {
                if (!conf_.expand.use_patronus_lock)
                {
                    CHECK_EQ(cached_meta_.expanding[subtable_idx], 1);
                }
            }
            cached_meta_.expanding[subtable_idx] = 0;
            return kNoMem;
        }
        rdma_adpt_->put_all_rdma_buffer();
        maybe_expand_try_extend_lock_lease(subtable_idx, trace);

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
                    DLOG_IF(INFO, config::kEnableExpandDebug)
                        << "[race][expand] (2) Update gd and directory: "
                           "setting subtable["
                        << i << "] to subtale[" << from_subtable_idx << "]. "
                        << util::pre_map(trace.kv());
                    auto subtable_remote_addr =
                        cached_meta_.entries[from_subtable_idx];
                    auto ld = cached_ld(from_subtable_idx);
                    auto rc =
                        expand_write_entry_nodrain(i, subtable_remote_addr);
                    if (unlikely(rc == kRdmaProtectionErr))
                    {
                        return rc;
                    }
                    CHECK_EQ(rc, kOk);
                    rc = expand_update_ld_nodrain(i, ld);
                    if (unlikely(rc == kRdmaProtectionErr))
                    {
                        return rc;
                    }
                    CHECK_EQ(rc, kOk);
                    // TODO(race): update cache here. Not sure if it is right
                    cached_meta_.entries[i] = subtable_remote_addr;
                    cached_meta_.lds[i] = ld;
                }
            }
            auto rc = rdma_adpt_->commit();
            if (unlikely(rc == kRdmaProtectionErr))
            {
                return rc;
            }
            CHECK_EQ(rc, kOk);

            DLOG_IF(INFO, config::kEnableExpandDebug)
                << "[race][expand] (2) Update gd and directory: setting gd to "
                << next_depth;
            rc = expand_cas_gd_drain(expect_gd, next_depth);
            if (unlikely(rc == kRdmaProtectionErr))
            {
                return rc;
            }
            CHECK_EQ(rc, kOk);
        }
        trace.pin("expand directory");

        CHECK_LT(next_subtable_idx, kDEntryNr);
        maybe_expand_try_extend_lock_lease(subtable_idx, trace);

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
        rdma_adpt_->put_all_rdma_buffer();
        maybe_expand_try_extend_lock_lease(subtable_idx, trace);

        // 3) allocate subtable here
        auto alloc_size = SubTableT::size_bytes();
        if (subtable_mem_handles_[next_subtable_idx].valid())
        {
            const auto &c = conf_.meta.d;
            rdma_adpt_->relinquish_perm(
                subtable_mem_handles_[next_subtable_idx],
                c.alloc_hint,
                c.relinquish_flag);
        }
        subtable_mem_handles_[next_subtable_idx] =
            remote_alloc_acquire_subtable_directory(alloc_size);
        auto &next_subtable_handle = subtable_mem_handles_[next_subtable_idx];
        auto new_remote_subtable =
            subtable_mem_handles_[next_subtable_idx].gaddr();
        LOG_IF(INFO, config::kEnableExpandDebug)
            << "[race][expand] (3) expanding subtable[" << subtable_idx
            << "] to next subtable[" << next_subtable_idx << "]. Allocated "
            << alloc_size << " at " << new_remote_subtable;
        rdma_adpt_->put_all_rdma_buffer();
        maybe_expand_try_extend_lock_lease(subtable_idx, trace);
        trace.pin("allocate subtable");

        // 4) init subtable: setup the bucket header
        auto ld = next_depth;
        auto suffix = next_subtable_idx;
        LOG_IF(INFO, config::kEnableExpandDebug)
            << "[race][expand] (4) set up header for subtable["
            << next_subtable_idx << "]. ld: " << ld
            << ", suffix: " << next_subtable_idx;
        // should remotely memset the buffer to zero
        // then set up the header.
        SubTableHandleT new_remote_st_handle(new_remote_subtable,
                                             next_subtable_handle);
        rc = expand_init_and_update_remote_bucket_header_drain(
            new_remote_st_handle, ld, suffix, trace);
        CHECK_EQ(rc, kOk);
        cached_meta_.lds[next_subtable_idx] = ld;

        rdma_adpt_->put_all_rdma_buffer();
        maybe_expand_try_extend_lock_lease(subtable_idx, trace);
        trace.pin("4) init subtable");

        // 5) insert the subtable into the directory AND lock the subtable.
        DLOG_IF(INFO, config::kEnableExpandDebug)
            << "[race][expand] (5) Lock subtable[" << next_subtable_idx
            << "] at directory. Insert subtable " << new_remote_subtable
            << " into directory. Update ld to " << ld << " for subtable["
            << subtable_idx << "] and subtable[" << next_subtable_idx << "]";
        rc = expand_try_lock_subtable_drain(next_subtable_idx);
        maybe_expand_try_extend_lock_lease(subtable_idx, trace);
        if (unlikely(rc == kRdmaProtectionErr))
        {
            return rc;
        }
        CHECK_EQ(rc, kOk) << "Should not be conflict with concurrent expand.";
        CHECK_EQ(expand_install_subtable_nodrain(next_subtable_idx,
                                                 new_remote_subtable),
                 kOk);
        maybe_expand_try_extend_lock_lease(subtable_idx, trace);

        rc = expand_update_ld_nodrain(subtable_idx, ld);
        if (unlikely(rc == kRdmaProtectionErr))
        {
            return rc;
        }
        CHECK_EQ(rc, kOk);
        rc = expand_update_ld_nodrain(next_subtable_idx, ld);
        if (unlikely(rc == kRdmaProtectionErr))
        {
            return rc;
        }
        CHECK_EQ(rc, kOk);
        rc = rdma_adpt_->commit();
        if (unlikely(rc == kRdmaProtectionErr))
        {
            return rc;
        }
        CHECK_EQ(rc, kOk);
        // update local cache
        cached_meta_.entries[next_subtable_idx] = new_remote_subtable;
        cached_meta_.lds[subtable_idx] = ld;
        cached_meta_.lds[next_subtable_idx] = ld;

        rdma_adpt_->put_all_rdma_buffer();
        maybe_expand_try_extend_lock_lease(subtable_idx, trace);
        trace.pin("5) allocate subtable");

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
                     origin_subtable_handle, ld, subtable_idx, trace),
                 kOk);
        cached_meta_.lds[subtable_idx] = ld;

        rdma_adpt_->put_all_rdma_buffer();
        maybe_expand_try_extend_lock_lease(subtable_idx, trace);
        trace.pin("6.1) update bucket suffix");

        // Before 6.1) Iterate all the entries in @entries_, check for any
        // recursive updates to the entries.
        // For example, when
        // subtable_idx in {1, 5, 9, 13} pointing to the same LD = 2, suffix =
        // 0b01, when expanding subtable 1 to 5, should also set 9 pointing to 1
        // (not changed) and 13 pointing to 5 (changed)
        // 6.2)
        DLOG_IF(INFO, config::kEnableExpandDebug)
            << "[race][expand] (6.1) recursively checks all the entries "
               "for further updates. "
            << util::pre_map(trace.kv());
        auto &dir_mem_handle = get_directory_mem_handle();
        CHECK_EQ(expand_cascade_update_entries_drain(
                     new_remote_subtable,
                     subtable_remote_addr,
                     ld,
                     round_to_bits(next_subtable_idx, ld),
                     *rdma_adpt_,
                     dir_mem_handle,
                     trace),
                 kOk);
        rdma_adpt_->put_all_rdma_buffer();
        maybe_expand_try_extend_lock_lease(subtable_idx, trace);

        trace.pin("6.2) cascade update entries");

        // 6.3) insert all items from the old bucket to the new
        DLOG_IF(INFO, config::kEnableExpandDebug)
            << "[race][expand] (6.3) migrate slots from subtable["
            << subtable_idx << "] to subtable[" << next_subtable_idx
            << "] with the " << ld << "-th bit == 1. "
            << util::pre_map(trace.kv());
        auto bits = ld;
        CHECK_GE(bits, 1) << "Test the " << bits
                          << "-th bits, which index from one.";
        auto &src_st_mem_handle = org_st_mem_handle;
        auto &dst_st_mem_handle = next_subtable_handle;
        SubTableHandleT org_st(origin_subtable_remote_addr, src_st_mem_handle);
        SubTableHandleT dst_st(new_remote_subtable, dst_st_mem_handle);
        rc = expand_migrate_subtable(org_st, dst_st, bits - 1, trace);
        if (unlikely(rc == kRdmaProtectionErr))
        {
            return rc;
        }
        CHECK_EQ(rc, kOk);
        rdma_adpt_->put_all_rdma_buffer();
        maybe_expand_try_extend_lock_lease(subtable_idx, trace);

        trace.pin("6.3) migrate slots");

        // 7) unlock
        DLOG_IF(INFO, config::kEnableExpandDebug)
            << "[race][expand] (7) Unlock subtable[" << next_subtable_idx
            << "] and subtable[" << subtable_idx << "]. "
            << util::pre_map(trace.kv());
        rc = expand_unlock_subtable_nodrain(next_subtable_idx);
        CHECK_EQ(rc, kOk);
        rc = expand_unlock_subtable_nodrain(subtable_idx);
        CHECK_EQ(rc, kOk);
        rc = rdma_adpt_->commit();
        if (unlikely(rc == kRdmaProtectionErr))
        {
            return rc;
        }
        CHECK_EQ(rc, kOk);
        cached_meta_.expanding[next_subtable_idx] = 0;
        cached_meta_.expanding[subtable_idx] = 0;
        trace.pin("7) unlock");

        if constexpr (debug())
        {
            print_latest_meta_image(trace);
        }
        return kOk;
    }
    void print_latest_meta_image(util::TraceView trace)
    {
        auto rdma_buf = rdma_adpt_->get_rdma_buffer(sizeof(MetaT));
        DCHECK_GE(rdma_buf.size, sizeof(MetaT));
        auto &dir_mem_handle = get_directory_mem_handle();
        rdma_adpt_
            ->rdma_read(rdma_buf.buffer,
                        table_meta_addr_,
                        sizeof(MetaT),
                        0 /* flag */,
                        dir_mem_handle)
            .expect(RC::kOk);
        rdma_adpt_->commit().expect(RC::kOk);
        auto &meta = *(MetaT *) rdma_buf.buffer;
        DLOG_IF(INFO, config::kEnableExpandDebug)
            << "[race][expand][result][debug] The latest remote meta: " << meta
            << ". trace: " << util::pre_map(trace.kv());
    }
    RetCode expand_cascade_update_entries_drain(
        GlobalAddress next_subtable_addr,
        GlobalAddress origin_subtable_addr,
        uint32_t ld,
        uint32_t suffix,
        IRdmaAdaptor &rdma_adpt,
        RemoteMemHandle &dir_mem_handle,
        util::TraceView trace)
    {
        DCHECK_EQ(next_subtable_addr.nodeID, 0);
        DCHECK_EQ(origin_subtable_addr.nodeID, 0);
        for (size_t i = 0; i < pow((size_t) 2, (size_t) cached_meta_.gd); ++i)
        {
            auto subtable_addr = GlobalAddress(cached_meta_.entries[i]);
            auto rounded_i = round_to_bits(i, ld);
            // DLOG_IF(INFO, config::kEnableExpandDebug )
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
                auto ld_rdma_buf = rdma_adpt.get_rdma_buffer(sizeof(uint32_t));
                DCHECK_GE(ld_rdma_buf.size, sizeof(uint32_t));
                *(uint32_t *) ld_rdma_buf.buffer = ld;
                rdma_adpt
                    .rdma_write(ld_remote,
                                ld_rdma_buf.buffer,
                                sizeof(uint32_t),
                                0 /* flag */,
                                dir_mem_handle)
                    .expect(RC::kOk);
                cached_meta_.lds[i] = ld;
                DLOG_IF(INFO, config::kEnableExpandDebug)
                    << "[race][trace] expand_cascade_update_entries_drain: "
                       "UPDATE LD "
                       "subtable["
                    << i << "] update LD to " << ld << ". "
                    << util::pre_map(trace.kv());

                // if suffix match, further update entries
                if (rounded_i == suffix)
                {
                    // for entry
                    auto entry_rdma_buf =
                        rdma_adpt.get_rdma_buffer(sizeof(next_subtable_addr));
                    DCHECK_GE(entry_rdma_buf.size, sizeof(next_subtable_addr));
                    *(uint64_t *) entry_rdma_buf.buffer =
                        next_subtable_addr.val;
                    auto entry_remote = entries_remote_addr(i);
                    rdma_adpt
                        .rdma_write(entry_remote,
                                    entry_rdma_buf.buffer,
                                    sizeof(next_subtable_addr),
                                    0 /* flag */,
                                    dir_mem_handle)
                        .expect(RC::kOk);
                    DLOG_IF(INFO, config::kEnableExpandDebug)
                        << "[race][trace] expand_cascade_update_entries_drain: "
                           "UPDATE ENTRY "
                           "subtable["
                        << i << "] update entry from " << origin_subtable_addr
                        << " to " << next_subtable_addr << ". "
                        << util::pre_map(trace.kv());
                }
            }
        }
        auto rc = rdma_adpt_->commit();
        if (unlikely(rc == kRdmaProtectionErr))
        {
            return rc;
        }
        CHECK_EQ(rc, kOk);
        return kOk;
    }
    RetCode phase_two_deduplicate(const Key &key,
                                  uint64_t hash,
                                  uint32_t cached_ld,
                                  ssize_t retry_nr,
                                  util::TraceView trace)
    {
        if (unlikely(retry_nr <= 0))
        {
            LOG(FATAL) << "** This is wierd. Tried many times for "
                          "deduplication, but always found cache stale.";
            // return kCacheStale;
            return kOk;
        }
        auto m = hash_m(hash);
        auto rounded_m = round_to_bits(m, gd());
        auto [h1, h2] = hash_h1_h2(hash);
        auto fp = hash_fp(hash);

        auto subtable_idx = rounded_m;

        auto st = subtable_handle(subtable_idx);

        // get the two combined buckets the same time
        // validate staleness at the same time
        auto cbs = st.get_two_combined_bucket_handle(h1, h2, *rdma_adpt_);
        auto rc = rdma_adpt_->commit();
        if (unlikely(rc == kRdmaProtectionErr))
        {
            return rc;
        }
        CHECK_EQ(rc, kOk);

        std::unordered_set<SlotHandle> slot_handles;
        rc = cbs.locate(fp, cached_ld, m, slot_handles, trace);
        DLOG_IF(WARNING, slot_handles.size() >= 5)
            << "** Got a lots of real match. m: " << m
            << ", rounded_m: " << rounded_m << ", h1: " << pre_hash(h1)
            << ", h2: " << pre_hash(h2) << ", fp: " << pre_hash(fp)
            << ". key: " << key
            << ". Got possible match nr: " << slot_handles.size();
        if (rc == kCacheStale && auto_update_dir_)
        {
            // update cache and retry
            update_directory_cache(trace).expect(RC::kOk);
            return phase_two_deduplicate(
                key, hash, cached_ld, retry_nr - 1, trace);
        }

        // duplicate existed. Do the deduplication
        if (slot_handles.size() >= 2)
        {
            std::unordered_set<SlotHandle> real_match_slot_handles;
            auto rc = get_real_match_slots(
                slot_handles, key, real_match_slot_handles, trace);
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
                    auto rc = do_remove(subtable_idx, view, trace);
                    CHECK(rc == kOk || rc == kRetry);
                }
            }
        }
        trace.pin("Finished");
        return kOk;
    }

    RetCode do_remove(size_t subtable_idx,
                      SlotHandle slot_handle,
                      util::TraceView trace)
    {
        auto expect_slot = slot_handle.slot_view();
        uint64_t expect_val = expect_slot.val();
        auto desired_slot = slot_handle.view_after_clear();
        auto rdma_buf = rdma_adpt_->get_rdma_buffer(8);
        DCHECK_GE(rdma_buf.size, 8);
        auto &subtable_mem_handle = get_subtable_mem_handle(subtable_idx);
        rdma_adpt_
            ->rdma_cas(slot_handle.remote_addr(),
                       expect_val,
                       desired_slot.val(),
                       rdma_buf.buffer,
                       0 /* flag */,
                       subtable_mem_handle)
            .expect(RC::kOk);
        auto rc = rdma_adpt_->commit();
        if (unlikely(rc == kRdmaProtectionErr))
        {
            return rc;
        }
        CHECK_EQ(rc, kOk);
        bool success = memcmp(rdma_buf.buffer, &expect_val, 8) == 0;
        if (success)
        {
            DLOG_IF(INFO, config::kEnableDebug)
                << "[race][trace] do_remove SUCC: clearing slot " << slot_handle
                << ". kOk: " << util::pre_map(trace.kv());
            do_free_kvblock(expect_slot.ptr());
            return kOk;
        }
        else
        {
            DLOG_IF(INFO, config::kEnableDebug)
                << "[race][trace] do_remove FAILED: clearing slot "
                << slot_handle << ". kRetry. " << util::pre_map(trace.kv());
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
        std::unordered_set<SlotHandle> &real_matches,
        util::TraceView trace)
    {
        auto f = [&real_matches](const Key &key,
                                 SlotHandle slot,
                                 KVBlockHandle kvblock_handle,
                                 util::TraceView trace) {
            std::ignore = key;
            std::ignore = kvblock_handle;
            std::ignore = trace;
            real_matches.insert(slot);
            return kOk;
        };
        return for_the_real_match_do(slot_handles, key, f, trace);
    }
    RetCode put_phase_one(const Key &key,
                          GlobalAddress kv_block,
                          size_t len,
                          uint64_t hash,
                          util::TraceView trace)
    {
        RetCode rc = kOk;
        auto m = hash_m(hash);
        auto fp = hash_fp(hash);
        auto [h1, h2] = hash_h1_h2(hash);
        auto rounded_m = round_to_bits(m, gd());
        auto ld = cached_ld(rounded_m);
        DLOG_IF(INFO, config::kEnableDebug)
            << "[race][trace] put_phase_one: got hash " << pre_hash(hash)
            << ", m: " << m << ", rounded to " << rounded_m
            << " (subtable) by gd(may stale): " << gd()
            << ". fp: " << pre_fp(fp) << ", cached_ld: " << ld;
        auto st = subtable_handle(rounded_m);

        DCHECK_EQ(kv_block.nodeID, 0)
            << "The gaddr we got here should have been transformed: the upper "
               "bits should be zeros";
        SlotView new_slot(fp, len, kv_block);

        auto cbs = st.get_two_combined_bucket_handle(h1, h2, *rdma_adpt_);
        rdma_adpt_->commit().expect(RC::kOk);
        trace.pin("p1.get_two_combined_bucket_handle");

        DLOG_IF(INFO, config::kEnableMemoryDebug)
            << "[race][mem] allocated kv_block gaddr: " << kv_block
            << " with len: " << len
            << ". actual len: " << new_slot.actual_len_bytes();

        std::unordered_set<SlotHandle> slot_handles;
        rc = cbs.locate(fp, ld, m, slot_handles, trace);
        CHECK_NE(rc, kNotFound)
            << "Even not found should not response not found";
        if (rc != kOk)
        {
            CHECK_EQ(rc, kCacheStale)
                << "** cbs.locate only allow kCacheStale err";
            return rc;
        }

        rc = update_if_exists(rounded_m,
                              slot_handles,
                              key,
                              new_slot,
                              trace.child("update_if_exists"));

        if (rc == kOk)
        {
            return rc;
        }
        // also another way of "kOk", we succeeded but found nothing
        else if (rc == kNotFound)
        {
            rc = insert_if_exist_empty_slot(
                rounded_m,
                cbs,
                cached_ld(rounded_m),
                m,
                new_slot,
                trace.child("insert_if_exist_empty_slot"));
            CHECK_NE(rc, kRetry)
                << "** insert_if_exist_empty_slot should do the retry itself.";
            CHECK(rc == kOk || rc == kNoMem);
            return rc;
        }
        else
        {
            CHECK_EQ(rc, kRetry);
            return rc;
        }
    }
    RetCode insert_if_exist_empty_slot(
        size_t subtable_idx,
        const TwoCombinedBucketHandle<kSlotNr> &cb,
        uint32_t ld,
        uint32_t suffix,
        SlotView new_slot,
        util::TraceView trace)
    {
        std::vector<BucketHandle<kSlotNr>> buckets;
        buckets.reserve(4);
        auto rc = cb.get_bucket_handle(buckets);
        if (unlikely(rc == kRdmaProtectionErr))
        {
            return rc;
        }
        CHECK_EQ(rc, kOk);
        for (auto &bucket : buckets)
        {
            RetCode rc;
            if ((rc = bucket.validate_staleness(ld, suffix, trace)) != kOk)
            {
                DCHECK(rc == kCacheStale || rc == kRdmaProtectionErr)
                    << "** unexpected rc: " << rc;
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
                                               *rdma_adpt_,
                                               st_mem_handle,
                                               nullptr,
                                               trace);
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
    bool has_to_insert_block() const
    {
        return !to_insert_block.gaddrs_.empty();
    }
    RetCode prepare_alloc_kv(const Key &key, const Value &value)
    {
        auto rc = RC::kOk;
        if (has_to_insert_block())
        {
            return rc;
        }
        size_t kvblock_size = sizeof(KVBlock) + key.size() + value.size();
        // scale kvblock_size here, because may be overflow by limited bits in
        // tagged ptr
        kvblock_size = std::max(kvblock_size, conf_.kvblock_expect_size);
        size_t tag_ptr_len = len_to_ptr_len(kvblock_size);
        kvblock_size = ptr_len_to_len(tag_ptr_len);
        CHECK_EQ(kvblock_size, conf_.kvblock_expect_size)
            << "** tagged pointer unable to successfully store "
               "kvblock_expect_size: "
            << conf_.kvblock_expect_size << ". tag_ptr_len: " << tag_ptr_len
            << ", convert back to " << kvblock_size << ". Possible overflow";

        to_insert_block.cur_kvblock_size_ = kvblock_size;
        DCHECK_GE(std::numeric_limits<decltype(
                      to_insert_block.cur_tagged_len_)>::max(),
                  tag_ptr_len);
        to_insert_block.cur_tagged_len_ = tag_ptr_len;
        return begin_insert_kvblock(kvblock_size);
    }
    RetCode consume_kv_block()
    {
        DCHECK(!to_insert_block.gaddrs_.empty());
        to_insert_block.gaddrs_.pop();
        if (to_insert_block.gaddrs_.empty())
        {
            const auto &c = conf_.alloc_kvblock;
            rdma_adpt_->relinquish_perm(
                to_insert_block.handle_, c.alloc_hint, c.relinquish_flag);
        }
        return RC::kOk;
    }

    RetCode prepare_write_kv(const Key &key,
                             const Value &value,
                             uint64_t hash,
                             GlobalAddress kv_gaddr,
                             size_t kvblock_size,
                             RemoteMemHandle &kvblock_handle,
                             IRdmaAdaptor &rdma_adpt,
                             util::TraceView trace)
    {
        auto rdma_buf = rdma_adpt.get_rdma_buffer(kvblock_size);
        DCHECK_GE(rdma_buf.size, kvblock_size);
        auto &kv_block = *(KVBlock *) rdma_buf.buffer;
        kv_block.hash = hash;
        kv_block.key_len = key.size();
        kv_block.value_len = value.size();
        memcpy(kv_block.buf, key.data(), key.size());
        memcpy(kv_block.buf + key.size(), value.data(), value.size());

        CHECK(kvblock_handle.valid());

        DLOG_IF(INFO, config::kEnableDebug)
            << "[race][trace] prepare_write_kv: allocate kv_block: " << kv_gaddr
            << ", allocated_size: " << kvblock_size << ". "
            << util::pre_map(trace.kv());

        return rdma_adpt.rdma_write(kv_gaddr,
                                    (char *) rdma_buf.buffer,
                                    kvblock_size,
                                    0 /* flag */,
                                    kvblock_handle);
    }
    using ApplyF = std::function<RetCode(
        const Key &, SlotHandle, KVBlockHandle, TraceView)>;

    RetCode for_the_real_match_do(
        const std::unordered_set<SlotHandle> &slot_handles,
        const Key &key,
        const ApplyF &func,
        util::TraceView trace)
    {
        std::map<SlotHandle, KVBlockHandle> slots_rdma_buffers;
        // TODO: maybe enable batching here. Patronus API?
        // debug_fp_conflict_m_.collect(slot_handles.size());

        // bool has_real_match = false;
        for (auto slot_handle : slot_handles)
        {
            size_t actual_size = slot_handle.slot_view().actual_len_bytes();
            DCHECK_GT(actual_size, 0)
                << "make no sense to have actual size == 0. slot_handle: "
                << slot_handle;
            auto rdma_buffer = rdma_adpt_->get_rdma_buffer(actual_size);
            DCHECK_GE(rdma_buffer.size, actual_size);

            auto remote_kvblock_addr = GlobalAddress(slot_handle.ptr());
            DLOG_IF(INFO, config::kEnableMemoryDebug)
                << "[race][mem] Reading remote kvblock addr: "
                << remote_kvblock_addr << " with size " << actual_size
                << ", fp: " << pre_fp(slot_handle.fp());

            auto &kvblock_handle =
                begin_read_kvblock(remote_kvblock_addr, actual_size);

            CHECK_EQ(rdma_adpt_->rdma_read((char *) rdma_buffer.buffer,
                                           remote_kvblock_addr,
                                           actual_size,
                                           0 /* flag */,
                                           kvblock_handle),
                     kOk);
            slots_rdma_buffers.emplace(
                slot_handle,
                KVBlockHandle(remote_kvblock_addr,
                              (KVBlock *) rdma_buffer.buffer));
        }

        auto ret = rdma_adpt_->commit();
        // no matter what happened, should end_read_kvblock.
        if (unlikely(ret == kRdmaProtectionErr))
        {
            end_read_kvblock();
            return ret;
        }
        CHECK_EQ(ret, kOk);
        end_read_kvblock();

        RetCode rc = kNotFound;
        for (const auto &[slot_handle, kvblock_handle] : slots_rdma_buffers)
        {
            RetCode this_is_real_match =
                is_real_match(kvblock_handle.buffer_addr(), key, trace);
            if (unlikely(conf_.force_kvblock_to_match))
            {
                this_is_real_match = RC::kOk;
            }

            if (this_is_real_match != kOk)
            {
                continue;
            }
            // okay, it is the real match
            // has_real_match = true;
            rc = func(key, slot_handle, kvblock_handle, trace);
            CHECK_NE(rc, kNotFound)
                << "Make no sense to return kNotFound: already found for you.";
            if (unlikely(rc == kRdmaProtectionErr))
            {
                return rc;
            }
        }

        // if (!slot_handles.empty())
        // {
        //     debug_fp_miss_m_.collect(has_real_match);
        // }
        return rc;
    }
    RetCode get(const Key &key,
                Value &value,
                util::TraceView trace = util::nulltrace)
    {
        if (unlikely(!inited_))
        {
            init(trace.child("init"));
        }
        DCHECK(inited_);
        auto hash = hash_impl(key.data(), key.size());
        auto m = hash_m(hash);
        auto fp = hash_fp(hash);
        auto cached_gd = gd();
        auto rounded_m = round_to_bits(m, cached_gd);
        auto [h1, h2] = hash_h1_h2(hash);

        DVLOG(3) << "[race] GET key " << pre(key) << ", got hash "
                 << pre_hash(hash) << ", m: " << m << ", rounded to "
                 << rounded_m << " by cached_gd: " << cached_gd
                 << ". fp: " << pre_fp(fp);
        auto st = subtable_handle(rounded_m);

        // get the two combined buckets the same time
        // validate staleness at the same time
        auto cbs = st.get_two_combined_bucket_handle(h1, h2, *rdma_adpt_);
        rdma_adpt_->commit().expect(RC::kOk);
        trace.pin("get_two_combined_bucket_handle");

        std::unordered_set<SlotHandle> slot_handles;
        auto rc = cbs.locate(fp, cached_ld(rounded_m), m, slot_handles, trace);
        if (rc == kCacheStale && auto_update_dir_)
        {
            // update cache and retry
            update_directory_cache(trace).expect(RC::kOk);
            return get(key, value, trace);
        }
        DLOG_IF(INFO, config::kEnableDebug)
            << "[race][trace] GET from subtable[" << rounded_m << "] from hash "
            << pre_hash(hash) << " and gd " << cached_gd
            << ". Possible match nr: " << slot_handles.size() << ". "
            << util::pre_map(trace.kv());

        return get_from_slot_views(slot_handles, key, value, trace);
    }
    RetCode get_from_slot_views(
        const std::unordered_set<SlotHandle> &slot_handles,
        const Key &key,
        Value &value,
        util::TraceView trace)
    {
        auto f = [&value](const Key &key,
                          SlotHandle slot_handle,
                          KVBlockHandle kvblock_handle,
                          util::TraceView trace) {
            std::ignore = key;
            std::ignore = slot_handle;

            DLOG_IF(INFO, config::kEnableDebug)
                << "[race][trace] get_from_slot_views SUCC: slot_handle "
                << slot_handle << ". " << util::pre_map(trace.kv());
            value.resize(kvblock_handle.value_len());
            memcpy(value.data(),
                   (char *) kvblock_handle.buf() + kvblock_handle.key_len(),
                   kvblock_handle.value_len());
            return kOk;
        };

        return for_the_real_match_do(slot_handles, key, f, trace);
    }

    RetCode update_if_exists(size_t subtable_idx,
                             const std::unordered_set<SlotHandle> &slot_handles,
                             const Key &key,
                             SlotView new_slot,
                             util::TraceView trace)
    {
        auto &st_mem_handle = get_subtable_mem_handle(subtable_idx);
        auto f =
            [&rdma_adpt = *rdma_adpt_.get(), &st_mem_handle, new_slot, this](
                const Key &key,
                SlotHandle slot_handle,
                KVBlockHandle kvblock_handle,
                util::TraceView trace) -> RetCode {
            std::ignore = key;

            uint64_t expect_val = slot_handle.val();
            auto rdma_buf = rdma_adpt.get_rdma_buffer(8);
            DCHECK_GE(rdma_buf.size, 8);
            CHECK_EQ(rdma_adpt.rdma_cas(slot_handle.remote_addr(),
                                        expect_val,
                                        new_slot.val(),
                                        rdma_buf.buffer,
                                        0 /* flag */,
                                        st_mem_handle),
                     kOk);
            auto ret = rdma_adpt.commit();
            if (unlikely(ret == kRdmaProtectionErr))
            {
                return ret;
            }
            CHECK_EQ(ret, kOk);
            bool success =
                memcmp(rdma_buf.buffer, (char *) &expect_val, 8) == 0;
            expect_val = *(uint64_t *) rdma_buf.buffer;
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
                    do_free_kvblock(kvblock_remote_mem);
                }
                DLOG_IF(INFO, config::kEnableDebug)
                    << "[race][subtable] slot " << slot_handle << " update to "
                    << new_slot << util::pre_map(trace.kv());
                trace.pin("OK");
                return kOk;
            }
            DVLOG(4) << "[race][subtable] do_update FAILED: new_slot "
                     << new_slot;
            DLOG_IF(INFO, config::kEnableDebug)
                << "[race][trace][subtable] do_update FAILED: cas failed. slot "
                << slot_handle << util::pre_map(trace.kv());
            trace.pin("Retry");
            return kRetry;
        };

        return for_the_real_match_do(slot_handles, key, f, trace);
    }

    RetCode is_real_match(KVBlock *kvblock,
                          const Key &key,
                          util::TraceView trace)
    {
        std::ignore = trace;
        if (kvblock->key_len != key.size())
        {
            // DVLOG(4) << "[race][subtable] slot_real_match FAILED: key "
            //          << pre(key) << " miss: key len mismatch";
            DLOG_IF(INFO, config::kEnableLocateDebug)
                << "[race][stable] is_real_match FAILED: key len " << key.size()
                << " mismatch with block->key_len: " << kvblock->key_len << ". "
                << util::pre_map(trace.kv());
            return kNotFound;
        }
        if (memcmp(key.data(), kvblock->buf, key.size()) != 0)
        {
            // DVLOG(4) << "[race][subtable] slot_real_match FAILED: key "
            //          << pre(key) << " miss: key content mismatch";
            DLOG_IF(INFO, config::kEnableLocateDebug)
                << "[race][stable] is_real_match FAILED: key content mismatch. "
                   "expect: "
                << key << ", got: "
                << std::string((const char *) kvblock->buf, key.size())
                << util::pre_map(trace.kv());
            return kNotFound;
        }
        // DVLOG(4) << "[race][subtable] slot_real_match SUCCEED: key "
        //          << pre(key);
        DLOG_IF(INFO, config::kEnableLocateDebug)
            << "[race][stable] is_real_match SUCC. "
            << util::pre_map(trace.kv());
        return kOk;
    }

private:
    // OnePassBucketMonitor<uint64_t> debug_fp_conflict_m_{0, 100, 1};
    // OnePassMonitor debug_fp_miss_m_;

    RemoteMemHandle alloc_fake_handle()
    {
        auto ac_flag = (flag_t) AcquireRequestFlag::kNoRpc;
        auto ret = rdma_adpt_->acquire_perm(
            nullgaddr, 0, std::numeric_limits<size_t>::max(), 1ns, ac_flag);
        DCHECK(ret.valid());
        return ret;
    }
    void relinquish_fake_handle(RemoteMemHandle &handle)
    {
        if (handle.valid())
        {
            auto rel_flag = (flag_t) LeaseModifyFlag::kNoRpc;
            rdma_adpt_->relinquish_perm(handle, 0, rel_flag);
        }
    }

    RemoteMemHandle remote_alloc_acquire_subtable_directory(size_t size)
    {
        auto flag = (flag_t) AcquireRequestFlag::kNoGc |
                    (flag_t) AcquireRequestFlag::kWithAllocation |
                    (flag_t) AcquireRequestFlag::kNoBindPR;
        return rdma_adpt_->acquire_perm(
            nullgaddr, conf_.subtable_hint, size, 0ns, flag);
    }
    // the address of hash table at remote side
    uint16_t node_id_;
    GlobalAddress table_meta_addr_;
    MetaT cached_meta_;
    RaceHashingHandleConfig conf_;
    bool auto_expand_;
    bool auto_update_dir_;
    IRdmaAdaptor::pointer rdma_adpt_;
    std::array<RemoteMemHandle, subtable_nr()> subtable_mem_handles_{};
    RemoteMemHandle directory_mem_handle_;

    RemoteMemHandle fake_handle_;

    bool inited_{false};

    // for client private kvblock memory
    GlobalAddress kvblock_pool_gaddr_;

    std::vector<RemoteMemHandle> read_kvblock_handles_;
    RemoteMemHandle kvblock_region_handle_;
    RemoteMemHandle &begin_read_kvblock(GlobalAddress gaddr, size_t size)
    {
        if (conf_.kvblock_region.has_value())
        {
            return begin_read_kvblock_region(gaddr, size);
        }
        else
        {
            return begin_read_kvblock_individual(gaddr, size);
        }
    }
    RemoteMemHandle &begin_read_kvblock_region(
        [[maybe_unused]] GlobalAddress gaddr, [[maybe_unused]] size_t size)
    {
        if (unlikely(!kvblock_region_handle_.valid()))
        {
            const auto &c = conf_.kvblock_region.value();
            kvblock_region_handle_ =
                rdma_adpt_->acquire_perm(cached_meta_.kvblock_pool_gaddr,
                                         c.alloc_hint,
                                         cached_meta_.kvblock_pool_size,
                                         c.ns,
                                         c.acquire_flag);
            DCHECK(kvblock_region_handle_.valid());
        }
        return kvblock_region_handle_;
    }
    RemoteMemHandle &begin_read_kvblock_individual(GlobalAddress gaddr,
                                                   size_t size)
    {
        const auto &c = conf_.read_kvblock;
        read_kvblock_handles_.emplace_back(rdma_adpt_->acquire_perm(
            gaddr, c.alloc_hint, size, c.ns, c.acquire_flag));
        return read_kvblock_handles_.back();
    }
    void end_read_kvblock()
    {
        if (conf_.kvblock_region.has_value())
        {
            return end_read_kvblock_region();
        }
        else
        {
            return end_read_kvblock_individual();
        }
    }
    void end_read_kvblock_region()
    {
    }
    void end_read_kvblock_individual()
    {
        const auto &c = conf_.read_kvblock;
        for (auto &handle : read_kvblock_handles_)
        {
            rdma_adpt_->relinquish_perm(
                handle, c.alloc_hint, c.relinquish_flag);
        }
        read_kvblock_handles_.clear();
    }

    struct ToInsertBlock
    {
        std::optional<GlobalAddress> gaddr_;
        RemoteMemHandle handle_;
        uint8_t tagged_len_;
        size_t kvblock_size_;
    };
    struct
    {
        std::queue<GlobalAddress> gaddrs_;
        RemoteMemHandle handle_;
        uint8_t cur_tagged_len_;
        size_t cur_kvblock_size_;

    } to_insert_block;

    RemoteMemHandle insert_kvblock_batch_handle_;
    GlobalAddress insert_kvblock_batch_gaddr_;
    size_t alloc_idx_{0};

    // insert no batch
    size_t begin_insert_nr_{0};
    size_t end_insert_nr_{0};
    size_t free_insert_nr_{0};
    RetCode begin_insert_kvblock(size_t alloc_size)
    {
        const auto &c = conf_.alloc_kvblock;
        DCHECK(!has_to_insert_block());
        DCHECK(!to_insert_block.handle_.valid());
        size_t batch_alloc_size = alloc_size * conf_.alloc_batch;
        to_insert_block.handle_ = rdma_adpt_->acquire_perm(
            nullgaddr, c.alloc_hint, batch_alloc_size, c.ns, c.acquire_flag);
        begin_insert_nr_++;
        CHECK(to_insert_block.handle_.valid())
            << "** failed to remote_alloc_kvblock for size: " << alloc_size
            << ". Possibly run out of memory. Debug: allocated kvblocks: "
            << begin_insert_nr_ << ", ended: " << end_insert_nr_
            << ", free: " << free_insert_nr_;

        // to_insert_block.gaddr_ = to_insert_block.handle_.gaddr();
        auto begin_gaddr = to_insert_block.handle_.gaddr();
        for (size_t i = 0; i < conf_.alloc_batch; ++i)
        {
            to_insert_block.gaddrs_.push(begin_gaddr + i * alloc_size);
        }
        return RC::kOk;
    }
    void free_insert_kvblock_batch(size_t batch_size)
    {
        CHECK(false) << "TODO: " << batch_size;
    }
    void do_free_kvblock([[maybe_unused]] GlobalAddress gaddr)
    {
        // TODO: this trigger one RPC call for freeing a KV block
        auto rel_flag = (flag_t) LeaseModifyFlag::kServerDoNothing;
        auto place_holder_handle = alloc_fake_handle();
        rdma_adpt_->relinquish_perm(place_holder_handle, 0, rel_flag);
        relinquish_fake_handle(place_holder_handle);
    }

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
        auto st_start_gaddr = subtable_addr(idx);
        DCHECK_EQ(st_mem_handle.gaddr(), st_start_gaddr)
            << "call to get_subtable_mem_handle is expected to be consistent";
        SubTableHandleT st(st_start_gaddr, st_mem_handle);
        return st;
    }
    // guaranteed to be valid and consistent
    RemoteMemHandle &get_subtable_mem_handle(size_t idx)
    {
        DCHECK_LT(idx, subtable_mem_handles_.size());
        auto &ret = subtable_mem_handles_[idx];
        if (unlikely(!ret.valid()))
        {
            build_subtable_mem_handle(idx);
            return get_subtable_mem_handle(idx);
        }
        auto mem_handle_start_gaddr = ret.gaddr();
        auto st_start_gaddr = subtable_addr(idx);
        if (unlikely(mem_handle_start_gaddr != st_start_gaddr))
        {
            // inconsistency detected. rebuild the handle
            build_subtable_mem_handle(idx);
            return get_subtable_mem_handle(idx);
        }
        return ret;
    }
    void build_subtable_mem_handle(size_t idx)
    {
        const auto &c = conf_.meta.d;
        if (unlikely(subtable_mem_handles_[idx].valid()))
        {
            // exist old handle, free it before going on
            rdma_adpt_->relinquish_perm(
                subtable_mem_handles_[idx], c.alloc_hint, c.relinquish_flag);
        }

        DCHECK(!subtable_mem_handles_[idx].valid());
        subtable_mem_handles_[idx] =
            rdma_adpt_->acquire_perm(cached_meta_.entries[idx],
                                     c.alloc_hint,
                                     SubTableT::size_bytes(),
                                     c.ns,
                                     c.acquire_flag);
    }
    // RemoteMemHandle &get_global_kvblock_mem_handle()
    // {
    //     return kvblock_mem_handle_;
    // }
    RemoteMemHandle &get_directory_mem_handle()
    {
        if (unlikely(!directory_mem_handle_.valid()))
        {
            init_directory_mem_handle();
        }
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

    void init_directory_mem_handle()
    {
        const auto &c = conf_.meta.d;
        DCHECK(!directory_mem_handle_.valid());
        DCHECK_NE(table_meta_addr_, nullgaddr);
        directory_mem_handle_ = rdma_adpt_->acquire_perm(table_meta_addr_,
                                                         c.alloc_hint,
                                                         sizeof(MetaT),
                                                         c.ns,
                                                         c.acquire_flag);
        DCHECK(directory_mem_handle_.valid());
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

template <size_t kDEntryNr, size_t kBucketGroupNr, size_t kSlotNr>
class RaceHashingHandleWrapperImpl
{
public:
    using pointer = std::shared_ptr<RaceHashingHandleWrapperImpl>;
    using RaceHashingHandleT =
        RaceHashingHandleImpl<kDEntryNr, kBucketGroupNr, kSlotNr>;
    RaceHashingHandleWrapperImpl(uint16_t node_id,
                                 GlobalAddress table_meta_addr,
                                 const RaceHashingHandleConfig &conf,
                                 bool auto_expand,
                                 IRdmaAdaptor::pointer rdma_adpt)
        : rhh_(node_id, table_meta_addr, conf, auto_expand, rdma_adpt),
          rdma_adpt_(rdma_adpt)
    {
    }
    static pointer new_instance(uint16_t node_id,
                                GlobalAddress table_meta_addr,
                                const RaceHashingHandleConfig &conf,
                                bool auto_expand,
                                IRdmaAdaptor::pointer rdma_adpt)
    {
        return std::make_shared<RaceHashingHandleWrapperImpl>(
            node_id, table_meta_addr, conf, auto_expand, rdma_adpt);
    }
    static size_t max_capacity()
    {
        return RaceHashingHandleT::max_capacity();
    }

    void init(util::TraceView trace = util::nulltrace)
    {
        rhh_.init(trace);
        rdma_adpt_->put_all_rdma_buffer();
    }

    void hack_trigger_rdma_protection_error()
    {
        rhh_.hack_trigger_rdma_protection_error();
    }

    RetCode update_directory_cache(util::TraceView trace)
    {
        rhh_.update_directory_cache(trace);
    }
    RetCode del(const Key &key, util::TraceView trace = util::nulltrace)
    {
        maybe_start_del_trace();

        DLOG_IF(INFO, config::kMonitorRdma)
            << "[race] Started to Deleted key `" << key << "`. "
            << pre_rdma_adaptor(rdma_adpt_);
        auto rc = rhh_.del(key, trace);
        DLOG_IF(INFO, config::kMonitorRdma)
            << "[race] Deleted key `" << key << "`. got " << rc
            << pre_rdma_adaptor(rdma_adpt_);

        rdma_adpt_->put_all_rdma_buffer();

        maybe_end_trace(rc);
        return rc;
    }
    RetCode put(const Key &key,
                const Value &value,
                util::TraceView trace = util::nulltrace)
    {
        maybe_start_put_trace();

        DLOG_IF(INFO, config::kMonitorRdma)
            << "[race] Started to Put key `" << key << "."
            << pre_rdma_adaptor(rdma_adpt_);
        auto rc = rhh_.put(key, value, trace);
        DLOG_IF(INFO, config::kMonitorRdma)
            << "[race] Put key `" << key << "`. got " << rc
            << pre_rdma_adaptor(rdma_adpt_);

        rdma_adpt_->put_all_rdma_buffer();

        maybe_end_trace(rc);
        return rc;
    }
    RetCode get(const Key &key,
                Value &value,
                util::TraceView trace = util::nulltrace)
    {
        maybe_start_get_trace();

        DLOG_IF(INFO, config::kMonitorRdma)
            << "[race] Started to Get key `" << key << "`. "
            << pre_rdma_adaptor(rdma_adpt_);
        auto rc = rhh_.get(key, value, trace);
        DLOG_IF(INFO, config::kMonitorRdma)
            << "[race] Get key `" << key << "` got " << rc << ". "
            << pre_rdma_adaptor(rdma_adpt_);
        rdma_adpt_->put_all_rdma_buffer();

        maybe_end_trace(rc);
        return rc;
    }
    RetCode expand(size_t subtable_idx, util::TraceView trace)
    {
        maybe_start_expand_trace();

        DLOG_IF(INFO, config::kMonitorRdma) << "[race] Started to Expand";
        auto rc = rhh_.expand(subtable_idx, trace);
        DLOG_IF(INFO, config::kMonitorRdma)
            << "[race] expand subtable[" << subtable_idx << "] "
            << pre_rdma_adaptor(rdma_adpt_);

        rdma_adpt_->put_all_rdma_buffer();

        maybe_end_trace(rc);
        return rc;
    }
    void debug_out(std::ostream &os) const
    {
        return rhh_.debug_out(os);
    }

    template <size_t kA, size_t kB, size_t kC>
    friend std::ostream &operator<<(
        std::ostream &os, const RaceHashingHandleWrapperImpl<kA, kB, kC> &rhh);

private:
    RaceHashingHandleT rhh_;
    IRdmaAdaptor::pointer rdma_adpt_;

    void maybe_start_get_trace()
    {
        return maybe_do_start_trace("get", ::config::kRdmaTraceRateGet);
    }
    void maybe_start_put_trace()
    {
        return maybe_do_start_trace("put", ::config::kRdmaTraceRatePut);
    }
    void maybe_start_del_trace()
    {
        return maybe_do_start_trace("del", ::config::kRdmaTraceRateDel);
    }
    void maybe_start_expand_trace()
    {
        return maybe_do_start_trace("expand", ::config::kRdmaTraceRateExpand);
    }
    void maybe_do_start_trace(const char *name, double prob)
    {
        if constexpr (::config::kEnableRdmaTrace)
        {
            if (true_with_prob(prob))
            {
                rdma_adpt_->enable_trace(name);
                rdma_adpt_->trace_pin(name);
            }
        }
    }
    void maybe_end_trace(RetCode rc)
    {
        if constexpr (::config::kEnableRdmaTrace)
        {
            if (unlikely(rdma_adpt_->trace_enabled()))
            {
                rdma_adpt_->trace_pin("finished");
                rdma_adpt_->end_trace(nullptr /* give u nothing */);
                LOG(INFO) << "[trace] result: " << rc << ", "
                          << pre_rdma_adaptor_trace(rdma_adpt_);
            }
        }
    }
};

template <size_t kDEntryNr, size_t kBucketGroupNr, size_t kSlotNr>
inline std::ostream &operator<<(
    std::ostream &os,
    const RaceHashingHandleWrapperImpl<kDEntryNr, kBucketGroupNr, kSlotNr>
        &rhh_wrap)
{
    os << "{" << rhh_wrap.rhh_ << ", " << rhh_wrap.rdma_adpt_ << "}";
    return os;
}

template <size_t kD, size_t kB, size_t kS>
struct pre_rhh_debug
{
    RaceHashingHandleWrapperImpl<kD, kB, kS> *rhh;
};
template <size_t kD, size_t kB, size_t kS>
inline std::ostream &operator<<(std::ostream &os, pre_rhh_debug<kD, kB, kS> rhh)
{
    CHECK_NOTNULL(rhh.rhh)->debug_out(os);
    return os;
}

}  // namespace patronus::hash

#endif