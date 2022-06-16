#pragma once
#ifndef PATRONUS_CONCURRENT_QUEUE_HANDLE_H_
#define PATRONUS_CONCURRENT_QUEUE_HANDLE_H_

#include "thirdparty/linked-list/list_handle.h"
#include "util/RetCode.h"
#include "util/Tracer.h"

namespace patronus::cqueue
{
struct HandleConfig
{
    std::string name;
    bool bypass_prot{false};
    size_t entry_per_block{};
    list::HandleConfig list_handle_config;

    std::string conf_name() const
    {
        return name;
    }
};

inline std::ostream &operator<<(std::ostream &os, const HandleConfig &config)
{
    os << "{HandleConfig name: " << config.name
       << ", bypass_prot: " << config.bypass_prot
       << ", entry_per_block: " << config.entry_per_block << "}";
    return os;
}

// any T that behaves correctly under memcpy(&t, ..., sizeof(T))
template <typename T,
          size_t kSize,
          std::enable_if_t<std::is_trivially_copyable_v<T>, bool> = true>
class QueueHandle
{
public:
    using pointer = std::shared_ptr<QueueHandle<T, kSize>>;
    using Entry = QueueEntry<T, kSize>;
    using ListEntry = list::ListNode<Entry>;
    QueueHandle(uint16_t node_id,
                GlobalAddress meta,
                IRdmaAdaptor::pointer rdma_adpt,
                const HandleConfig &config)
        : node_id_(node_id),
          meta_gaddr_(meta),
          rdma_adpt_(rdma_adpt),
          config_(config)
    {
        list_handle_ = list::ListHandleImpl<Entry>::new_instance(
            node_id, meta, rdma_adpt, config.list_handle_config);
    }
    static pointer new_instance(uint16_t node_id,
                                GlobalAddress meta,
                                IRdmaAdaptor::pointer rdma_adpt,
                                const HandleConfig &conf)
    {
        return std::make_shared<QueueHandle<T, kSize>>(
            node_id, meta, rdma_adpt, conf);
    }
    using Visitor = std::function<bool(const T &)>;
    void lf_visit(const Visitor &visit)
    {
        list_handle_->lf_visit([&](const Entry &entry) {
            auto size = std::min(entry.idx, config_.entry_per_block);
            for (size_t i = 0; i < size; ++i)
            {
                bool cont = visit(entry.entries[i]);
                if (!cont)
                {
                    return false;
                }
            }
            return true;
        });
    }
    std::list<T> debug_iterator()
    {
        std::list<T> ret;
        lf_visit([&](const T &t) {
            ret.push_back(t);
            return true;
        });
        return ret;
    }

    bool has_cached_tail_node() const
    {
        return cached_tail_node.gaddr_.has_value();
    }

    RetCode lf_push_back(const T &t, util::TraceView trace = util::nulltrace)
    {
        // 0) update meta data and get the current tail
        if (unlikely(!has_cached_tail_node()))
        {
            update_cached_tail_node();
            DCHECK(has_cached_tail_node());
        }
        trace.pin("update cached tail entry");

        // loop until we find an empty slot
        std::optional<uint64_t> fetch_slot;
        // we only retry at most 10 times
        for (size_t retry_nr = 0; retry_nr <= 10; retry_nr++)
        {
            if (fetch_slot.has_value() &&
                fetch_slot.value() < config_.entry_per_block)
            {
                // we got that
                break;
            }
            // no block at all, or current block is full
            if (unlikely(cached_empty() ||
                         cached_tail_node.idx_ >= config_.entry_per_block))
            {
                auto rc = try_expand_update_tail_entry(trace);
                trace.pin("try expand");
                if (unlikely(rc != RC::kOk))
                {
                    CHECK_EQ(rc, RC::kRetry);
                    // NOTE: if expand failed, we should redo from the very
                    // beginning, i.e., reread the meta therefore, we return
                    // kRetry here.
                    return rc;
                }
                DCHECK_EQ(rc, RC::kOk);
            }

            // when reach here, we are possible to fetch one
            auto idx_size = sizeof(Entry::idx);
            auto tail_node_idx_buf = rdma_adpt_->get_rdma_buffer(idx_size);
            auto &tail_node_handle = cached_tail_node.handle_;
            auto tail_node_idx_gaddr = cached_tail_node.gaddr_.value() +
                                       offsetof(ListEntry, object) +
                                       offsetof(Entry, idx);
            rdma_adpt_
                ->rdma_faa(tail_node_idx_gaddr,
                           1,
                           tail_node_idx_buf.buffer,
                           tail_node_handle)
                .expect(RC::kOk);
            rdma_adpt_->commit().expect(RC::kOk);
            uint64_t got_idx = *(uint64_t *) tail_node_idx_buf.buffer;
            cached_tail_node.idx_ = got_idx;
            rdma_adpt_->put_all_rdma_buffer();

            fetch_slot = got_idx;
            trace.pin("fetch slot");
        }

        // fail to fetch valid slot after @retry_nr attempts
        if (!fetch_slot.has_value() ||
            fetch_slot.value() >= config_.entry_per_block)
        {
            return RC::kRetry;
        }

        // do the insertion
        DCHECK(fetch_slot.has_value());
        auto slot = fetch_slot.value();
        DCHECK_LT(slot, config_.entry_per_block);
        auto object_size = sizeof(T);
        auto object_gaddr = cached_tail_node.gaddr_.value() +
                            offsetof(ListEntry, object) +
                            offsetof(Entry, entries[slot]);
        auto rdma_object_buf = rdma_adpt_->get_rdma_buffer(object_size);
        memcpy(rdma_object_buf.buffer, &t, object_size);
        rdma_adpt_
            ->rdma_write(object_gaddr,
                         rdma_object_buf.buffer,
                         object_size,
                         cached_tail_node.handle_)
            .expect(RC::kOk);
        rdma_adpt_->commit().expect(RC::kOk);
        rdma_adpt_->put_all_rdma_buffer();
        trace.pin("write record");

        return RC::kOk;
    }
    RetCode try_expand_update_tail_entry(TraceView trace)
    {
        fill_to_insert_node_if_needed();
        DCHECK(to_insert_node.gaddr_.has_value());
        auto rc = list_handle_->lf_push_back(to_insert_node.gaddr_.value(),
                                             trace.child("list push back"));
        if (rc == RC::kOk)
        {
            set_cached_tail_node(to_insert_node.gaddr_.value(),
                                 std::move(to_insert_node.handle_),
                                 0);
            to_insert_node.gaddr_ = std::nullopt;
        }

        return rc;
    }
    void set_cached_tail_node(GlobalAddress gaddr,
                              RemoteMemHandle &&handle,
                              uint64_t idx)
    {
        cached_tail_node.gaddr_ = gaddr;
        if (unlikely(cached_tail_node.handle_.valid()))
        {
            default_relinquish_handle(cached_tail_node.handle_);
        }
        cached_tail_node.handle_ = std::move(handle);
        cached_tail_node.idx_ = idx;
    }
    RetCode lf_pop_front(size_t limit_nr,
                         T *t,
                         size_t &get_nr,
                         util::TraceView trace = util::nulltrace)
    {
        static Entry pop_entry;
        auto rc = list_handle_->lk_pop_front(&pop_entry, trace);
        if (rc != kOk)
        {
            return rc;
        }
        if (t != nullptr)
        {
            auto size = std::min(pop_entry.idx, config_.entry_per_block);
            for (size_t i = 0; i < size; ++i)
            {
                CHECK_LT(i, limit_nr)
                    << "** buffer overflow. use larger buffer.";
                *(t + i) = pop_entry.entry(i);
                get_nr = i + 1;
            }
        }
        else
        {
            DCHECK_EQ(limit_nr, 0) << "** parameter not consistent";
        }
        return pop_entry.idx > 0 ? kOk : kNotFound;
    }

    void update_cached_tail_node()
    {
        list_handle_->read_meta();
        const auto &list_meta = list_handle_->cached_meta();
        auto known_tail_node_gaddr = list_meta.ptail;

        if (cached_tail_node.gaddr_.has_value())
        {
            if (cached_tail_node.gaddr_.value() == known_tail_node_gaddr)
            {
                // okay, should be latest
                return;
            }
            else
            {
                // stale
                default_relinquish_handle(cached_tail_node.handle_);
                cached_tail_node.idx_ = 0;
                cached_tail_node.gaddr_ = std::nullopt;
            }
        }

        cached_tail_node.gaddr_ = known_tail_node_gaddr;
        cached_tail_node.handle_ = default_acquire_perm(
            cached_tail_node.gaddr_.value(), sizeof(ListEntry));

        auto idx_size = sizeof(Entry::idx);
        auto tail_node_idx_buf = rdma_adpt_->get_rdma_buffer(idx_size);
        auto tail_node_idx_gaddr = cached_tail_node.gaddr_.value() +
                                   offsetof(ListEntry, object) +
                                   offsetof(Entry, idx);
        rdma_adpt_
            ->rdma_read(tail_node_idx_buf.buffer,
                        tail_node_idx_gaddr,
                        idx_size,
                        cached_tail_node.handle_)
            .expect(RC::kOk);
        rdma_adpt_->commit().expect(RC::kOk);
        cached_tail_node.idx_ = *(uint64_t *) tail_node_idx_buf.buffer;

        rdma_adpt_->put_all_rdma_buffer();
    }
    bool cached_empty() const
    {
        return list_handle_->cached_empty();
    }
    RemoteMemHandle default_acquire_perm(GlobalAddress gaddr, size_t size)
    {
        auto ac_flag = (flag_t) AcquireRequestFlag::kNoGc |
                       (flag_t) AcquireRequestFlag::kNoBindPR;
        return rdma_adpt_->acquire_perm(
            gaddr, 0 /* hint */, size, 0ns, ac_flag);
    }
    void default_relinquish_handle(RemoteMemHandle &handle)
    {
        rdma_adpt_->relinquish_perm(handle, 0 /* hint */, 0 /* flag */);
    }

    RetCode debug()
    {
        return kOk;
    }

    ~QueueHandle()
    {
        if (lru_entry_handle_.valid())
        {
            default_relinquish_handle(lru_entry_handle_);
        }
        if (to_insert_node.handle_.valid())
        {
            default_relinquish_handle(to_insert_node.handle_);
        }
    }

private:
    RemoteMemHandle &get_entry_handle(GlobalAddress gaddr)
    {
        if (lru_entry_handle_.valid())
        {
            if (lru_entry_handle_.gaddr() == gaddr)
            {
                // hit
                DCHECK(lru_entry_handle_.valid());
                return lru_entry_handle_;
            }
            // miss, evit
            if (lru_entry_handle_.gaddr() != gaddr)
            {
                auto rel_flag = (flag_t) 0;
                rdma_adpt_->relinquish_perm(
                    lru_entry_handle_, 0 /* hint */, rel_flag);
            }
        }

        // definitely not valid and not match when reach here.
        auto ac_flag = (flag_t) AcquireRequestFlag::kNoGc |
                       (flag_t) AcquireRequestFlag::kNoBindPR;
        lru_entry_handle_ = rdma_adpt_->acquire_perm(
            gaddr, 0 /* hint */, sizeof(ListEntry), 0ns, ac_flag);
        DCHECK_EQ(lru_entry_handle_.gaddr(), gaddr);
        DCHECK(lru_entry_handle_.valid());
        return lru_entry_handle_;
    }

    uint16_t node_id_;
    GlobalAddress meta_gaddr_;
    IRdmaAdaptor::pointer rdma_adpt_;
    HandleConfig config_;

    typename list::ListHandleImpl<Entry>::pointer list_handle_;
    struct
    {
        std::optional<GlobalAddress> gaddr_;
        RemoteMemHandle handle_;
        uint64_t idx_;  // best effort
    } cached_tail_node;

    struct
    {
        std::optional<GlobalAddress> gaddr_;
        RemoteMemHandle handle_;
    } to_insert_node;

    void fill_to_insert_node_if_needed()
    {
        if (likely(to_insert_node.gaddr_.has_value()))
        {
            return;
        }
        if (unlikely(to_insert_node.handle_.valid()))
        {
            default_relinquish_handle(to_insert_node.handle_);
        }
        to_insert_node.handle_ = list_handle_->allocate_to_insert_node();
        DCHECK(to_insert_node.handle_.valid());
        to_insert_node.gaddr_ = to_insert_node.handle_.gaddr();

        // NOTE: prepare to write to idx, but not commit
        auto to_insert_node_idx_gaddr = to_insert_node.gaddr_.value() +
                                        offsetof(ListEntry, object) +
                                        offsetof(Entry, idx);
        size_t idx_size = sizeof(Entry::idx);
        static_assert(std::is_same_v<decltype(Entry::idx), uint64_t>);
        auto idx_buf = rdma_adpt_->get_rdma_buffer(idx_size);
        *(uint64_t *) idx_buf.buffer = 0;
        rdma_adpt_
            ->rdma_write(to_insert_node_idx_gaddr,
                         idx_buf.buffer,
                         idx_size,
                         to_insert_node.handle_)
            .expect(RC::kOk);
    }

    RemoteMemHandle lru_entry_handle_;
};
}  // namespace patronus::cqueue

#endif