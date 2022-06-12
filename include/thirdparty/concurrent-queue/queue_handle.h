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
        list_handle_ = list::ListHandle<Entry>::new_instance(
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

    RetCode lk_push_back(const T &t, util::TraceView trace = util::nulltrace)
    {
        // 0) update meta data and get the current tail
        if (unlikely(!cached_entry_inited_))
        {
            update_cached_tail_entry();
            CHECK(cached_entry_inited_);
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
            auto &tail_entry = cached_entry();
            // no block at all, or current block is full
            if (unlikely(cached_empty() ||
                         tail_entry.idx >= config_.entry_per_block))
            {
                auto rc = try_expand_update_tail_entry(trace);
                trace.pin("try expand");
                if (unlikely(rc != RC::kOk))
                {
                    CHECK_EQ(rc, RC::kRetry);
                    // concurrent expand detected
                    return rc;
                }
                DCHECK_EQ(rc, RC::kOk);
            }

            // when reach here, we are possible to fetch one
            auto entry_idx_size = sizeof(Entry::idx);
            auto entry_idx_buf = rdma_adpt_->get_rdma_buffer(entry_idx_size);
            auto &entry_handle = get_entry_handle(current_queue_entry_gaddr_);
            auto entry_idx_gaddr = current_queue_entry_gaddr_ +
                                   offsetof(ListEntry, object) +
                                   offsetof(Entry, idx);
            rdma_adpt_
                ->rdma_faa(
                    entry_idx_gaddr, 1, entry_idx_buf.buffer, entry_handle)
                .expect(RC::kOk);
            rdma_adpt_->commit().expect(RC::kOk);
            rdma_adpt_->put_all_rdma_buffer();
            uint64_t got_idx = *(uint64_t *) entry_idx_buf.buffer;
            tail_entry.idx = got_idx;
            fetch_slot = got_idx;
            trace.pin("fetch slot");
        }

        // TODO: delete me, just for sure.
        rdma_adpt_->commit().expect(RC::kOk);
        rdma_adpt_->put_all_rdma_buffer();

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
        auto object_gaddr = current_queue_entry_gaddr_ +
                            offsetof(ListEntry, object) +
                            offsetof(Entry, entries[slot]);
        auto &entry_handle = get_entry_handle(current_queue_entry_gaddr_);
        auto rdma_object_buf = rdma_adpt_->get_rdma_buffer(object_size);
        memcpy(rdma_object_buf.buffer, &t, object_size);
        rdma_adpt_
            ->rdma_write(
                object_gaddr, rdma_object_buf.buffer, object_size, entry_handle)
            .expect(RC::kOk);
        rdma_adpt_->commit().expect(RC::kOk);
        rdma_adpt_->put_all_rdma_buffer();
        trace.pin("write record");

        return RC::kOk;
    }
    RetCode try_expand_update_tail_entry(TraceView trace)
    {
        static Entry push_entry;
        push_entry.idx = 0;
        auto rc = list_handle_->lk_push_back(push_entry,
                                             trace.child("list push back"));
        if (rc == RC::kOk)
        {
            // from list's implementation, the meta has been updated
            const auto &list_meta = list_handle_->cached_meta();
            current_queue_entry_gaddr_ = list_meta.ptail;

            cached_entry_.idx = 0;
            cached_entry_inited_ = true;
        }
        return rc;
    }
    RetCode lk_pop_front(size_t limit_nr,
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

    void update_cached_tail_entry()
    {
        list_handle_->read_meta();
        const auto &list_meta = list_handle_->cached_meta();
        current_queue_entry_gaddr_ = list_meta.ptail;
        // dont rely on list implementation to fetch the tail
        // because we only need a header.
        auto idx_size = sizeof(Entry::idx);
        auto entry_buf = rdma_adpt_->get_rdma_buffer(idx_size);
        auto entry_idx_gaddr = current_queue_entry_gaddr_ +
                               offsetof(ListEntry, object) +
                               offsetof(Entry, idx);
        auto &entry_handle = get_entry_handle(current_queue_entry_gaddr_);
        rdma_adpt_
            ->rdma_read(
                entry_buf.buffer, entry_idx_gaddr, idx_size, entry_handle)
            .expect(RC::kOk);
        rdma_adpt_->commit().expect(RC::kOk);

        cached_entry_.idx = *(uint64_t *) entry_buf.buffer;
        cached_entry_inited_ = true;
    }
    bool cached_empty() const
    {
        return list_handle_->cached_empty();
    }

    RetCode debug()
    {
        return kOk;
    }

    ~QueueHandle()
    {
        auto rel_flag = (flag_t) 0;
        if (lru_entry_handle_.valid())
        {
            rdma_adpt_->relinquish_perm(
                lru_entry_handle_, 0 /* hint */, rel_flag);
        }
    }

private:
    const Entry &cached_entry() const
    {
        return cached_entry_;
    }
    Entry &cached_entry()
    {
        return cached_entry_;
    }

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

    typename list::ListHandle<Entry>::pointer list_handle_;

    Entry cached_entry_;
    bool cached_entry_inited_{false};
    GlobalAddress current_queue_entry_gaddr_;
    RemoteMemHandle lru_entry_handle_;
};
}  // namespace patronus::cqueue

#endif