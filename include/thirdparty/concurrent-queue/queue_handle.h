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
    size_t entry_per_block{1024};
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

    RetCode lk_push_back(const T &t, util::TraceView trace = util::nulltrace)
    {
        std::ignore = trace;
        // 0) update meta data and get the current tail
        if (unlikely(!cached_entry_inited_))
        {
            update_cached_tail_entry();
            CHECK(cached_entry_inited_);
        }

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
            const auto &tail_entry = cached_entry();
            // this block is full
            if (unlikely(tail_entry.idx >= config_.entry_per_block))
            {
                auto rc = try_expand_update_tail_entry();
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
            auto entry_idx_gaddr =
                current_queue_entry_gaddr_ + offsetof(Entry, idx);
            rdma_adpt_
                ->rdma_faa(
                    entry_idx_gaddr, 1, entry_idx_buf.buffer, entry_handle)
                .expect(RC::kOk);
            rdma_adpt_->commit().expect(RC::kOk);
            rdma_adpt_->put_all_rdma_buffer();
            fetch_slot = *(uint64_t *) entry_idx_buf.buffer;
        }

        if (!fetch_slot.has_value() ||
            fetch_slot.value() >= config_.entry_per_block)
        {
            return RC::kRetry;
        }

        // do the insertion
        auto slot = fetch_slot.value();
        auto object_size = sizeof(T);
        auto object_gaddr =
            current_queue_entry_gaddr_ + offsetof(Entry, entries[slot]);
        auto &entry_handle = get_entry_handle(current_queue_entry_gaddr_);
        auto rdma_object_buf = rdma_adpt_->get_rdma_buffer(object_size);
        memcpy(rdma_object_buf.buffer, &t, object_size);
        rdma_adpt_
            ->rdma_write(
                object_gaddr, rdma_object_buf.buffer, object_size, entry_handle)
            .expect(RC::kOk);
        rdma_adpt_->commit().expect(RC::kOk);
        rdma_adpt_->put_all_rdma_buffer();

        return RC::kOk;
    }
    RetCode try_expand_update_tail_entry()
    {
        static Entry push_entry;
        push_entry.idx = 0;
        auto rc = list_handle_->lk_push_back(push_entry);
        if (rc == RC::kOk)
        {
            // from list's implementation, the meta has been updated
            const auto &list_meta = list_handle_->cached_meta();
            current_queue_entry_gaddr_ = list_meta.ptail;
        }
        return rc;
    }
    RetCode lk_pop_front(T *t, util::TraceView trace = util::nulltrace)
    {
        CHECK(false) << "TODO: " << sizeof(t) << trace;
        return kOk;
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
        auto entry_idx_gaddr =
            current_queue_entry_gaddr_ + offsetof(Entry, idx);
        auto &entry_handle = get_entry_handle(current_queue_entry_gaddr_);
        rdma_adpt_
            ->rdma_read(
                entry_buf.buffer, entry_idx_gaddr, idx_size, entry_handle)
            .expect(RC::kOk);
        rdma_adpt_->commit().expect(RC::kOk);

        cached_entry_.idx = *(uint64_t *) entry_buf.buffer;
        cached_entry_inited_ = true;
    }

    RetCode debug()
    {
        return kOk;
    }

    ~QueueHandle()
    {
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
        else
        {
            // definitely not valid and not match when reach here.
            auto ac_flag = (flag_t) AcquireRequestFlag::kNoGc |
                           (flag_t) AcquireRequestFlag::kNoBindPR;
            lru_entry_handle_ = rdma_adpt_->acquire_perm(
                gaddr, 0 /* hint */, sizeof(Entry), 0ns, ac_flag);
            DCHECK_EQ(lru_entry_handle_.gaddr(), gaddr);
            DCHECK(lru_entry_handle_.valid());
        }
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