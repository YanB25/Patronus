#pragma once
#ifndef PATRONUS_LINKED_LIST_HANDLE_H_
#define PATRONUS_LINKED_LIST_HANDLE_H_

#include "util/RetCode.h"

namespace patronus::list
{
struct HandleConfig
{
    std::string name;
    bool bypass_prot{false};

    std::string conf_name() const
    {
        return name;
    }
};

inline std::ostream &operator<<(std::ostream &os, const HandleConfig &config)
{
    os << "{HandleConfig name: " << config.name
       << ", bypass_prot: " << config.bypass_prot << "}";
    return os;
}

template <typename T,
          std::enable_if_t<std::is_trivially_copyable_v<T>, bool> = true>
class ListHandle
{
public:
    using pointer = std::shared_ptr<ListHandle<T>>;
    ListHandle(uint16_t node_id,
               GlobalAddress meta,
               IRdmaAdaptor::pointer rdma_adpt,
               const HandleConfig &config)
        : node_id_(node_id),
          meta_gaddr_(meta),
          rdma_adpt_(rdma_adpt),
          config_(config)
    {
    }
    static pointer new_instance(uint16_t node_id,
                                GlobalAddress meta,
                                IRdmaAdaptor::pointer rdma_adpt,
                                const HandleConfig &conf)
    {
        return std::make_shared<ListHandle<T>>(node_id, meta, rdma_adpt, conf);
    }
    RetCode func_debug()
    {
        read_meta();
        return kOk;
    }

    RetCode lf_push_back(const T &t);
    RetCode lf_pop_front(const T &t);
    RetCode lk_push_back(const T &t)
    {
        auto rc = lock_push();
        if (rc != kOk)
        {
            DCHECK_EQ(rc, kRetry);
            return rc;
        }

        // 1.1) alloc: use acquire_perm with allocation semantics
        auto ac_new_entry_flag = (flag_t) AcquireRequestFlag::kWithAllocation |
                                 (flag_t) AcquireRequestFlag::kNoGc |
                                 (flag_t) AcquireRequestFlag::kNoBindPR;
        auto new_entry_handle = rdma_adpt_->acquire_perm(nullgaddr,
                                                         0 /* hint */,
                                                         sizeof(ListNode<T>),
                                                         0ns,
                                                         ac_new_entry_flag);
        auto new_entry_gaddr = new_entry_handle.gaddr();
        // 1.2) write to the allocated record
        auto object_rdma_buf = rdma_adpt_->get_rdma_buffer(sizeof(ListNode<T>));
        auto &list_item = *(ListNode<T> *) object_rdma_buf.buffer;
        list_item.object = t;
        list_item.next = nullgaddr;
        rdma_adpt_
            ->rdma_write(new_entry_gaddr,
                         object_rdma_buf.buffer,
                         sizeof(ListNode<T>),
                         new_entry_handle)
            .expect(RC::kOk);
        rdma_adpt_->commit().expect(RC::kOk);
        // 1.3) relinquish the handle
        auto rel_new_entry_flag =
            (flag_t) LeaseModifyFlag::kNoRelinquishUnbindPr;
        rdma_adpt_->put_all_rdma_buffer();

        // 2) read meta for the latest ptail
        read_meta();
        auto &meta = cached_meta();

        // 3) update all the pointers
        // 3.1) update the tail_entry->next
        auto tail_node_gaddr = meta.ptail;
        auto entry_handle = get_entry_mem_handle(tail_node_gaddr);
        auto next_ptr_buf =
            rdma_adpt_->get_rdma_buffer(sizeof(ListNode<T>::next));
        *(GlobalAddress *) next_ptr_buf.buffer = new_entry_gaddr;
        auto tail_next_ptr_gaddr =
            tail_node_gaddr + offsetof(ListNode<T>, next);
        rdma_adpt_
            ->rdma_write(tail_next_ptr_gaddr,
                         next_ptr_buf.buffer,
                         sizeof(ListNode<T>::next),
                         entry_handle)
            .expect(RC::kOk);
        // 3.2) update the meta->ptail, and meta->phead if necessary
        meta.ptail = new_entry_gaddr;
        if (unlikely(meta.phead == nullgaddr))
        {
            meta.phead = new_entry_gaddr;
        }
        auto &meta_handle = get_meta_handle();
        auto meta_rdma_buf = rdma_adpt_->get_rdma_buffer(meta_size());
        memcpy(meta_rdma_buf.buffer, &meta, meta_size());
        rdma_adpt_
            ->rdma_write(
                meta_gaddr(), meta_rdma_buf.buffer, meta_size(), meta_handle)
            .expect(RC::kOk);
        rdma_adpt_->commit().expect(RC::kOk);
        rdma_adpt_->put_all_rdma_buffer();

        // 3) update the tail to point correctly
        unlock_push();

        // 4) offload all the relinquish out of critical path.
        rdma_adpt_->relinquish_perm(
            new_entry_handle, 0 /* hint */, rel_new_entry_flag);
        rdma_adpt_->relinquish_perm(entry_handle, 0 /* hint */, 0 /* flag */);
        return kOk;
    }

    RemoteMemHandle get_entry_mem_handle(GlobalAddress entry_gaddr)
    {
        return rdma_adpt_->acquire_perm(
            entry_gaddr,
            0 /* hint */,
            sizeof(ListNode<T>),
            0ns,
            (flag_t) AcquireRequestFlag::kNoGc |
                (flag_t) AcquireRequestFlag::kNoBindPR);
    }

    ListNode<T> &cached_node()
    {
        return cached_node_;
    }
    const ListNode<T> &cached_node() const
    {
        return cached_node_;
    }
    RetCode lk_pop_front(T *t)
    {
        auto rc = lock_pop();
        if (rc != kOk)
        {
            DCHECK_EQ(rc, kRetry);
            return rc;
        }
        // 0) update meta
        read_meta();
        auto &meta = cached_meta();
        if (unlikely(meta.phead == nullgaddr || meta.ptail == nullgaddr))
        {
            // is full
            CHECK_EQ(meta.ptail, nullgaddr);
            CHECK_EQ(meta.phead, nullgaddr);
            unlock_pop();
            return RC::kNotFound;
        }

        // 1) read entry = *meta_.phead
        // or GlobalAddress head_next = meta_.phead->next
        auto head_gaddr = meta.phead;
        auto entry_handle = get_entry_mem_handle(head_gaddr);
        GlobalAddress head_next = nullgaddr;
        if (likely(t != nullptr))
        {
            // also read object
            auto node_size = sizeof(ListNode<T>);
            auto entry_buf = rdma_adpt_->get_rdma_buffer(node_size);
            rdma_adpt_
                ->rdma_read(
                    entry_buf.buffer, head_gaddr, node_size, entry_handle)
                .expect(RC::kOk);
            rdma_adpt_->commit().expect(RC::kOk);
            const auto &node = *(ListNode<T> *) entry_buf.buffer;
            *t = node.object;
            head_next = node.next;
        }
        else
        {
            auto head_next_gaddr = head_gaddr + offsetof(ListNode<T>, next);
            auto head_next_ptr_buf =
                rdma_adpt_->get_rdma_buffer(sizeof(ListNode<T>::next));
            rdma_adpt_
                ->rdma_read(head_next_ptr_buf.buffer,
                            head_next_gaddr,
                            sizeof(ListNode<T>::next),
                            entry_handle)
                .expect(RC::kOk);
            rdma_adpt_->commit().expect(RC::kOk);
            head_next = *(GlobalAddress *) head_next_ptr_buf.buffer;
        }
        rdma_adpt_->put_all_rdma_buffer();

        // 2) update meta phead
        auto old_phead = meta.phead;
        meta.phead = head_next;
        write_meta();

        // 3) unlock
        unlock_pop();

        // 5) relinquish & free
        rdma_adpt_->relinquish_perm(entry_handle, 0 /* hint */, 0 /* flag */);
        remote_free_list_node(old_phead);
        rdma_adpt_->put_all_rdma_buffer();

        return kOk;
    }
    std::list<T> debug_iterator()
    {
        std::list<T> ret;

        read_meta();
        const auto &meta = cached_meta();
        if (unlikely(meta.phead == nullgaddr))
        {
            return ret;
        }

        auto list_node_buf = rdma_adpt_->get_rdma_buffer(sizeof(ListNode<T>));
        auto cur_list_node_gaddr = meta.phead;
        while (true)
        {
            auto handle = rdma_adpt_->acquire_perm(
                cur_list_node_gaddr,
                0 /* hint */,
                sizeof(ListNode<T>),
                0ns,
                (flag_t) AcquireRequestFlag::kNoGc |
                    (flag_t) AcquireRequestFlag::kNoBindPR);
            rdma_adpt_
                ->rdma_read(list_node_buf.buffer,
                            cur_list_node_gaddr,
                            sizeof(ListNode<T>),
                            handle)
                .expect(RC::kOk);
            rdma_adpt_->commit().expect(RC::kOk);
            rdma_adpt_->relinquish_perm(handle, 0 /* hint */, 0 /* flag */);

            const auto &list_node = *(ListNode<T> *) list_node_buf.buffer;
            const auto &object = list_node.object;
            ret.push_back(object);

            cur_list_node_gaddr = list_node.next;
            if (unlikely(cur_list_node_gaddr == nullgaddr))
            {
                break;
            }
        }
        rdma_adpt_->put_all_rdma_buffer();
        return ret;
    }

    void remote_free_list_node(GlobalAddress gaddr)
    {
        LOG_FIRST_N(WARNING, 1) << "[list] ignore remote free " << gaddr;
    }

    void read_meta()
    {
        auto &meta_handle = get_meta_handle();
        auto rdma_buf = rdma_adpt_->get_rdma_buffer(meta_size());
        rdma_adpt_
            ->rdma_read(rdma_buf.buffer, meta_gaddr_, meta_size(), meta_handle)
            .expect(RC::kOk);
        rdma_adpt_->commit().expect(RC::kOk);
        memcpy(&cached_meta_, rdma_buf.buffer, meta_size());

        rdma_adpt_->put_all_rdma_buffer();
        cached_inited_ = true;
    }
    void write_meta()
    {
        auto &meta_handle = get_meta_handle();
        auto rdma_buf = rdma_adpt_->get_rdma_buffer(meta_size());
        memcpy(rdma_buf.buffer, &cached_meta_, meta_size());
        rdma_adpt_
            ->rdma_write(meta_gaddr_, rdma_buf.buffer, meta_size(), meta_handle)
            .expect(RC::kOk);
        rdma_adpt_->commit().expect(RC::kOk);
        rdma_adpt_->put_all_rdma_buffer();
    }

    Meta &cached_meta()
    {
        return cached_meta_;
    }
    const Meta &cached_meta() const
    {
        return cached_meta_;
    }
    size_t meta_size() const
    {
        return Meta::size();
    }

    ~ListHandle()
    {
        if (meta_handle_.valid())
        {
            auto rel_flag = (flag_t) 0;
            rdma_adpt_->relinquish_perm(meta_handle_, 0 /* hint */, rel_flag);
        }
    }

private:
    RemoteMemHandle &get_meta_handle()
    {
        if (unlikely(!meta_handle_.valid()))
        {
            update_meta_handle();
        }
        return meta_handle_;
    }
    void update_meta_handle()
    {
        CHECK(!meta_handle_.valid());
        auto ac_flag = (flag_t) AcquireRequestFlag::kNoGc |
                       (flag_t) AcquireRequestFlag::kNoBindPR;
        meta_handle_ = rdma_adpt_->acquire_perm(
            meta_gaddr_, 0 /* alloc_hint */, meta_size(), 0ns, ac_flag);
        CHECK(meta_handle_.valid());
    }
    GlobalAddress meta_gaddr() const
    {
        return meta_gaddr_;
    }
    GlobalAddress meta_tail_gaddr() const
    {
        return meta_gaddr_ + offsetof(Meta, ptail);
    }
    GlobalAddress meta_head_gaddr() const
    {
        return meta_gaddr_ + offsetof(Meta, phead);
    }
    RetCode lock_push()
    {
        auto &push_lock_handle = get_push_lock_handle();
        auto rdma_buf = rdma_adpt_->get_rdma_buffer(sizeof(uint64_t));
        rdma_adpt_
            ->rdma_cas(
                push_lock_gaddr(), 0, 1, rdma_buf.buffer, push_lock_handle)
            .expect(RC::kOk);
        rdma_adpt_->commit().expect(RC::kOk);
        rdma_adpt_->put_all_rdma_buffer();

        uint64_t got = *(uint64_t *) rdma_buf.buffer;
        DCHECK(got == 0 || got == 1)
            << "got invalid lock value: " << got << ". Not 0 or 1.";

        if (got == 0)
        {
            return kOk;
        }
        else
        {
            return kRetry;
        }
    }
    void unlock_push()
    {
        auto &push_lock_handle = get_push_lock_handle();
        auto rdma_buf = rdma_adpt_->get_rdma_buffer(sizeof(uint64_t));
        *((uint64_t *) rdma_buf.buffer) = 0;

        rdma_adpt_
            ->rdma_write(push_lock_gaddr(),
                         rdma_buf.buffer,
                         sizeof(uint64_t),
                         push_lock_handle)
            .expect(RC::kOk);
        rdma_adpt_->commit().expect(RC::kOk);
        rdma_adpt_->put_all_rdma_buffer();
    }
    RetCode lock_pop()
    {
        auto &pop_lock_handle = get_pop_lock_handle();
        auto rdma_buf = rdma_adpt_->get_rdma_buffer(sizeof(uint64_t));
        auto rc = rdma_adpt_->rdma_cas(
            pop_lock_gaddr(), 0, 1, rdma_buf.buffer, pop_lock_handle);
        CHECK_EQ(rc, kOk);
        rc = rdma_adpt_->commit();
        CHECK_EQ(rc, kOk);
        rdma_adpt_->put_all_rdma_buffer();

        uint64_t got = *(uint64_t *) rdma_buf.buffer;
        DCHECK(got == 0 || got == 1)
            << "got invalid lock value: " << got << ". Not 0 or 1.";

        if (got == 0)
        {
            return kOk;
        }
        else
        {
            return kRetry;
        }
    }
    void unlock_pop()
    {
        auto &pop_lock_handle = get_pop_lock_handle();
        auto rdma_buf = rdma_adpt_->get_rdma_buffer(sizeof(uint64_t));
        *((uint64_t *) rdma_buf.buffer) = 0;

        rdma_adpt_
            ->rdma_write(pop_lock_gaddr(),
                         rdma_buf.buffer,
                         sizeof(uint64_t),
                         pop_lock_handle)
            .expect(RC::kOk);
        rdma_adpt_->commit().expect(RC::kOk);
        rdma_adpt_->put_all_rdma_buffer();
    }

    GlobalAddress push_lock_gaddr() const
    {
        return meta_gaddr() + offsetof(Meta, push_lock);
    }
    GlobalAddress pop_lock_gaddr() const
    {
        return meta_gaddr() + offsetof(Meta, pop_lock);
    }
    RemoteMemHandle &get_push_lock_handle()
    {
        return get_handle_impl(0, push_lock_gaddr(), sizeof(uint64_t));
    }
    RemoteMemHandle &get_pop_lock_handle()
    {
        return get_handle_impl(1, pop_lock_gaddr(), sizeof(uint64_t));
    }
    RemoteMemHandle &get_handle_impl(uint64_t id,
                                     GlobalAddress gaddr,
                                     size_t size)
    {
        auto &ret = handles_[id];
        if (unlikely(!ret.valid()))
        {
            auto ac_flag = (flag_t) AcquireRequestFlag::kNoGc |
                           (flag_t) AcquireRequestFlag::kNoBindPR;
            ret = rdma_adpt_->acquire_perm(gaddr, 0, size, 0ns, ac_flag);
        }
        if constexpr (debug())
        {
            if (debug_handle_cache_validate_.count(id) == 0)
            {
                debug_handle_cache_validate_[id] =
                    MemDesc{.gaddr = gaddr, .size = size};
            }
            else
            {
                const auto &old = debug_handle_cache_validate_[id];
                CHECK_EQ(old.gaddr, gaddr);
                CHECK_EQ(old.size, size);
            }
        }
        return ret;
    }

    uint16_t node_id_;
    GlobalAddress meta_gaddr_;
    IRdmaAdaptor::pointer rdma_adpt_;
    HandleConfig config_;

    bool cached_inited_{false};
    Meta cached_meta_;

    ListNode<T> cached_node_;

    std::unordered_map<uint64_t, RemoteMemHandle> handles_;

    RemoteMemHandle meta_handle_;

    struct MemDesc
    {
        GlobalAddress gaddr;
        size_t size;
    };
    std::unordered_map<uint64_t, MemDesc> debug_handle_cache_validate_;
};
}  // namespace patronus::list

#endif