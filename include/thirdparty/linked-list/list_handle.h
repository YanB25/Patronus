#pragma once
#ifndef PATRONUS_LINKED_LIST_HANDLE_H_
#define PATRONUS_LINKED_LIST_HANDLE_H_

#include "util/RetCode.h"
#include "util/Tracer.h"

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

    /**
     * @brief try to link the list_node
     *
     * @param list_node
     * @return RetCode
     */
    RetCode lf_push_back(const T &t, util::TraceView trace = util::nulltrace)
    {
        if (unlikely(!has_to_insert_node()))
        {
            allocate_to_insert_node().expect(RC::kOk);
        }
        read_meta();

        prepare_write_to_node(t,
                              to_insert_node_gaddr(),
                              nullgaddr /* next */,
                              to_insert_node_handle(),
                              trace)
            .expect(RC::kOk);
        rdma_adpt_->commit().expect(RC::kOk);

        return lf_link_back(to_insert_node_gaddr());
    }

    RetCode lf_link_back(GlobalAddress node)
    {
        auto tail_node_gaddr = meta.ptail;
        GlobalAddress old_ptail = meta.ptail;
        auto entry_handle = get_entry_mem_handle(tail_node_gaddr);
        auto next_ptr_buf =
            rdma_adpt_->get_rdma_buffer(sizeof(ListNode<T>::next));
        auto tail_next_ptr_gaddr =
            tail_node_gaddr + offsetof(ListNode<T>, next);
        rdma_adpt_
            ->rdma_cas(tail_next_ptr_gaddr,
                       nullgaddr,
                       node,
                       next_ptr_buf.buffer,
                       entry_handle)
            .expect(RC::kOk);
        rdma_adpt_->commit().expect(RC::kOk);
        GlobalAddress old_next = *(GlobalAddress *) next_ptr_buf.buffer;
        RetCode rc;
        if (old_next == nullgaddr)
        {
            // succeeded
            // update meta
            rc = RC::kOk;
            rdma_adpt_
                ->rdma_cas(meta_tail_gaddr(),
                           old_ptail,
                           node,
                           // use the buffer again, should be safe
                           next_ptr_buf.buffer,
                           get_meta_handle())
                .expect(RC::kOk);
            rdma_adpt_->commit().expect(RC::kOk);
            GlobalAddress old_meta_ptail =
                *(GlobalAddress *) next_ptr_buf.buffer;
            CHECK_EQ(old_meta_ptail, old_ptail)
                << "** The CAS has to be successful.";
        }
        else
        {
            rc = RC::kRetry;
        }

        relinquish_entry_mem_handle(entry_handle);
        return rc;
    }
    RetCode prepare_write_to_node(const T &t,
                                  GlobalAddress node_gaddr,
                                  GlobalAddress next_gaddr,
                                  RemoteMemHandle &node_handle)
    {
        auto object_rdma_buf = rdma_adpt_->get_rdma_buffer(sizeof(ListNode<T>));
        auto &list_item = *(ListNode<T> *) object_rdma_buf.buffer;
        list_item.next = next_gaddr;
        list_item.object = t;
        return rdma_adpt_->rdma_write(node_gaddr,
                                      object_rdma_buf.buffer,
                                      sizeof(ListNode<T>),
                                      node_handle);
    }

    bool has_to_insert_node() const
    {
        return lf.to_insert_node_gaddr_.has_value();
    }
    RetCode allocate_to_insert_node()
    {
        LOG_IF(WARNING, has_to_insert_node())
            << "to_insert_node is not null. Allocating another one will leak "
               "the previous one.";
        // drop out the old ones.
        auto rc = consume_to_insert_node();
        DCHECK(rc == RC::kOk || rc == RC::kNotFound);

        auto ac_flag = (flag_t) AcquireRequestFlag::kWithAllocation |
                       (flag_t) AcquireRequestFlag::kNoGc |
                       (flag_t) AcquireRequestFlag::kNoBindPR;
        lf.to_insert_node_handle_ = rdma_adpt_->acquire_perm(
            nullgaddr, 0 /* hint */, sizeof(ListNode<T>), 0ns, ac_flag);
        DCHECK(lf.to_insert_node_handle_.valid());
        lf.to_insert_node_gaddr_ = lf.to_insert_node_handle_.gaddr();
        return RC::kOk;
    }
    GlobalAddress to_insert_node_gaddr()
    {
        DCHECK(lf.to_insert_node_gaddr_.has_value());
        return lf.to_insert_node_gaddr_.value();
    }
    RemoteMemHandle &to_insert_node_handle()
    {
        DCHECK(lf.to_insert_node_gaddr_.has_value())
            << "** does not have to_insert_node.";
        return lf.to_insert_node_handle_;
    }
    RetCode consume_to_insert_node()
    {
        if (unlikely(!has_to_insert_node()))
        {
            return RC::kNotFound;
        }
        lf.to_insert_node_gaddr_ = std::nullopt;
        default_relinquish_handle(lf.to_insert_node_handle_);
        return RC::kOk;
    }

    // TODO: expose an API which handles the initialization of t
    // so that reduces IO of the network
    RetCode lk_push_back(const T &t, util::TraceView trace = util::nulltrace)
    {
        auto rc = lock_push();
        trace.pin("lock_push");

        if (rc != kOk)
        {
            DCHECK_EQ(rc, kRetry);
            return rc;
        }

        // 1) alloc: use acquire_perm with allocation semantics
        if (unlikely(!has_to_insert_node()))
        {
            allocate_to_insert_node().expect(RC::kOk);
        }
        trace.pin("acquire new_entry");

        // 2.1) write to the allocated record
        auto new_entry_gaddr = to_insert_node_gaddr();
        auto &new_entry_handle = to_insert_node_handle();
        prepare_write_to_node(t, new_entry_gaddr, nullgaddr, new_entry_handle)
            .expect(RC::kOk);

        // 2.2) read meta for the latest ptail
        auto read_meta_rdma_buf = prepare_read_meta();
        rdma_adpt_->commit().expect(RC::kOk);
        post_commit_read_meta(read_meta_rdma_buf);
        auto &meta = cached_meta();
        trace.pin("write record, read meta");

        // 3) update all the pointers
        // 3.1) update the tail_entry->next
        auto tail_node_gaddr = meta.ptail;
        auto tail_node_handle = get_tail_node_handle(tail_node_gaddr);
        trace.pin("acquire entry");
        auto next_ptr_buf =
            rdma_adpt_->get_rdma_buffer(sizeof(ListNode<T>::next));
        *(GlobalAddress *) next_ptr_buf.buffer = new_entry_gaddr;
        auto tail_next_ptr_gaddr =
            tail_node_gaddr + offsetof(ListNode<T>, next);
        rdma_adpt_
            ->rdma_write(tail_next_ptr_gaddr,
                         next_ptr_buf.buffer,
                         sizeof(ListNode<T>::next),
                         tail_node_handle)
            .expect(RC::kOk);
        // 3.2) update the meta->ptail, and meta->phead if necessary
        meta.ptail = new_entry_gaddr;
        if (unlikely(meta.phead == nullgaddr))
        {
            meta.phead = new_entry_gaddr;
        }
        prepare_write_meta();

        // 3.3) unlock
        prepare_unlock_push();
        // 3.4) commit
        rdma_adpt_->commit().expect(RC::kOk);
        rdma_adpt_->put_all_rdma_buffer();
        trace.pin("write entry->next + write meta and unlock");

        // 4) offload all the relinquish out of critical path.
        consume_to_insert_node().expect(RC::kOk);
        trace.pin("relinquish new_entry");
        rdma_adpt_->put_all_rdma_buffer();
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

    // ListNode<T> &cached_node()
    // {
    //     return cached_node_;
    // }
    // const ListNode<T> &cached_node() const
    // {
    //     return cached_node_;
    // }
    RetCode lk_pop_front(T *t, util::TraceView trace = util::nulltrace)
    {
        auto rc = lock_pop();
        if (rc != kOk)
        {
            DCHECK_EQ(rc, kRetry);
            return rc;
        }
        trace.pin("lock pop");
        // 0) update meta
        read_meta();
        auto &meta = cached_meta();
        trace.pin("read meta");
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
        trace.pin("read entry");

        // 2.1) update meta phead, and ptail if needed.
        auto old_phead = meta.phead;
        meta.phead = head_next;
        if (unlikely(meta.phead == nullgaddr))
        {
            // this pop make the list empty
            meta.ptail = nullgaddr;
        }
        prepare_write_meta();

        // 2.2) unlock
        prepare_unlock_pop();
        // 2.3) commit
        rdma_adpt_->commit().expect(RC::kOk);
        rdma_adpt_->put_all_rdma_buffer();
        trace.pin("write meta + unlock");

        // 3) relinquish & free
        rdma_adpt_->relinquish_perm(entry_handle, 0 /* hint */, 0 /* flag */);
        remote_free_list_node(old_phead);
        rdma_adpt_->put_all_rdma_buffer();
        trace.pin("rel entry");

        return kOk;
    }
    using Visitor = std::function<bool(const T &)>;
    void lf_visit(const Visitor &visit)
    {
        read_meta();
        const auto &meta = cached_meta();
        if (unlikely(meta.phead == nullgaddr))
        {
            return;
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

            bool should_continue = visit(object);
            if (!should_continue)
            {
                break;
            }
            cur_list_node_gaddr = list_node.next;
            if (unlikely(cur_list_node_gaddr == nullgaddr))
            {
                break;
            }
        }
        rdma_adpt_->put_all_rdma_buffer();
        return;
    }
    std::list<T> debug_iterator()
    {
        std::list<T> ret;

        lf_visit([&ret](const T &t) {
            ret.push_back(t);
            return true;
        });

        return ret;
    }

    void read_meta()
    {
        auto rdma_buffer = prepare_read_meta();
        rdma_adpt_->commit().expect(RC::kOk);
        post_commit_read_meta(rdma_buffer);

        rdma_adpt_->put_all_rdma_buffer();
    }
    Buffer prepare_read_meta()
    {
        auto &meta_handle = get_meta_handle();
        auto rdma_buf = rdma_adpt_->get_rdma_buffer(meta_size());
        rdma_adpt_
            ->rdma_read(rdma_buf.buffer, meta_gaddr(), meta_size(), meta_handle)
            .expect(RC::kOk);
        return rdma_buf;
    }
    void post_commit_read_meta(Buffer rdma_buf)
    {
        memcpy(&meta.cache_, rdma_buf.buffer, meta_size());
        meta.inited_ = true;
    }

    void write_meta()
    {
        prepare_write_meta();
        rdma_adpt_->commit().expect(RC::kOk);
        rdma_adpt_->put_all_rdma_buffer();
    }

    void prepare_write_meta()
    {
        auto &meta_handle = get_meta_handle();
        auto rdma_buf = rdma_adpt_->get_rdma_buffer(meta_size());
        const auto &m = cached_meta();
        memcpy(rdma_buf.buffer, &m, meta_size());
        rdma_adpt_
            ->rdma_write(
                meta_gaddr(), rdma_buf.buffer, meta_size(), meta_handle)
            .expect(RC::kOk);
    }
    Meta &cached_meta()
    {
        return meta.cache_;
    }
    const Meta &cached_meta() const
    {
        return meta.cache_;
    }
    size_t meta_size() const
    {
        return Meta::size();
    }

    bool cached_empty() const
    {
        return cached_meta().phead == nullgaddr;
    }

    ~ListHandle()
    {
        auto rel_flag = (flag_t) 0;
        relinquish_meta_handle();
        if (tail_node.handle_.valid())
        {
            default_relinquish_handle(tail_node.handle_);
        }
        if (lf.to_insert_node_handle_.valid())
        {
            default_relinquish_handle(lf.to_insert_node_handle_);
        }
        for (auto &[_, handle] : handles_)
        {
            std::ignore = _;
            rdma_adpt_->relinquish_perm(handle, 0 /* hint */, rel_flag);
        }
    }

private:
    void remote_free_list_node(GlobalAddress gaddr)
    {
        LOG_FIRST_N(WARNING, 1) << "[list] ignore remote free " << gaddr;
    }

    RemoteMemHandle &get_meta_handle()
    {
        if (unlikely(!meta.handle_.valid()))
        {
            meta.handle_ = default_acquire_perm(meta.gaddr_, meta_size());
            CHECK(meta.handle_.valid());
        }
        return meta.handle_;
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
        if (handle.valid())
        {
            rdma_adpt_->relinquish_perm(handle, 0 /* hint */, 0 /* flag */);
        }
    }
    void relinquish_meta_handle()
    {
        if (meta.handle_.valid())
        {
            default_relinquish_handle(meta.handle_);
        }
    }
    GlobalAddress meta_gaddr() const
    {
        return meta.gaddr_;
    }
    GlobalAddress meta_tail_gaddr() const
    {
        return meta_gaddr() + offsetof(Meta, ptail);
    }
    GlobalAddress meta_head_gaddr() const
    {
        return meta_gaddr() + offsetof(Meta, phead);
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
        prepare_unlock_push();
        rdma_adpt_->commit().expect(RC::kOk);
        rdma_adpt_->put_all_rdma_buffer();
    }
    void prepare_unlock_push()
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
    void prepare_unlock_pop()
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
    }
    void unlock_pop()
    {
        prepare_unlock_pop();
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

    struct
    {
        GlobalAddress gaddr_{nullgaddr};
        Meta cache_;
        RemoteMemHandle handle_;
        bool inited_{false};
    } meta;

    struct
    {
        // std::optional<T> cache_obj_;
        // std::optional<GlobalAddress> cached_next_;
        RemoteMemHandle handle_;
    } tail_node;
    RemoteMemHandle &get_tail_node_handle(GlobalAddress gaddr)
    {
        if (tail_node.handle_.valid())
        {
            if (tail_node.handle_.gaddr() == gaddr)
            {
                // hit
                return tail_node.handle_;
            }
            else
            {
                default_relinquish_handle(tail_node.handle_);
            }
        }
        DCHECK(!tail_node.handle_.valid());
        tail_node.handle_ = default_acquire_perm(gaddr, sizeof(ListNode<T>));
        return tail_node.handle_;
    }

    std::unordered_map<uint64_t, RemoteMemHandle> handles_;

    struct
    {
        std::optional<GlobalAddress> to_insert_node_gaddr_;
        RemoteMemHandle to_insert_node_handle_;
    } lf;

    struct MemDesc
    {
        GlobalAddress gaddr;
        size_t size;
    };
    std::unordered_map<uint64_t, MemDesc> debug_handle_cache_validate_;
};
}  // namespace patronus::list

#endif