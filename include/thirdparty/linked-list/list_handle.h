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

    /**
     * @brief try to link the list_node
     *
     * @param list_node
     * @return RetCode
     */
    RetCode lf_push_back(const T &t, util::TraceView trace = util::nulltrace)
    {
        // 0) init of allocated new node
        if (unlikely(!has_to_insert_node()))
        {
            allocate_to_insert_node().expect(RC::kOk);
            trace.pin("allocate to insert node");
            prepare_write_to_node(t,
                                  to_insert_node_gaddr(),
                                  nullgaddr /* next */,
                                  to_insert_node_handle())
                .expect(RC::kOk);
            rdma_adpt_->commit().expect(RC::kOk);
            trace.pin("write to allocated node");
        }

        for (size_t i = 0; i < 10; ++i)
        {
            auto rc = lf_link_back(to_insert_node_gaddr(),
                                   trace.child(std::to_string(i)));
            if (rc == kOk)
            {
                consume_to_insert_node().expect(RC::kOk);
                return RC::kOk;
            }
            CHECK_EQ(rc, RC::kRetry);
        }
        return RC::kRetry;
    }
    RetCode lf_pop_front(T *t, util::TraceView trace = util::nulltrace)
    {
        // 0) read meta
        read_meta();
        trace.pin("read meta");

        // 1) get GlobalAddress next = front->next;
        const auto &meta = cached_meta();
        if (unlikely(meta.phead == nullgaddr))
        {
            CHECK_EQ(meta.ptail, nullgaddr);
            return RC::kNotFound;
        }
        GlobalAddress old_head = meta.phead;
        auto front_node_gaddr = meta.phead;
        auto front_node_next_gaddr =
            front_node_gaddr + offsetof(ListNode<T>, next);
        auto front_node_handle = get_entry_mem_handle(front_node_gaddr);
        size_t node_next_size = sizeof(ListNode<T>::next);
        auto front_node_next_buf = rdma_adpt_->get_rdma_buffer(node_next_size);
        rdma_adpt_
            ->rdma_read(front_node_next_buf.buffer,
                        front_node_next_gaddr,
                        node_next_size,
                        front_node_handle)
            .expect(RC::kOk);
        rdma_adpt_->commit().expect(RC::kOk);
        GlobalAddress node_next = *(GlobalAddress *) front_node_next_buf.buffer;
        rdma_adpt_->put_all_rdma_buffer();

        trace.pin("read front->next");

        // 2) CAS meta.pfront to next
        auto meta_pfront_buffer =
            rdma_adpt_->get_rdma_buffer(sizeof(Meta::phead));
        rdma_adpt_
            ->rdma_cas(meta_head_gaddr(),
                       old_head.val,
                       node_next.val,
                       meta_pfront_buffer.buffer,
                       get_meta_handle())
            .expect(RC::kOk);
        rdma_adpt_->commit().expect(RC::kOk);
        GlobalAddress got_head = *(GlobalAddress *) meta_pfront_buffer.buffer;
        rdma_adpt_->put_all_rdma_buffer();
        trace.pin("cas meta.pfront");

        RetCode rc;
        if (got_head == old_head)
        {
            rc = kOk;
            if (unlikely(node_next == nullgaddr))
            {
                // the last item. also update tail
                CHECK_EQ(got_head, meta.ptail)
                    << "If it is the last node, the previous head should also "
                       "be the tail";

                auto rdma_tail_buf =
                    rdma_adpt_->get_rdma_buffer(sizeof(Meta::ptail));
                *(GlobalAddress *) rdma_tail_buf.buffer = nullgaddr;
                rdma_adpt_
                    ->rdma_write(meta_tail_gaddr(),
                                 rdma_tail_buf.buffer,
                                 sizeof(Meta::ptail),
                                 get_meta_handle())
                    .expect(RC::kOk);
                rdma_adpt_->commit().expect(RC::kOk);
                rdma_adpt_->put_all_rdma_buffer();
                trace.pin("also update meta.ptail");
            }

            remote_free_list_node(old_head);
            trace.pin("remote free node");
            if (t)
            {
                auto object_buf = rdma_adpt_->get_rdma_buffer(sizeof(T));
                auto object_gaddr = old_head + offsetof(ListNode<T>, object);
                rdma_adpt_
                    ->rdma_read(object_buf.buffer,
                                object_gaddr,
                                sizeof(T),
                                front_node_handle)
                    .expect(RC::kOk);
                rdma_adpt_->commit().expect(RC::kOk);
                rdma_adpt_->put_all_rdma_buffer();
                memcpy(t, object_buf.buffer, sizeof(T));
                trace.pin("read back object");
            }
        }
        else
        {
            rc = kRetry;
        }
        relinquish_entry_mem_handle(front_node_handle);
        trace.pin("relinquish");
        return rc;
    }

    RetCode lf_link_back_first_node(GlobalAddress node,
                                    util::TraceView trace = util::nulltrace)
    {
        const auto &meta = cached_meta();
        auto tail_node_gaddr = meta.ptail;
        DCHECK_EQ(tail_node_gaddr, nullgaddr) << "** precondition violated";
        auto meta_tail_buf = rdma_adpt_->get_rdma_buffer(sizeof(Meta::ptail));
        rdma_adpt_
            ->rdma_cas(meta_tail_gaddr(),
                       nullgaddr.val,
                       node.val,
                       meta_tail_buf.buffer,
                       get_meta_handle())
            .expect(RC::kOk);
        rdma_adpt_->commit().expect(RC::kOk);
        GlobalAddress got_ptail = *(GlobalAddress *) meta_tail_buf.buffer;
        rdma_adpt_->put_all_rdma_buffer();
        trace.pin("cas meta.ptail");
        if (got_ptail == nullgaddr)
        {
            // cas succeeded
            auto meta_head_buf =
                rdma_adpt_->get_rdma_buffer(sizeof(Meta::phead));
            *(GlobalAddress *) meta_head_buf.buffer = node;
            rdma_adpt_
                ->rdma_cas(meta_head_gaddr(),
                           nullgaddr.val,
                           node.val,
                           meta_head_buf.buffer,
                           get_meta_handle())
                .expect(RC::kOk);
            rdma_adpt_->commit().expect(RC::kOk);
            GlobalAddress got_phead = *(GlobalAddress *) meta_head_buf.buffer;
            CHECK_EQ(got_phead, nullgaddr)
                << "** the CAS here should succeed, because only the one "
                   "succeeded in CAS-ing meta.ptail can reach here";
            rdma_adpt_->put_all_rdma_buffer();
            trace.pin("cas meta.phead");
            return RC::kOk;
        }
        else
        {
            return RC::kRetry;
        }
    }
    RetCode lf_link_back_not_first_node(GlobalAddress node,
                                        util::TraceView trace = util::nulltrace)
    {
        const auto &meta = cached_meta();
        auto tail_node_gaddr = meta.ptail;
        GlobalAddress old_meta_ptail = meta.ptail;
        auto tail_node_handle = get_entry_mem_handle(tail_node_gaddr);
        trace.pin("get entry handle");

        auto next_ptr_buf =
            rdma_adpt_->get_rdma_buffer(sizeof(ListNode<T>::next));
        auto tail_next_ptr_gaddr =
            tail_node_gaddr + offsetof(ListNode<T>, next);
        rdma_adpt_
            ->rdma_cas(tail_next_ptr_gaddr,
                       nullgaddr.val,
                       node.val,
                       next_ptr_buf.buffer,
                       tail_node_handle)
            .expect(RC::kOk);
        rdma_adpt_->commit().expect(RC::kOk);
        GlobalAddress got_tail_node_next =
            *(GlobalAddress *) next_ptr_buf.buffer;
        trace.pin("cas meta.front");

        RetCode rc;
        if (unlikely(got_tail_node_next == nullgaddr))
        {
            rc = RC::kOk;

            auto meta_ptail_buf =
                rdma_adpt_->get_rdma_buffer(sizeof(Meta::ptail));
            size_t cas_tried{0};
            while (true)
            {
                rdma_adpt_
                    ->rdma_cas(meta_tail_gaddr(),
                               old_meta_ptail.val,
                               node.val,
                               meta_ptail_buf.buffer,
                               get_meta_handle())
                    .expect(RC::kOk);
                rdma_adpt_->commit().expect(RC::kOk);
                GlobalAddress got_meta_ptail =
                    *(GlobalAddress *) meta_ptail_buf.buffer;
                trace.pin("cas meta.tail");
                if (got_meta_ptail == old_meta_ptail)
                {
                    // cas succeeded
                    break;
                }
                else
                {
                    // cas failed
                    // wait for other clients to update the pre-condition
                    cas_tried++;
                    LOG_IF(INFO, cas_tried)
                        << "** Failed to CAS meta for 100 tries. " << cas_tried
                        << "-th. expect: " << old_meta_ptail << ", got "
                        << got_meta_ptail;
                    continue;
                }
            }
        }
        else
        {
            rc = kRetry;
        }

        relinquish_entry_mem_handle(tail_node_handle);
        trace.pin("relinquish entry handle");
        return rc;
    }

    RetCode lf_link_back(GlobalAddress node,
                         util::TraceView trace = util::nulltrace)
    {
        read_meta();
        trace.pin("read");

        auto &meta = cached_meta();
        if (unlikely(meta.phead == nullgaddr && meta.ptail == nullgaddr))
        {
            return lf_link_back_first_node(node, trace.child("first node"));
        }
        else
        {
            return lf_link_back_not_first_node(node,
                                               trace.child("not first node"));
        }
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
            << "to_insert_node is not null. Allocating another one will "
               "leak "
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
    void relinquish_entry_mem_handle(RemoteMemHandle &handle)
    {
        rdma_adpt_->relinquish_perm(handle, 0 /* hint */, 0 /* flag */);
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
        relinquish_entry_mem_handle(entry_handle);
        remote_free_list_node(old_phead);
        rdma_adpt_->put_all_rdma_buffer();
        trace.pin("rel entry");

        return kOk;
    }
    using Visitor = std::function<bool(const T &)>;
    void lf_visit(const Visitor &visit, TraceView trace = util::nulltrace)
    {
        read_meta(trace);
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
            trace.pin("Collect " + std::to_string(cur_list_node_gaddr.val));
        }
        rdma_adpt_->put_all_rdma_buffer();
        return;
    }
    std::list<T> debug_iterator(TraceView trace = util::nulltrace)
    {
        std::list<T> ret;

        lf_visit(
            [&ret](const T &t) {
                ret.push_back(t);
                return true;
            },
            trace);

        return ret;
    }

    void read_meta(TraceView trace = util::nulltrace)
    {
        auto rdma_buffer = prepare_read_meta();
        rdma_adpt_->commit().expect(RC::kOk);
        post_commit_read_meta(rdma_buffer);

        rdma_adpt_->put_all_rdma_buffer();
        trace.pin("read_meta");
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