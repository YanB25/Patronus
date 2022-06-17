#pragma once
#ifndef PATRONUS_LINKED_LIST_HANDLE_IMPL_H_
#define PATRONUS_LINKED_LIST_HANDLE_IMPL_H_

#include "util/RetCode.h"
#include "util/Tracer.h"

namespace patronus::list
{
template <typename T,
          std::enable_if_t<std::is_trivially_copyable_v<T>, bool> = true>
class ListHandleImpl
{
public:
    using pointer = std::shared_ptr<ListHandleImpl<T>>;
    ListHandleImpl(uint16_t node_id,
                   GlobalAddress meta_gaddr,
                   IRdmaAdaptor::pointer rdma_adpt,
                   const ListImplConfig &config)
        : node_id_(node_id), rdma_adpt_(rdma_adpt), config_(config)
    {
        meta.gaddr_ = meta_gaddr;
    }
    static pointer new_instance(uint16_t node_id,
                                GlobalAddress meta,
                                IRdmaAdaptor::pointer rdma_adpt,
                                const ListImplConfig &conf)
    {
        return std::make_shared<ListHandleImpl<T>>(
            node_id, meta, rdma_adpt, conf);
    }
    RetCode func_debug()
    {
        read_meta();
        return kOk;
    }
    [[nodiscard]] RetCode commit()
    {
        return rdma_adpt_->commit();
    }
    [[nodiscard]] RetCode lf_pop_front(T *t,
                                       util::TraceView trace = util::nulltrace)
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

            default_remote_deallocate(old_head);
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

    [[nodiscard]] RetCode lf_push_back_first_node(
        GlobalAddress new_node_gaddr, util::TraceView trace = util::nulltrace)
    {
        auto meta_tail_buf = rdma_adpt_->get_rdma_buffer(sizeof(Meta::ptail));
        rdma_adpt_
            ->rdma_cas(meta_tail_gaddr(),
                       nullgaddr.val,
                       new_node_gaddr.val,
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
            *(GlobalAddress *) meta_head_buf.buffer = new_node_gaddr;
            rdma_adpt_
                ->rdma_cas(meta_head_gaddr(),
                           nullgaddr.val,
                           new_node_gaddr.val,
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
            trace.pin("cas retry");
            return RC::kRetry;
        }
    }

    [[nodiscard]] RetCode lf_push_back_not_first_node(
        GlobalAddress tail_node_gaddr,
        GlobalAddress new_node_gaddr,
        RemoteMemHandle &tail_node_handle,
        util::TraceView trace = util::nulltrace)
    {
        auto old_meta_ptail = tail_node_gaddr;
        auto next_ptr_buf =
            rdma_adpt_->get_rdma_buffer(sizeof(ListNode<T>::next));
        auto tail_next_ptr_gaddr =
            tail_node_gaddr + offsetof(ListNode<T>, next);
        rdma_adpt_
            ->rdma_cas(tail_next_ptr_gaddr,
                       nullgaddr.val,
                       new_node_gaddr.val,
                       next_ptr_buf.buffer,
                       tail_node_handle)
            .expect(RC::kOk);
        rdma_adpt_->commit().expect(RC::kOk);
        GlobalAddress got_tail_node_next =
            *(GlobalAddress *) next_ptr_buf.buffer;
        trace.pin("cas meta.front");

        RetCode rc;
        if (got_tail_node_next == nullgaddr)
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
                               new_node_gaddr.val,
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

        if (rc == kOk)
        {
            trace.pin("ok");
        }
        else
        {
            trace.pin("retry");
        }

        return rc;
    }

    [[nodiscard]] RetCode lf_push_back(GlobalAddress node,
                                       util::TraceView trace = util::nulltrace)
    {
        read_meta();
        trace.pin("read");

        auto &meta = cached_meta();
        if (unlikely(meta.phead == nullgaddr && meta.ptail == nullgaddr))
        {
            return lf_push_back_first_node(node, trace.child("first node"));
        }
        else
        {
            auto tail_node_gaddr = meta.ptail;
            auto tail_node_handle = get_entry_mem_handle(tail_node_gaddr);
            auto rc =
                lf_push_back_not_first_node(tail_node_gaddr,
                                            node,
                                            tail_node_handle,
                                            trace.child("not first node"));
            relinquish_entry_mem_handle(tail_node_handle);
            return rc;
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

    [[nodiscard]] RemoteMemHandle allocate_to_insert_node()
    {
        return default_remote_allocate(sizeof(ListNode<T>));
    }

    [[nodiscard]] RetCode lk_push_back(GlobalAddress new_entry_gaddr,
                                       util::TraceView trace = util::nulltrace)
    {
        auto rc = lock_push();
        trace.pin("lock_push");

        if (rc != kOk)
        {
            DCHECK_EQ(rc, kRetry);
            return rc;
        }

        // 2.2) read meta for the latest ptail
        auto read_meta_rdma_buf = prepare_read_meta();
        rdma_adpt_->commit().expect(RC::kOk);
        post_commit_read_meta(read_meta_rdma_buf);
        auto &meta = cached_meta();
        trace.pin("write record, read meta");

        // 3) update all the pointers
        // 3.1) update the tail_entry->next
        auto tail_node_gaddr = meta.ptail;
        auto &tail_node_handle = get_tail_node_handle(tail_node_gaddr);
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
        trace.pin("write entry->next + write meta and unlock " +
                  std::to_string(new_entry_gaddr.val));

        rdma_adpt_->put_all_rdma_buffer();
        return kOk;
    }

    // TODO: make it a cache, so that we wont relinquish it each time.
    [[nodiscard]] RemoteMemHandle get_entry_mem_handle(
        GlobalAddress entry_gaddr)
    {
        return default_acquire_perm(entry_gaddr, sizeof(ListNode<T>));
    }
    void relinquish_entry_mem_handle(RemoteMemHandle &handle)
    {
        default_relinquish_handle(handle);
    }

    [[nodiscard]] RetCode lk_pop_front(T *t,
                                       util::TraceView trace = util::nulltrace)
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
        default_remote_deallocate(old_phead);
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
            auto handle =
                default_acquire_perm(cur_list_node_gaddr, sizeof(ListNode<T>));
            rdma_adpt_
                ->rdma_read(list_node_buf.buffer,
                            cur_list_node_gaddr,
                            sizeof(ListNode<T>),
                            handle)
                .expect(RC::kOk);
            rdma_adpt_->commit().expect(RC::kOk);
            default_relinquish_handle(handle);

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

    Meta debug_meta()
    {
        auto &meta_handle = get_meta_handle();
        auto rdma_buf = rdma_adpt_->get_rdma_buffer(meta_size());
        rdma_adpt_
            ->rdma_read(rdma_buf.buffer, meta_gaddr(), meta_size(), meta_handle)
            .expect(RC::kOk);
        rdma_adpt_->commit().expect(RC::kOk);

        Meta ret;
        memcpy(&ret, rdma_buf.buffer, meta_size());
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
    [[nodiscard]] Buffer prepare_read_meta()
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

    ~ListHandleImpl()
    {
        relinquish_meta_handle();
        if (tail_node.handle_.valid())
        {
            default_relinquish_handle(tail_node.handle_);
        }
        for (auto &[_, handle] : handles_)
        {
            std::ignore = _;
            default_relinquish_handle(handle);
        }
    }
    [[nodiscard]] RemoteMemHandle default_acquire_perm(GlobalAddress gaddr,
                                                       size_t size)
    {
        const auto &c = config_.rdma.default_;
        return rdma_adpt_->acquire_perm(
            gaddr, c.alloc_hint, size, c.ns, c.acquire_flag);
    }
    void default_relinquish_handle(RemoteMemHandle &handle)
    {
        if (handle.valid())
        {
            const auto &c = config_.rdma.default_;
            rdma_adpt_->relinquish_perm(
                handle, c.alloc_hint, c.relinquish_flag);
        }
    }

private:
    RemoteMemHandle meta_acquire_perm(GlobalAddress gaddr, size_t size)
    {
        const auto &c = config_.rdma.meta_;
        return rdma_adpt_->acquire_perm(
            gaddr, c.alloc_hint, size, c.ns, c.acquire_flag);
    }
    void meta_relinquish_handle(RemoteMemHandle &handle)
    {
        if (handle.valid())
        {
            const auto &c = config_.rdma.meta_;
            rdma_adpt_->relinquish_perm(
                handle, c.alloc_hint, c.relinquish_flag);
        }
    }
    [[nodiscard]] RemoteMemHandle default_remote_allocate(size_t size)
    {
        const auto &c = config_.rdma.alloc_;
        return rdma_adpt_->acquire_perm(
            nullgaddr, c.alloc_hint, size, c.ns, c.acquire_flag);
    }

    void default_remote_deallocate(GlobalAddress gaddr)
    {
        LOG_FIRST_N(WARNING, 1) << "[list] ignore remote free " << gaddr;
    }

    [[nodiscard]] RemoteMemHandle &get_meta_handle()
    {
        if (unlikely(!meta.handle_.valid()))
        {
            meta.handle_ = meta_acquire_perm(meta.gaddr_, meta_size());
            CHECK(meta.handle_.valid());
        }
        return meta.handle_;
    }
    void relinquish_meta_handle()
    {
        if (meta.handle_.valid())
        {
            meta_relinquish_handle(meta.handle_);
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
    [[nodiscard]] RetCode lock_push()
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
    [[nodiscard]] RetCode lock_pop()
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
    [[nodiscard]] RemoteMemHandle &get_handle_impl(uint64_t id,
                                                   GlobalAddress gaddr,
                                                   size_t size)
    {
        auto &ret = handles_[id];
        if (unlikely(!ret.valid()))
        {
            ret = default_acquire_perm(gaddr, size);
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
    IRdmaAdaptor::pointer rdma_adpt_;
    ListImplConfig config_;

    struct
    {
        GlobalAddress gaddr_{nullgaddr};
        Meta cache_;
        RemoteMemHandle handle_;
        bool inited_{false};
    } meta;

    struct
    {
        RemoteMemHandle handle_;
    } tail_node;
    [[nodiscard]] RemoteMemHandle &get_tail_node_handle(GlobalAddress gaddr)
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

    struct MemDesc
    {
        GlobalAddress gaddr;
        size_t size;
    };
    std::unordered_map<uint64_t, MemDesc> debug_handle_cache_validate_;
};
}  // namespace patronus::list

#endif