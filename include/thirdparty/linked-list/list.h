#pragma once
#ifndef PATRONUS_LINKED_LIST_H_
#define PATRONUS_LINKED_LIST_H_
#include "./conf.h"
#include "./meta.h"
#include "GlobalAddress.h"
#include "util/IRdmaAdaptor.h"

namespace patronus::list
{
struct ListConfig
{
};

inline std::ostream &operator<<(std::ostream &os, const ListConfig &config)
{
    std::ignore = config;
    os << "{ListConfig }";
    return os;
}

template <typename T>
struct ListNode
{
    T object;
    GlobalAddress next;
};

template <typename T>
class List
{
public:
    using pointer = std::shared_ptr<List<T>>;
    List(IRdmaAdaptor::pointer rdma_adpt,
         patronus::mem::IAllocator::pointer allocator,
         const ListConfig &config)
        : rdma_adpt_(rdma_adpt), allocator_(allocator), conf_(config)
    {
        init_meta();
        setup_meta();
    }
    static pointer new_instance(IRdmaAdaptor::pointer rdma_adpt,
                                patronus::mem::IAllocator::pointer allocator,
                                const ListConfig &config)
    {
        return std::make_shared<List<T>>(rdma_adpt, allocator, config);
    }

    GlobalAddress meta_gaddr() const
    {
        return to_exposed_remote_mem(meta_addr());
    }
    const Meta &meta() const
    {
        return *(Meta *) meta_;
    }
    ~List()
    {
        allocator_->free(meta_);
    }
    void *meta_addr() const
    {
        return meta_;
    }

private:
    GlobalAddress to_exposed_remote_mem(void *mem) const
    {
        return rdma_adpt_->to_exposed_gaddr(DCHECK_NOTNULL(mem));
    }
    void init_meta()
    {
        auto meta_size = Meta::size();
        void *meta_addr = DCHECK_NOTNULL(allocator_)->alloc(meta_size);
        meta_ = (Meta *) DCHECK_NOTNULL(meta_addr);
    }
    void setup_meta()
    {
        auto *meta_phead = (std::atomic<uint64_t> *) &meta_->phead;
        meta_phead->store(nullgaddr.val);
        auto *meta_ptail = (std::atomic<uint64_t> *) &meta_->ptail;
        meta_ptail->store(nullgaddr.val);
        auto *meta_pop_lock = (std::atomic<uint64_t> *) &meta_->pop_lock;
        meta_pop_lock->store(0);
        auto *meta_push_lock = (std::atomic<uint64_t> *) &meta_->push_lock;
        meta_push_lock->store(0);
    }

    size_t node_size() const
    {
        return sizeof(ListNode<T>);
    }
    IRdmaAdaptor::pointer rdma_adpt_;
    patronus::mem::IAllocator::pointer allocator_;
    ListConfig conf_;
    Meta *meta_;

    void *witness_buf_{nullptr};
    void *finished_buf_{nullptr};
    void *entries_buf_{nullptr};
};
}  // namespace patronus::list

#endif