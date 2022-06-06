#pragma once
#ifndef PATRONUS_CONCURRENT_QUEUE_H_
#define PATRONUS_CONCURRENT_QUEUE_H_
#include "./conf.h"
#include "./meta.h"
#include "GlobalAddress.h"
#include "util/IRdmaAdaptor.h"

namespace patronus::cqueue
{
struct QueueConfig
{
    size_t client_nr;
    size_t max_entry_nr;
};

inline std::ostream &operator<<(std::ostream &os, const QueueConfig &config)
{
    os << "{QueueConfig client_nr: " << config.client_nr
       << ", max_entry_nr: " << config.max_entry_nr << "}";
    return os;
}

template <typename T>
class Queue
{
public:
    using pointer = std::shared_ptr<Queue<T>>;
    Queue(IRdmaAdaptor::pointer rdma_adpt,
          patronus::mem::IAllocator::pointer allocator,
          const QueueConfig &config)
        : rdma_adpt_(rdma_adpt), allocator_(allocator), conf_(config)
    {
        init_meta();
        setup_meta();
    }
    static pointer new_instance(IRdmaAdaptor::pointer rdma_adpt,
                                patronus::mem::IAllocator::pointer allocator,
                                const QueueConfig &config)
    {
        return std::make_shared<Queue<T>>(rdma_adpt, allocator, config);
    }

    GlobalAddress meta_gaddr() const
    {
        return to_exposed_remote_mem(meta_addr());
    }
    Meta meta() const
    {
        return *(Meta *) meta_;
    }
    ~Queue()
    {
        allocator_->free(witness_buf_);
        allocator_->free(finished_buf_);
        allocator_->free(entries_buf_);
        allocator_->free(meta_);
    }

private:
    GlobalAddress to_exposed_remote_mem(void *mem) const
    {
        return rdma_adpt_->to_exposed_gaddr(DCHECK_NOTNULL(mem));
    }
    void *meta_addr() const
    {
        return meta_;
    }
    void init_meta()
    {
        auto meta_size = Meta::size();
        void *meta_addr = DCHECK_NOTNULL(allocator_)->alloc(meta_size);
        meta_ = (Meta *) DCHECK_NOTNULL(meta_addr);
    }
    void setup_meta()
    {
        auto meta_size = Meta::size();
        memset(meta_, 0, meta_size);
        auto client_nr = conf_.client_nr;
        meta_->client_nr = client_nr;

        auto client_witness_size = sizeof(uint64_t) * client_nr;
        witness_buf_ = DCHECK_NOTNULL(allocator_->alloc(client_witness_size));
        meta_->client_witness = to_exposed_remote_mem(witness_buf_);

        auto client_finished_size = sizeof(uint64_t) * client_nr;
        finished_buf_ = DCHECK_NOTNULL(allocator_->alloc(client_finished_size));
        meta_->client_finished = to_exposed_remote_mem(finished_buf_);

        auto max_entry_nr = conf_.max_entry_nr;
        auto entry_buf_size = max_entry_nr * sizeof(T);
        entries_buf_ = DCHECK_NOTNULL(allocator_->alloc(entry_buf_size));
        meta_->entries_gaddr = to_exposed_remote_mem(entries_buf_);
    }
    IRdmaAdaptor::pointer rdma_adpt_;
    patronus::mem::IAllocator::pointer allocator_;
    QueueConfig conf_;
    Meta *meta_;

    void *witness_buf_{nullptr};
    void *finished_buf_{nullptr};
    void *entries_buf_{nullptr};
};
}  // namespace patronus::cqueue

#endif