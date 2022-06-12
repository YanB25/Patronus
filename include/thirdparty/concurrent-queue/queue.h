#pragma once
#ifndef PATRONUS_CONCURRENT_QUEUE_H_
#define PATRONUS_CONCURRENT_QUEUE_H_
#include "./conf.h"
#include "./entry.h"
#include "GlobalAddress.h"
#include "thirdparty/linked-list/list.h"
#include "util/IRdmaAdaptor.h"

namespace patronus::cqueue
{
struct QueueConfig
{
    list::ListConfig list_config;
    size_t entry_per_block{};
};

inline std::ostream &operator<<(std::ostream &os, const QueueConfig &config)
{
    std::ignore = config;
    os << "{QueueConfig }";
    return os;
}

template <typename T, size_t kSize>
class Queue
{
public:
    using pointer = std::shared_ptr<Queue<T, kSize>>;
    using Entry = QueueEntry<T, kSize>;
    Queue(IRdmaAdaptor::pointer rdma_adpt,
          patronus::mem::IAllocator::pointer allocator,
          const QueueConfig &config)
        : rdma_adpt_(rdma_adpt), allocator_(allocator), conf_(config)
    {
        list_ = list::List<Entry>::new_instance(
            rdma_adpt_, allocator_, conf_.list_config);
    }
    static pointer new_instance(IRdmaAdaptor::pointer rdma_adpt,
                                patronus::mem::IAllocator::pointer allocator,
                                const QueueConfig &config)
    {
        return std::make_shared<Queue<T, kSize>>(rdma_adpt, allocator, config);
    }

    GlobalAddress meta_gaddr() const
    {
        return list_->meta_gaddr();
    }
    ~Queue()
    {
    }

private:
    typename list::List<Entry>::pointer list_;
    IRdmaAdaptor::pointer rdma_adpt_;
    patronus::mem::IAllocator::pointer allocator_;
    QueueConfig conf_;
};
}  // namespace patronus::cqueue

#endif