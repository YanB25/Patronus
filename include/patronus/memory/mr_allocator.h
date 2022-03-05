#pragma once
#ifndef MEMORY_MR_ALLOCATOR_H_
#define MEMORY_MR_ALLOCATOR_H_

#include "Rdma.h"
#include "allocator.h"
#include "slab_allocator.h"

namespace patronus::mem
{
struct MRAllocatorConfig
{
    std::shared_ptr<IAllocator> allocator;
    RdmaContext *rdma_context;
};

class MRAllocator : public IAllocator
{
public:
    MRAllocator(MRAllocatorConfig config) : conf_(config)
    {
    }

    void *alloc(size_t size) override
    {
        auto *ret = conf_.allocator->alloc(size);
        if (ret != nullptr)
        {
            bind_mr(ret, size);
        }
        return ret;
    }
    void free(void *addr) override
    {
        if (addr != nullptr)
        {
            unbind_mr(addr);
        }
        conf_.allocator->free(addr);
    }
    void free(void *addr, size_t size) override
    {
        if (addr != nullptr)
        {
            unbind_mr(addr);
        }
        conf_.allocator->free(addr, size);
    }
    std::shared_ptr<IAllocator> get_internal_allocator()
    {
        return conf_.allocator;
    }

private:
    void bind_mr(void *addr, size_t size)
    {
        auto *mr = CHECK_NOTNULL(
            createMemoryRegion((uint64_t) addr, size, conf_.rdma_context));
        addr_to_mr_[addr] = mr;
    }
    void unbind_mr(void *addr)
    {
        auto it = addr_to_mr_.find(addr);
        if (it == addr_to_mr_.end())
        {
            LOG(FATAL)
                << "[mr-alloc] failed to find addr: not allocated by me.";
        }
        ibv_mr *free_mr = it->second;
        destroyMemoryRegion(free_mr);

        addr_to_mr_.erase(it);
    }

    MRAllocatorConfig conf_;

    std::unordered_map<void *, ibv_mr *> addr_to_mr_;
};
}  // namespace patronus::mem

#endif