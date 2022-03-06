#pragma once
#ifndef MEMORY_DIRECT_ALLOCATOR_H_
#define MEMORY_DIRECT_ALLOCATOR_H_

#include "allocator.h"

namespace patronus::mem
{
class DirectAllocator : public IAllocator
{
public:
    void *alloc(size_t size) override
    {
        void *ret = nullptr;
        if (size >= 2_MB)
        {
            ret = ::hugePageAlloc(size);
        }
        else
        {
            ret = ::malloc(size);
        }
        if (ret != nullptr)
        {
            addr_to_size_[ret] = size;
        }
        if constexpr (debug())
        {
            allocated_ += size;
        }
        DVLOG(10) << "[direct-alloc] allocating size " << size << ", ret "
                  << (void *) ret;
        return ret;
    }
    void free(void *addr) override
    {
        auto it = addr_to_size_.find(addr);
        if (it == addr_to_size_.end())
        {
            LOG(FATAL) << "[huge-alloc] failed to free " << addr
                       << ", not allocated by me.";
        }
        if (it->second >= 2_MB)
        {
            ::hugePageFree(addr, it->second);
        }
        else
        {
            ::free(addr);
        }
        addr_to_size_.erase(it);
        DVLOG(10) << "[direct-alloc] freeing " << (void *) addr;
    }
    void free(void *addr, size_t size) override
    {
        auto it = addr_to_size_.find(addr);
        if (it == addr_to_size_.end())
        {
            LOG(FATAL) << "[huge-alloc] failed to free " << addr
                       << ", not allocated by me.";
        }
        DCHECK_EQ(it->second, size);

        if (it->second >= 2_MB)
        {
            ::hugePageFree(addr, it->second);
        }
        else
        {
            ::free(addr);
        }
        addr_to_size_.erase(it);
        DVLOG(10) << "[direct-alloc] freeing " << (void *) addr << " with size "
                  << size;
    }

    size_t debug_allocated_bytes() const
    {
        return allocated_;
    }

private:
    std::unordered_map<void *, size_t> addr_to_size_;
    size_t allocated_{0};
};

/**
 * @brief The user should keep tracks the size of each allocation
 */
class RawAllocator : public IAllocator
{
public:
    void *alloc(size_t size) override
    {
        void *ret = nullptr;
        if (size >= 2_MB)
        {
            ret = ::hugePageAlloc(size);
        }
        else
        {
            ret = ::malloc(size);
        }
        return ret;
    }
    void free(void *addr) override
    {
        LOG(FATAL) << "[raw-alloc] raw allocator requires user to keep tracks "
                      "of the allocated memory size. addr: "
                   << addr;
    }
    void free(void *addr, size_t size) override
    {
        if (size >= 2_MB)
        {
            ::hugePageFree(addr, size);
        }
        else
        {
            ::free(addr);
        }
    }

private:
};

}  // namespace patronus::mem

#endif