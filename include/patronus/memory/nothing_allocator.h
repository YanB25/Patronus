#pragma once
#ifndef MEMORY_NOTHING_ALLOCATOR_H_
#define MEMORY_NOTHING_ALLOCATOR_H_

#include "allocator.h"

namespace patronus::mem
{
class NothingAllocator : public IAllocator
{
public:
    void *alloc([[maybe_unused]] size_t size,
                [[maybe_unused]] CoroContext *ctx = nullptr) override
    {
        return &addr_;
    }
    void free(void *addr, [[maybe_unused]] CoroContext *ctx = nullptr) override
    {
        DCHECK_EQ(addr, &addr_);
    }
    void free(void *addr,
              [[maybe_unused]] size_t size,
              [[maybe_unused]] CoroContext *ctx = nullptr) override
    {
        DCHECK_EQ(addr, &addr_);
    }

private:
    uint64_t addr_;
};

}  // namespace patronus::mem

#endif