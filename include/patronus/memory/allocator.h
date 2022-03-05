#pragma once
#ifndef PATRONUS_MEMORY_ALLOCATOR_H_
#define PATRONUS_MEMORY_ALLOCATOR_H_

#include "Common.h"

namespace patronus::mem
{
using namespace define::literals;
inline std::pair<void *, size_t> align_address(void *addr,
                                               size_t size,
                                               size_t align)
{
    auto diff = ((uint64_t) addr) % align;
    if (diff == 0)
    {
        return {addr, size};
    }
    auto adjust = align - diff;
    auto aligned_addr = (void *) ((uint64_t) addr + adjust);
    if (size < adjust)
    {
        return {nullptr, 0};
    }
    auto aligned_size = size - adjust;
    return {aligned_addr, aligned_size};
}

class IAllocator
{
public:
    virtual ~IAllocator() = default;
    virtual void *alloc(size_t size) = 0;
    virtual void free(void *addr, size_t size) = 0;
    virtual void free(void *addr) = 0;
};
}  // namespace patronus::mem

#endif