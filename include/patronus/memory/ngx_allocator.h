#pragma once
#ifndef MEMORY_NGX_ALLOCATOR_H_
#define MEMORY_NGX_ALLOCATOR_H_

#include "thirdparty/memory/nginx/ngx_palloc.h"

namespace patronus::mem
{
class NginxAllocator : public IAllocator
{
public:
    /**
     * @brief Construct a new Nginx Memory Pool object
     * The pool will only use the provided memory, i.e., @addr and @size
     */
    NginxAllocator(void *addr, size_t size)
    {
        pool_ = CHECK_NOTNULL(ngx_create_pool(addr, size, nullptr));
    }
    /**
     * @brief Construct a new Nginx Memory Pool object
     * The pool will rely on the @allocator for memory allocation/freed.
     * Typically at the granularity of @size.
     */
    NginxAllocator(size_t size,
                   std::shared_ptr<patronus::mem::IAllocator> allocator)
    {
        pool_ = CHECK_NOTNULL(
            ngx_create_pool(nullptr, size, CHECK_NOTNULL(allocator)));
    }
    /**
     * @brief Construct a new Nginx Memory Pool object
     * Make sure @addr is free-able by @allocator. That is
     * allocator->free(addr) should be well-defined.
     */
    NginxAllocator(void *addr,
                   size_t size,
                   std::shared_ptr<patronus::mem::IAllocator> allocator)
    {
        pool_ = CHECK_NOTNULL(ngx_create_pool(
            CHECK_NOTNULL(addr), size, CHECK_NOTNULL(allocator)));
    }
    void *alloc(size_t size) override
    {
        return ngx_palloc(pool_, size);
    }
    void free(void *addr) override
    {
        CHECK_EQ(ngx_pfree(pool_, addr), NGX_OK)
            << "[ngx][alloc] declined memory deallocation at addr " << addr
            << ", for pool: " << (void *) pool_;
    }

private:
    ngx_pool_t *pool_{nullptr};
};
}  // namespace patronus::mem

#endif