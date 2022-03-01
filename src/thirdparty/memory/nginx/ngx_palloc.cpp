/*
 * Copyright (C) Igor Sysoev
 * Copyright (C) Nginx, Inc.
 */

#include "thirdparty/memory/nginx/ngx_palloc.h"

#include "glog/logging.h"
#include "thirdparty/memory/nginx/ngx_config.h"
#include "thirdparty/memory/nginx/ngx_core.h"

static ngx_inline void *ngx_palloc_small(ngx_pool_t *pool,
                                         size_t size,
                                         ngx_uint_t align);
static void *ngx_palloc_block(ngx_pool_t *pool, size_t size);
static void *ngx_palloc_large(ngx_pool_t *pool, size_t size);

/**
 * @brief create a memory pool for faster memory allocation
 * (@addr, @size) and @allocator, only require to provide one of them
 * If (@addr, @size) is provided, the pool will be built on the provided address
 * Otherwise, the pool will make use of the allocator.
 *
 * @param addr
 * @param size
 * @param allocator
 * @return ngx_pool_t*
 */
ngx_pool_t *ngx_create_pool(
    void *addr,
    size_t size,
    std::shared_ptr<patronus::mem::IAllocator> allocator)
{
    ngx_pool_t *p;

    if (addr != nullptr)
    {
        auto [aligned_addr, aligned_size] =
            patronus::mem::align_address(addr, size, NGX_POOL_ALIGNMENT);
        addr = aligned_addr;
        size = aligned_size;
        p = (ngx_pool_t *) addr;
    }
    else
    {
        p = (ngx_pool_t *) CHECK_NOTNULL(allocator)->alloc(size);
    }

    if (p == NULL)
    {
        return NULL;
    }

    p->d.last = (u_char *) p + sizeof(ngx_pool_t);
    p->d.end = (u_char *) p + size;
    p->d.next = NULL;
    p->d.failed = 0;

    size = size - sizeof(ngx_pool_t);
    p->max = (size < NGX_MAX_ALLOC_FROM_POOL) ? size : NGX_MAX_ALLOC_FROM_POOL;

    p->current = p;
    p->chain = NULL;
    p->large = NULL;
    p->cleanup = NULL;

    p->internal_allocator = allocator;

    return p;
}

void ngx_destroy_pool(ngx_pool_t *pool)
{
    ngx_pool_t *p, *n;
    ngx_pool_large_t *l;
    ngx_pool_cleanup_t *c;

    for (c = pool->cleanup; c; c = c->next)
    {
        if (c->handler)
        {
            DVLOG(6) << "[ngx][alloc] run cleanup: " << (void *) c;
            c->handler(c->data);
        }
    }

#if (NGX_DEBUG)

    /*
     * we could allocate the pool->log from this pool
     * so we cannot use this log while free()ing the pool
     */

    for (l = pool->large; l; l = l->next)
    {
        ngx_log_debug1(NGX_LOG_DEBUG_ALLOC, pool->log, 0, "free: %p", l->alloc);
    }

    for (p = pool, n = pool->d.next; /* void */; p = n, n = n->d.next)
    {
        ngx_log_debug2(NGX_LOG_DEBUG_ALLOC,
                       pool->log,
                       0,
                       "free: %p, unused: %uz",
                       p,
                       p->d.end - p->d.last);

        if (n == NULL)
        {
            break;
        }
    }

#endif

    for (l = pool->large; l; l = l->next)
    {
        if (l->alloc)
        {
            if (pool->internal_allocator)
            {
                pool->internal_allocator->free(l->alloc);
            }
        }
    }

    for (p = pool, n = pool->d.next; /* void */; p = n, n = n->d.next)
    {
        if (pool->internal_allocator)
        {
            pool->internal_allocator->free(p);
        }

        if (n == NULL)
        {
            break;
        }
    }
}

void ngx_reset_pool(ngx_pool_t *pool)
{
    ngx_pool_t *p;
    ngx_pool_large_t *l;

    for (l = pool->large; l; l = l->next)
    {
        if (l->alloc)
        {
            if (pool->internal_allocator)
            {
                pool->internal_allocator->free(l->alloc);
            }
        }
    }

    for (p = pool; p; p = p->d.next)
    {
        p->d.last = (u_char *) p + sizeof(ngx_pool_t);
        p->d.failed = 0;
    }

    pool->current = pool;
    pool->chain = NULL;
    pool->large = NULL;
}

void *ngx_palloc(ngx_pool_t *pool, size_t size)
{
#if !(NGX_DEBUG_PALLOC)
    if (size <= pool->max)
    {
        return ngx_palloc_small(pool, size, 1);
    }
#endif

    return ngx_palloc_large(pool, size);
}

void *ngx_pnalloc(ngx_pool_t *pool, size_t size)
{
#if !(NGX_DEBUG_PALLOC)
    if (size <= pool->max)
    {
        return ngx_palloc_small(pool, size, 0);
    }
#endif

    return ngx_palloc_large(pool, size);
}

static ngx_inline void *ngx_palloc_small(ngx_pool_t *pool,
                                         size_t size,
                                         ngx_uint_t align)
{
    u_char *m;
    ngx_pool_t *p;

    p = pool->current;

    do
    {
        m = p->d.last;

        if (align)
        {
            m = ngx_align_ptr(m, NGX_ALIGNMENT);
        }

        if ((size_t)(p->d.end - m) >= size)
        {
            p->d.last = m + size;

            return m;
        }

        p = p->d.next;

    } while (p);

    return ngx_palloc_block(pool, size);
}

static void *ngx_palloc_block(ngx_pool_t *pool, size_t size)
{
    u_char *m;
    size_t psize;
    ngx_pool_t *p, *new_var;

    psize = (size_t)(pool->d.end - (u_char *) pool);

    if (pool->internal_allocator)
    {
        DVLOG(6) << "[ngx][alloc] allocating block of " << psize;
        m = (u_char *) pool->internal_allocator->alloc(psize);
    }
    else
    {
        m = NULL;
    }

    if (m == NULL)
    {
        return NULL;
    }

    new_var = (ngx_pool_t *) m;

    new_var->d.end = m + psize;
    new_var->d.next = NULL;
    new_var->d.failed = 0;

    m += sizeof(ngx_pool_data_t);
    m = ngx_align_ptr(m, NGX_ALIGNMENT);
    new_var->d.last = m + size;

    for (p = pool->current; p->d.next; p = p->d.next)
    {
        if (p->d.failed++ > 4)
        {
            pool->current = p->d.next;
        }
    }

    p->d.next = new_var;

    return m;
}

static void *ngx_palloc_large(ngx_pool_t *pool, size_t size)
{
    void *p;
    ngx_uint_t n;
    ngx_pool_large_t *large;

    if (pool->internal_allocator)
    {
        DVLOG(6) << "[ngx][alloc] allocating block of " << size;
        p = pool->internal_allocator->alloc(size);
    }
    else
    {
        p = NULL;
    }

    if (p == NULL)
    {
        return NULL;
    }

    n = 0;

    for (large = pool->large; large; large = large->next)
    {
        if (large->alloc == NULL)
        {
            large->alloc = p;
            return p;
        }

        if (n++ > 3)
        {
            break;
        }
    }

    large = (ngx_pool_large_t *) ngx_palloc_small(
        pool, sizeof(ngx_pool_large_t), 1);
    if (large == NULL)
    {
        if (pool->internal_allocator)
        {
            pool->internal_allocator->free(p);
        }
        return NULL;
    }

    large->alloc = p;
    large->next = pool->large;
    pool->large = large;

    return p;
}

void *ngx_pmemalign(ngx_pool_t *pool, size_t size, size_t alignment)
{
    void *p;
    ngx_pool_large_t *large;

    if (pool->internal_allocator)
    {
        DVLOG(6) << "[ngx][alloc] allocate block of " << size;
        p = pool->internal_allocator->alloc(size);
        CHECK_EQ((uint64_t) p % alignment, 0);
    }
    else
    {
        p = NULL;
    }

    if (p == NULL)
    {
        return NULL;
    }

    large = (ngx_pool_large_t *) ngx_palloc_small(
        pool, sizeof(ngx_pool_large_t), 1);
    if (large == NULL)
    {
        if (pool->internal_allocator)
        {
            pool->internal_allocator->free(p);
        }
        return NULL;
    }

    large->alloc = p;
    large->next = pool->large;
    pool->large = large;

    return p;
}

ngx_int_t ngx_pfree(ngx_pool_t *pool, void *p)
{
    ngx_pool_large_t *l;

    for (l = pool->large; l; l = l->next)
    {
        if (p == l->alloc)
        {
            if (pool->internal_allocator)
            {
                pool->internal_allocator->free(l->alloc);
            }
            l->alloc = NULL;

            return NGX_OK;
        }
    }

    return NGX_DECLINED;
}

void *ngx_pcalloc(ngx_pool_t *pool, size_t size)
{
    void *p;
    p = ngx_palloc(pool, size);
    if (p)
    {
        memset(p, 0, size);
    }

    return p;
}

ngx_pool_cleanup_t *ngx_pool_cleanup_add(ngx_pool_t *p, size_t size)
{
    ngx_pool_cleanup_t *c;

    c = (ngx_pool_cleanup_t *) ngx_palloc(p, sizeof(ngx_pool_cleanup_t));
    if (c == NULL)
    {
        return NULL;
    }

    if (size)
    {
        c->data = ngx_palloc(p, size);
        if (c->data == NULL)
        {
            return NULL;
        }
    }
    else
    {
        c->data = NULL;
    }

    c->handler = NULL;
    c->next = p->cleanup;

    p->cleanup = c;

    DVLOG(6) << "[ngx][alloc] add cleanup: " << (void *) c;

    return c;
}

#if 0

static void *
ngx_get_cached_block(size_t size)
{
    void                     *p;
    ngx_cached_block_slot_t  *slot;

    if (ngx_cycle->cache == NULL) {
        return NULL;
    }

    slot = &ngx_cycle->cache[(size + ngx_pagesize - 1) / ngx_pagesize];

    slot->tries++;

    if (slot->number) {
        p = slot->block;
        slot->block = slot->block->next;
        slot->number--;
        return p;
    }

    return NULL;
}

#endif