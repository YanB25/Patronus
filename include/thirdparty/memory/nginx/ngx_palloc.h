
/*
 * Copyright (C) Igor Sysoev
 * Copyright (C) Nginx, Inc.
 */

#ifndef _NGX_PALLOC_H_INCLUDED_
#define _NGX_PALLOC_H_INCLUDED_

#include <memory>

#include "ngx_config.h"
#include "ngx_core.h"
#include "patronus/memory/block_allocator.h"

/*
 * NGX_MAX_ALLOC_FROM_POOL should be (ngx_pagesize - 1), i.e. 4095 on x86.
 * On Windows NT it decreases a number of locked pages in a kernel.
 */
// #define NGX_MAX_ALLOC_FROM_POOL (4096 - 1)
// Note: we allow allocating a large piece from the pool
// 128 GB
#define NGX_MAX_ALLOC_FROM_POOL (128ull * 1024 * 1024 * 1024 - 1)

#define NGX_DEFAULT_POOL_SIZE (16 * 1024)

#define NGX_POOL_ALIGNMENT 16
#define NGX_MIN_POOL_SIZE                                          \
    ngx_align((sizeof(ngx_pool_t) + 2 * sizeof(ngx_pool_large_t)), \
              NGX_POOL_ALIGNMENT)

typedef void (*ngx_pool_cleanup_pt)(void *data);

typedef struct ngx_pool_cleanup_s ngx_pool_cleanup_t;

struct ngx_pool_cleanup_s
{
    ngx_pool_cleanup_pt handler;
    void *data;
    ngx_pool_cleanup_t *next;
};

typedef struct ngx_pool_large_s ngx_pool_large_t;

struct ngx_pool_large_s
{
    ngx_pool_large_t *next;
    void *alloc;
};

typedef struct
{
    u_char *last;
    u_char *end;
    ngx_pool_t *next;
    ngx_uint_t failed;
} ngx_pool_data_t;

struct ngx_pool_s
{
    ngx_pool_data_t d;
    size_t max;
    ngx_pool_t *current;
    ngx_chain_t *chain;
    ngx_pool_large_t *large;
    ngx_pool_cleanup_t *cleanup;

    std::shared_ptr<patronus::mem::BlockAllocator> internal_allocator;
};

typedef struct
{
    ngx_fd_t fd;
    u_char *name;
} ngx_pool_cleanup_file_t;

ngx_pool_t *ngx_create_pool(void *addr,
                            size_t size,
                            std::shared_ptr<patronus::mem::BlockAllocator>);
void ngx_destroy_pool(ngx_pool_t *pool);
void ngx_reset_pool(ngx_pool_t *pool);

void *ngx_palloc(ngx_pool_t *pool, size_t size);
void *ngx_pnalloc(ngx_pool_t *pool, size_t size);
void *ngx_pcalloc(ngx_pool_t *pool, size_t size);
void *ngx_pmemalign(ngx_pool_t *pool, size_t size, size_t alignment);
ngx_int_t ngx_pfree(ngx_pool_t *pool, void *p);

ngx_pool_cleanup_t *ngx_pool_cleanup_add(ngx_pool_t *p, size_t size);

#endif /* _NGX_PALLOC_H_INCLUDED_ */
