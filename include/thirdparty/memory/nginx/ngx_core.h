/*
 * Copyright (C) Igor Sysoev
 * Copyright (C) Nginx, Inc.
 */

#ifndef _NGX_CORE_H_INCLUDED_
#define _NGX_CORE_H_INCLUDED_

#include "ngx_config.h"

typedef struct ngx_pool_s ngx_pool_t;
typedef int ngx_fd_t;
typedef struct ngx_chain_s ngx_chain_t;

#define NGX_OK 0
#define NGX_ERROR -1
#define NGX_AGAIN -2
#define NGX_BUSY -3
#define NGX_DONE -4
#define NGX_DECLINED -5
#define NGX_ABORT -6

#define LF (u_char) '\n'
#define CR (u_char) '\r'
#define CRLF "\r\n"

#define ngx_abs(value) (((value) >= 0) ? (value) : -(value))
#define ngx_max(val1, val2) ((val1 < val2) ? (val2) : (val1))
#define ngx_min(val1, val2) ((val1 > val2) ? (val2) : (val1))

void ngx_cpuinfo(void);

#if (NGX_HAVE_OPENAT)
#define NGX_DISABLE_SYMLINKS_OFF 0
#define NGX_DISABLE_SYMLINKS_ON 1
#define NGX_DISABLE_SYMLINKS_NOTOWNER 2
#endif

#endif /* _NGX_CORE_H_INCLUDED_ */
