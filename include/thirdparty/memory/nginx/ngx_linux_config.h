/*
 * Copyright (C) Igor Sysoev
 * Copyright (C) Nginx, Inc.
 */

#ifndef _NGX_LINUX_CONFIG_H_INCLUDED_
#define _NGX_LINUX_CONFIG_H_INCLUDED_

#ifndef _GNU_SOURCE
#define _GNU_SOURCE /* pread(), pwrite(), gethostname() */
#endif

#define _FILE_OFFSET_BITS 64

#include <arpa/inet.h>
#include <crypt.h>
#include <ctype.h>
#include <dirent.h>
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <glob.h>
#include <grp.h>
#include <limits.h> /* IOV_MAX */
#include <malloc.h> /* memalign() */
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h> /* TCP_NODELAY, TCP_CORK */
#include <pwd.h>
#include <sched.h>
#include <signal.h>
#include <stdarg.h>
#include <stddef.h> /* offsetof() */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <sys/utsname.h> /* uname() */
#include <sys/vfs.h>     /* statfs() */
#include <sys/wait.h>
#include <time.h> /* tzset() */
#include <unistd.h>

#include "ngx_auto_config.h"

#if (NGX_HAVE_POSIX_SEM)
#include <semaphore.h>
#endif

#if (NGX_HAVE_SYS_PRCTL_H)
#include <sys/prctl.h>
#endif

#if (NGX_HAVE_SENDFILE64)
#include <sys/sendfile.h>
#else
extern ssize_t sendfile(int s, int fd, int32_t *offset, size_t size);
#define NGX_SENDFILE_LIMIT 0x80000000
#endif

#if (NGX_HAVE_POLL)
#include <poll.h>
#endif

#if (NGX_HAVE_EPOLL)
#include <sys/epoll.h>
#endif

#if (NGX_HAVE_SYS_EVENTFD_H)
#include <sys/eventfd.h>
#endif
#include <sys/syscall.h>
#if (NGX_HAVE_FILE_AIO)
#include <linux/aio_abi.h>
typedef struct iocb ngx_aiocb_t;
#endif

#if (NGX_HAVE_CAPABILITIES)
#include <linux/capability.h>
#endif

#define NGX_LISTEN_BACKLOG 511

#ifndef NGX_HAVE_SO_SNDLOWAT
/* setsockopt(SO_SNDLOWAT) returns ENOPROTOOPT */
#define NGX_HAVE_SO_SNDLOWAT 0
#endif

#ifndef NGX_HAVE_INHERITED_NONBLOCK
#define NGX_HAVE_INHERITED_NONBLOCK 0
#endif

#define NGX_HAVE_OS_SPECIFIC_INIT 1
#define ngx_debug_init()

extern char **environ;

#endif /* _NGX_LINUX_CONFIG_H_INCLUDED_ */
