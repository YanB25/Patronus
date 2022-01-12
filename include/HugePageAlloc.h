#pragma once
#ifndef __HUGEPAGEALLOC_H__
#define __HUGEPAGEALLOC_H__

#include <memory.h>
#include <sys/mman.h>

#include <cstdint>

char *getIP();
void *hugePageAlloc(size_t size);
bool hugePageFree(void *ptr, size_t size);

#endif /* __HUGEPAGEALLOC_H__ */
