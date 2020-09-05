#ifndef __HUGEPAGEALLOC_H__
#define __HUGEPAGEALLOC_H__

#include <memory.h>
#include <sys/mman.h>

#include <cstdint>

char *getIP();
inline void *hugePageAlloc(size_t size)
{
    void *res = mmap(NULL,
                     size,
                     PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB,
                     -1,
                     0);
    if (res == MAP_FAILED)
    {
        error("%s mmap failed!\n", getIP());
    }

    return res;
}

#endif /* __HUGEPAGEALLOC_H__ */
