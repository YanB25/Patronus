#include "HugePageAlloc.h"
#include <stdexcept>
#include <memory.h>
#include <sys/mman.h>

#include <cstdint>
#include "Debug.h"
void *hugePageAlloc(size_t size)
{
    void *res = mmap(NULL,
                     size,
                     PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB,
                     -1,
                     0);
    if (res == MAP_FAILED)
    {
        error("%s mmap failed for size %lu\n", getIP(), size);
        throw std::runtime_error("mmap failed to alloc");
        exit(-1);
    }

    return res;
}