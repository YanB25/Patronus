#include "HugePageAlloc.h"
#include <stdexcept>
#include <memory.h>
#include <sys/mman.h>

#include <cstdint>

#include "Common.h"
#include <glog/logging.h>
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
        LOG(FATAL) << getIP() << " mmap failed for size " << size;
        exit(-1);
    }

    return res;
}

bool hugePageFree(void* ptr, size_t size)
{
    size_t align = 2 * 1024 * 1024;
    CHECK((uint64_t) ptr % align == 0);
    size = ROUND_UP(size, align);
    CHECK(size % align == 0);
    if (munmap(ptr, size))
    {
        PLOG(ERROR) << "failed to free huge page";
        return false;
    }
    return true;
}