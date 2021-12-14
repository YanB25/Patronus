#if !defined(_GLOBAL_ALLOCATOR_H_)
#define _GLOBAL_ALLOCATOR_H_

#include <cstring>

#include "Common.h"
#include "Debug.h"
#include "GlobalAddress.h"

// global allocator for coarse-grained (chunck level) alloc
// used by home agent
// bitmap based
class GlobalAllocator
{
public:
    GlobalAllocator(const GlobalAddress &start, size_t size)
        : start(start), size(size)
    {
        bitmap_len = size / define::kChunkSize;
        bitmap = new bool[bitmap_len];
        memset(bitmap, 0, bitmap_len);

        // null ptr
        bitmap[0] = true;
        bitmap_tail = 1;
    }
    static std::unique_ptr<GlobalAllocator> newInstance(
        const GlobalAddress &start, size_t size)
    {
        return future::make_unique<GlobalAllocator>(start, size);
    }

    ~GlobalAllocator()
    {
        delete[] bitmap;
    }

    GlobalAddress alloc_chunck()
    {
        GlobalAddress res = start;
        if (bitmap_tail >= bitmap_len)
        {
            assert(false);
            warn("shared memory space run out");
        }

        if (bitmap[bitmap_tail] == false)
        {
            bitmap[bitmap_tail] = true;
            res.offset += bitmap_tail * define::kChunkSize;

            bitmap_tail++;
        }
        else
        {
            assert(false);
            warn("TODO");
        }

        return res;
    }

    void free_chunk(const GlobalAddress &addr)
    {
        bitmap[(addr.offset - start.offset) / define::kChunkSize] = false;
    }

private:
    GlobalAddress start;
    [[maybe_unused]] size_t size;

    bool *bitmap;
    size_t bitmap_len;
    size_t bitmap_tail;
};

#endif
