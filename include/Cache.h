#pragma once
#if !defined(_CACHE_H_)
#define _CACHE_H_

#include "Config.h"
#include "HugePageAlloc.h"

class CacheConfig;
class Cache
{
public:
    Cache(const CacheConfig &cache_config);

    uint64_t data;
    uint64_t size;

private:
};

struct Buffer
{
    char *buffer;
    size_t size;
    Buffer() : buffer(nullptr), size(0)
    {
    }
    Buffer(char *buffer, size_t size) : buffer(buffer), size(size)
    {
    }
    Buffer(Buffer &&rhs)
    {
        (*this) = std::move(rhs);
    }
    Buffer(const Buffer &rhs) = delete;
    Buffer &operator=(const Buffer &) = delete;
    Buffer &operator=(Buffer &&rhs)
    {
        buffer = rhs.buffer;
        size = rhs.size;
        rhs.buffer = nullptr;
        rhs.size = 0;
        return *this;
    }

    /**
     * Buffer holds resources, so ideally it is only movable.
     * However, due to history reason, it is complex to remove all the copy of
     * buffer.
     * Therefore, allow .clone() call to do the copy. Use it with care!
     */
    Buffer clone() const
    {
        return Buffer(buffer, size);
    }
};
inline std::ostream &operator<<(std::ostream &os, const Buffer &buf)
{
    os << "{Buffer base: " << (void *) buf.buffer << ", len: " << buf.size
       << "}";
    return os;
}

inline void validate_buffer_not_overlapped(const Buffer &lhs, const Buffer &rhs)
{
    // https://stackoverflow.com/questions/325933/determine-whether-two-date-ranges-overlap
    auto start_1 = (uint64_t) lhs.buffer;
    auto end_1 = (uint64_t) start_1 + lhs.size;
    auto start_2 = (uint64_t) rhs.buffer;
    auto end_2 = (uint64_t) start_2 + rhs.size;
    // exclusive
    bool overlap = (start_1 < end_2) && (start_2 < end_1);
    CHECK(!overlap) << "Buffer_1 [" << (void *) start_1 << ", "
                    << (void *) end_1 << ") v.s. buffer_2 [" << (void *) start_2
                    << ", " << end_2 << "). Overlapped.";
}
inline void validate_buffer_not_overlapped(const std::vector<Buffer> &buffers)
{
    for (size_t i = 0; i < buffers.size(); ++i)
    {
        for (size_t j = i + 1; j < buffers.size(); ++j)
        {
            validate_buffer_not_overlapped(buffers[i], buffers[j]);
        }
    }
}

#endif  // _CACHE_H_
