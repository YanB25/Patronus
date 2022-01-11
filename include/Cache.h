#pragma once
#if !defined(_CACHE_H_)
#define _CACHE_H_

#include "Config.h"
#include "HugePageAlloc.h"

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
};
inline std::ostream &operator<<(std::ostream &os, const Buffer &buf)
{
    os << "{Buffer base: " << (void *) buf.buffer << ", len: " << buf.size
       << "}";
    return os;
}

#endif  // _CACHE_H_
