#pragma once
#ifndef PATRONUS_LEASE_CACHE_H
#define PATRONUS_LEASE_CACHE_H

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <list>
#include <vector>

namespace patronus
{
// [begin, end), data.size() == end - begin
struct LeaseCacheItem
{
    LeaseCacheItem(uint64_t b, uint64_t e, std::vector<char> &&d)
    {
        begin = b;
        end = e;
        data = std::move(d);
    }
    uint64_t begin;
    uint64_t end;
    std::vector<char> data;
};

// TODO(patronus): not handle range merging.
// Don't waste time. Impl only when needed.
template <size_t kLimitNr>
class LeaseCache
{
public:
    bool query(uint64_t addr, size_t len, char *i_buf)
    {
        auto it = internal_query(addr, len);
        if (it == cache_.end())
        {
            return false;
        }
        memcpy(i_buf, it->data.data() + (addr - it->begin), len);
        // lru
        cache_.splice(cache_.begin(), cache_, it);
        return true;
    }
    void insert(uint64_t addr, size_t len, const char *buf)
    {
        auto it = internal_query(addr, len);
        if (it == cache_.end())
        {
            std::vector<char> data;
            data.resize(len);
            memcpy(data.data(), buf, len);
            cache_.emplace_front(addr, addr + len, std::move(data));

            while (cache_.size() > kLimitNr)
            {
                // evition
                cache_.pop_back();
            }
        }
        else
        {
            // lru
            cache_.splice(cache_.begin(), cache_, it);
        }
    }

private:
    using CacheT = std::list<LeaseCacheItem>;

    CacheT::const_iterator internal_query(uint64_t addr, size_t len)
    {
        uint64_t request_begin = addr;
        uint64_t request_end = addr + len;
        for (auto it = cache_.begin(); it != cache_.end(); ++it)
        {
            if (it->begin <= request_begin && it->end >= request_end)
            {
                return it;
            }
        }
        return cache_.end();
    }

    std::list<LeaseCacheItem> cache_;
};

}  // namespace patronus

#endif