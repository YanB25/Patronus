#include "Cache.h"

#include <inttypes.h>

Cache::Cache(const CacheConfig &cache_config)
{
    dwarn_if(cache_config.cacheSize % define::MB != 0,
             "cache size %" PRIu64 "is not aligned to MB.",
             cache_config.cacheSize);
    size = cache_config.cacheSize;
    data = (uint64_t) hugePageAlloc(size);
}