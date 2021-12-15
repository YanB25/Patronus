#include "Cache.h"

#include <inttypes.h>
#include <glog/logging.h>

Cache::Cache(const CacheConfig &cache_config)
{
    DLOG_IF(WARNING, cache_config.cacheSize % define::MB != 0) << 
             "cache size " << cache_config.cacheSize <<  "is not aligned to MB.";
    size = cache_config.cacheSize;
    data = (uint64_t) hugePageAlloc(size);
}