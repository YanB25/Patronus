#include "Cache.h"

#include <glog/logging.h>
#include <inttypes.h>

#include "util/Pre.h"

Cache::Cache(const CacheConfig &cache_config)
{
    DLOG_IF(WARNING, cache_config.cacheSize % 1_MB != 0)
        << "cache size " << cache_config.cacheSize << "is not aligned to MB.";
    size = cache_config.cacheSize;
    data = (uint64_t) CHECK_NOTNULL(hugePageAlloc(size));
}