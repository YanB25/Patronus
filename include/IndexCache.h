#if !defined(_INDEX_CACHE_H_)
#define _INDEX_CACHE_H_

#include "CacheEntry.h"
#include "inlineskiplist.h"

using CacheSkipList = InlineSkipList<CacheEntryComparator>;

class IndexCache {

public:
  IndexCache(int cache_size);

  

private:
  uint64_t cache_size; // MB;
  CacheSkipList skiplist;
};

#endif // _INDEX_CACHE_H_
