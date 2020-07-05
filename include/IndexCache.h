#if !defined(_INDEX_CACHE_H_)
#define _INDEX_CACHE_H_

#include "CacheEntry.h"
#include "HugePageAlloc.h"
#include "inlineskiplist.h"

#include <atomic>

using CacheSkipList = InlineSkipList<CacheEntryComparator>;

class IndexCache {

public:
  IndexCache(int cache_size);

  bool add_to_cache(const Key &from, const Key &to, InternalPage *page) {

  }
  
  GlobalAddress search_from_cache(const Key &from, const Key &to,);

  bool add_entry(const Key &from, const Key &to, InternalPage *ptr);
  const CacheEntry *find_entry(const Key &k);

private:
  uint64_t cache_size; // MB;
  std::atomic<int64_t> free_page_cnt;

  // SkipList
  CacheSkipList *skiplist;
  CacheEntryComparator cmp;
  Allocator alloc;
};

inline IndexCache::IndexCache(int cache_size) : cache_size(cache_size) {
  skiplist = new CacheSkipList(cmp, &alloc, 21);
  uint64_t memory_size = define::MB * cache_size;
  free_page_cnt.store(memory_size / sizeof(InternalPage));
}

// [from, to）
inline bool IndexCache::add_entry(const Key &from, const Key &to,
                                  InternalPage *ptr) {
  auto buf = skiplist->AllocateKey(sizeof(CacheEntry));
  auto &e = *(CacheEntry *)buf;
  e.from = from;
  e.to = to - 1; // !IMPORTANT;
  e.ptr = ptr;

  return skiplist->InsertConcurrently(buf);
}

inline const CacheEntry *IndexCache::find_entry(const Key &k) {
  CacheSkipList::Iterator iter(skiplist);

  CacheEntry e;
  e.from = e.to = k;
  iter.Seek((char *)&e);
  if (iter.Valid()) {
    auto val = (const CacheEntry *)iter.key();
    while (val->ptr == nullptr) {
      iter.Next();
      if (!iter.Valid()) {
        return nullptr;
      }
      val = (const CacheEntry *)iter.key();
    }
    return val;
  } else {
    return nullptr;
  }
}

#endif // _INDEX_CACHE_H_
