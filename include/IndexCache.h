#if !defined(_INDEX_CACHE_H_)
#define _INDEX_CACHE_H_

#include "CacheEntry.h"
#include "HugePageAlloc.h"
#include "Timer.h"
#include "inlineskiplist.h"

#include <atomic>

extern bool enter_debug;

using CacheSkipList = InlineSkipList<CacheEntryComparator>;

class IndexCache {

public:
  IndexCache(int cache_size);

  bool add_to_cache(InternalPage *page);
  const CacheEntry *search_from_cache(const Key &k, GlobalAddress *addr);

  bool add_entry(const Key &from, const Key &to, InternalPage *ptr);
  const CacheEntry *find_entry(const Key &k);
  const CacheEntry *find_entry(const Key &from, const Key &to);

  void invalidate(const CacheEntry *entry);

  void statistics();

  void bench();

private:
  uint64_t cache_size; // MB;
  std::atomic<int64_t> free_page_cnt;
  std::atomic<int64_t> skiplist_node_cnt;
  int64_t all_page_cnt;

  // SkipList
  CacheSkipList *skiplist;
  CacheEntryComparator cmp;
  Allocator alloc;

  void evict_one();
};

inline IndexCache::IndexCache(int cache_size) : cache_size(cache_size) {
  skiplist = new CacheSkipList(cmp, &alloc, 21);
  uint64_t memory_size = define::MB * cache_size;

  all_page_cnt = memory_size / sizeof(InternalPage);
  free_page_cnt.store(all_page_cnt);
  skiplist_node_cnt.store(0);
}

// [from, to）
inline bool IndexCache::add_entry(const Key &from, const Key &to,
                                  InternalPage *ptr) {

  // TODO memory leak
  auto buf = skiplist->AllocateKey(sizeof(CacheEntry));
  auto &e = *(CacheEntry *)buf;
  e.from = from;
  e.to = to - 1; // !IMPORTANT;
  e.ptr = ptr;

  return skiplist->InsertConcurrently(buf);
}

inline const CacheEntry *IndexCache::find_entry(const Key &from,
                                                const Key &to) {
  CacheSkipList::Iterator iter(skiplist);

  CacheEntry e;
  e.from = from;
  e.to = to - 1;
  iter.Seek((char *)&e);
  if (iter.Valid()) {
    auto val = (const CacheEntry *)iter.key();
    // while (val->ptr == nullptr) {
    //   iter.Next();
    //   if (!iter.Valid()) {
    //     return nullptr;
    //   }
    //   val = (const CacheEntry *)iter.key();
    // }
    return val;
  } else {
    return nullptr;
  }
}

inline const CacheEntry *IndexCache::find_entry(const Key &k) {
  return find_entry(k, k + 1);
}

inline bool IndexCache::add_to_cache(InternalPage *page) {
  auto new_page = (InternalPage *)malloc(kInternalPageSize);
  memcpy(new_page, page, kInternalPageSize);
  new_page->index_cache_freq = 0;

  if (this->add_entry(page->hdr.lowest, page->hdr.highest, new_page)) {
    skiplist_node_cnt.fetch_add(1);
    auto v = free_page_cnt.fetch_add(-1);
    if (v <= 0) {
      evict_one();
    }

    return true;
  } else { // conflicted
    auto e = this->find_entry(page->hdr.lowest, page->hdr.highest);
    if (e && e->from == page->hdr.lowest && e->to == page->hdr.highest - 1) {
      auto ptr = e->ptr;
      if (ptr == nullptr &&
          __sync_bool_compare_and_swap(&(e->ptr), 0ull, new_page)) {

        // if (enter_debug) {
        //   page->verbose_debug();
        // }
        auto v = free_page_cnt.fetch_add(-1);
        if (v <= 0) {
          evict_one();
        }
        return true;
      }
    }

    free(new_page);
    return false;
  }
}

inline const CacheEntry *IndexCache::search_from_cache(const Key &k,
                                                       GlobalAddress *addr) {
  auto entry = find_entry(k);

  InternalPage *page = entry ? entry->ptr : nullptr;

  if (page && entry->from <= k && entry->to >= k) {

    // if (enter_debug) {
    //   page->verbose_debug();
    // }

    page->index_cache_freq++;

    auto cnt = page->hdr.last_index + 1;
    if (k < page->records[0].key) {
      *addr = page->hdr.leftmost_ptr;
    } else {

      bool find = false;
      for (int i = 1; i < cnt; ++i) {
        if (k < page->records[i].key) {
          find = true;
          *addr = page->records[i - 1].ptr;
          break;
        }
      }
      if (!find) {
        *addr = page->records[cnt - 1].ptr;
      }
    }

    compiler_barrier();
    if (entry->ptr) { // check if it is freed.
      // printf("Cache HIt\n");
      return entry;
    }
  }

  return nullptr;
}

inline void IndexCache::invalidate(const CacheEntry *entry) {
  auto ptr = entry->ptr;

  if (__sync_bool_compare_and_swap(&(entry->ptr), ptr, 0)) {
    free(ptr);
    free_page_cnt.fetch_add(1);
  }
}

inline void IndexCache::evict_one() { assert(false); }

inline void IndexCache::statistics() {
  printf("[skiplist node: %ld]  [page cache: %ld]\n", skiplist_node_cnt.load(),
         all_page_cnt - free_page_cnt.load());
}

inline void IndexCache::bench() {

  Timer t;
  t.begin();
  const int loop = 100000;

  for (int i = 0; i < loop; ++i) {
    uint64_t r = rand() % (5 * define::MB);
    this->find_entry(r);
  }

  t.end_print(loop);
}

#endif // _INDEX_CACHE_H_
