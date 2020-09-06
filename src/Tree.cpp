#include "Tree.h"

#include <city.h>

#include <algorithm>
#include <iostream>
#include <queue>
#include <utility>

#include "HotBuffer.h"
#include "IndexCache.h"
#include "RdmaBuffer.h"
#include "SimpleHT.h"
#include "Timer.h"

bool enter_debug = false;

#ifdef TEST_SINGLE_THREAD
SimpleHT mapping;
#endif

HotBuffer hot_buf;
uint64_t cache_miss[MAX_APP_THREAD][8];
uint64_t cache_hit[MAX_APP_THREAD][8];
uint64_t lock_fail[MAX_APP_THREAD][8];
uint64_t pattern[MAX_APP_THREAD][8];
uint64_t hierarchy_lock[MAX_APP_THREAD][8];
uint64_t handover_count[MAX_APP_THREAD][8];
uint64_t hot_filter_count[MAX_APP_THREAD][8];
uint64_t latency[MAX_APP_THREAD][LATENCY_WINDOWS];
volatile bool need_stop = false;

thread_local CoroCall Tree::worker[define::kMaxCoro];
thread_local CoroCall Tree::master;
thread_local GlobalAddress path_stack[define::kMaxCoro]
                                     [define::kMaxLevelOfTree];

// for coroutine schedule
struct CoroDeadline
{
    uint64_t deadline;
    uint16_t coro_id;

    bool operator<(const CoroDeadline &o) const
    {
        return this->deadline < o.deadline;
    }
};

thread_local Timer timer;
thread_local std::queue<uint16_t> hot_wait_queue;
thread_local std::priority_queue<CoroDeadline> deadline_queue;

Tree::Tree(std::shared_ptr<DSM> dsm, uint16_t tree_id)
    : dsm(dsm), tree_id(tree_id)
{
#ifdef CONFIG_ENABLE_HIERARCHIAL_LOCK
    for (int i = 0; i < dsm->getClusterSize(); ++i)
    {
        local_locks[i] = new LocalLockNode[define::kNumOfLock];
        for (size_t k = 0; k < define::kNumOfLock; ++k)
        {
            auto &n = local_locks[i][k];
            n.ticket_lock.store(0);
            n.hand_over = false;
            n.hand_time = 0;
        }
    }
#endif

    assert(dsm->is_register());
    print_verbose();

    index_cache = new IndexCache(define::kIndexCacheSize);

    root_ptr_ptr = get_root_ptr_ptr();

    // try to init tree and install root pointer
    auto page_buffer = (dsm->get_rbuf(0)).get_page_buffer();
    auto root_addr = dsm->alloc(kLeafPageSize);
    auto root_page = new (page_buffer) LeafPage;

    root_page->set_consistent();
    dsm->write_sync(page_buffer, root_addr, kLeafPageSize);

    auto cas_buffer = (dsm->get_rbuf(0)).get_cas_buffer();
    bool res = dsm->cas_sync(root_ptr_ptr, 0, root_addr.val, cas_buffer);
    if (res)
    {
        std::cout << "Tree root pointer value " << root_addr << std::endl;
    }
    else
    {
        // std::cout << "fail\n";
    }
}

void Tree::print_verbose()
{
    constexpr int kLeafHdrOffset = STRUCT_OFFSET(LeafPage, hdr);
    constexpr int kInternalHdrOffset = STRUCT_OFFSET(InternalPage, hdr);
    static_assert(kLeafHdrOffset == kInternalHdrOffset, "XXX");

    if (dsm->getMyNodeID() == 0)
    {
        std::cout << "Header size: " << sizeof(Header) << std::endl;
        std::cout << "Internal Page size: " << sizeof(InternalPage) << " ["
                  << kInternalPageSize << "]" << std::endl;
        std::cout << "Internal per Page: " << kInternalCardinality << std::endl;
        std::cout << "Leaf Page size: " << sizeof(LeafPage) << " ["
                  << kLeafPageSize << "]" << std::endl;
        std::cout << "Leaf per Page: " << kLeafCardinality << std::endl;
        std::cout << "LeafEntry size: " << sizeof(LeafEntry) << std::endl;
        std::cout << "InternalEntry size: " << sizeof(InternalEntry)
                  << std::endl;

#ifdef CONFIG_ENABLE_OP_COUPLE
        std::cout << "Opearation Couple: On" << std::endl;
#endif

#ifdef CONFIG_ENABLE_ON_CHIP_LOCK
        std::cout << "On-chip Lock: On" << std::endl;
#endif

#ifdef CONFIG_ENABLE_FINER_VERSION
        std::cout << "Finer Verision: On" << std::endl;
#endif

#ifdef CONFIG_ENABLE_HOT_FILTER

        std::cout << "Hot Filter: On" << std::endl;

#endif

#ifdef CONFIG_ENABLE_HIERARCHIAL_LOCK

        std::cout << "Hierarchial Lock: On" << std::endl;

#endif

#ifdef CONFIG_EABLE_BAKERY_LOCK

        std::cout << "Bakery Lock: On" << std::endl;

#endif
    }
}

inline void Tree::before_operation(CoroContext *cxt, int coro_id)
{
    for (size_t i = 0; i < define::kMaxLevelOfTree; ++i)
    {
        path_stack[coro_id][i] = GlobalAddress::Null();
    }
}

GlobalAddress Tree::get_root_ptr_ptr()
{
    GlobalAddress addr;
    addr.nodeID = 0;
    addr.offset =
        define::kRootPointerStoreOffest + sizeof(GlobalAddress) * tree_id;

    return addr;
}

extern GlobalAddress g_root_ptr;
extern int g_root_level;
extern bool enable_cache;
GlobalAddress Tree::get_root_ptr(CoroContext *cxt, int coro_id)
{
    if (g_root_ptr == GlobalAddress::Null())
    {
        auto page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
        dsm->read_sync(page_buffer, root_ptr_ptr, sizeof(GlobalAddress), cxt);
        GlobalAddress root_ptr = *(GlobalAddress *) page_buffer;
        return root_ptr;
    }
    else
    {
        return g_root_ptr;
    }

    // std::cout << "root ptr " << root_ptr << std::endl;
}

void Tree::broadcast_new_root(GlobalAddress new_root_addr, int root_level)
{
    RawMessage m;
    m.type = RpcType::NEW_ROOT;
    m.addr = new_root_addr;
    m.level = root_level;
    for (int i = 0; i < dsm->getClusterSize(); ++i)
    {
        dsm->rpc_call_dir(m, i);
    }
}

bool Tree::update_new_root(GlobalAddress left,
                           const Key &k,
                           GlobalAddress right,
                           int level,
                           GlobalAddress old_root,
                           CoroContext *cxt,
                           int coro_id)
{
    auto page_buffer = dsm->get_rbuf(coro_id).get_page_buffer();
    auto cas_buffer = dsm->get_rbuf(coro_id).get_cas_buffer();
    auto new_root = new (page_buffer) InternalPage(left, k, right, level);

    auto new_root_addr = dsm->alloc(kInternalPageSize);

    new_root->set_consistent();
    dsm->write_sync(page_buffer, new_root_addr, kInternalPageSize, cxt);
    if (dsm->cas_sync(root_ptr_ptr, old_root, new_root_addr, cas_buffer, cxt))
    {
        broadcast_new_root(new_root_addr, level);
        std::cout << "new root level " << level << " " << new_root_addr
                  << std::endl;
        // << " split key =" << k << " buffer " << (uint64_t)
        // page_buffer << std::endl;
        return true;
    }
    else
    {
        std::cout << "cas root fail " << std::endl;
    }

    return false;
}

void Tree::print_and_check_tree(CoroContext *cxt, int coro_id)
{
    assert(dsm->is_register());

    auto root = get_root_ptr(cxt, coro_id);
    // SearchResult result;

    GlobalAddress p = root;
    GlobalAddress levels[define::kMaxLevelOfTree];
    int level_cnt = 0;
    auto page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
    GlobalAddress leaf_head;

next_level:

    dsm->read_sync(page_buffer, p, kLeafPageSize);
    auto header = (Header *) (page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
    levels[level_cnt++] = p;
    if (header->level != 0)
    {
        p = header->leftmost_ptr;
        goto next_level;
    }
    else
    {
        leaf_head = p;
    }

next:
    dsm->read_sync(page_buffer, leaf_head, kLeafPageSize);
    auto page = (LeafPage *) page_buffer;
    for (int i = 0; i < kLeafCardinality; ++i)
    {
        if (page->records[i].value != kValueNull)
        {
#ifdef TEST_SINGLE_THREAD
            mapping.set(page->records[i].key, leaf_head);
#endif
        }
    }
    while (page->hdr.sibling_ptr != GlobalAddress::Null())
    {
        leaf_head = page->hdr.sibling_ptr;
        goto next;
    }

    // for (int i = 0; i < level_cnt; ++i) {
    //   dsm->read_sync(page_buffer, levels[i], kLeafPageSize);
    //   auto header = (Header *)(page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
    //   // std::cout << "addr: " << levels[i] << " ";
    //   // header->debug();
    //   // std::cout << " | ";
    //   while (header->sibling_ptr != GlobalAddress::Null()) {
    //     dsm->read_sync(page_buffer, header->sibling_ptr, kLeafPageSize);
    //     header = (Header *)(page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
    //     // std::cout << "addr: " << header->sibling_ptr << " ";
    //     // header->debug();
    //     // std::cout << " | ";
    //   }
    //   // std::cout << "\n------------------------------------" << std::endl;
    //   // std::cout << "------------------------------------" << std::endl;
    // }
}

GlobalAddress Tree::query_cache(const Key &k)
{
#ifdef TEST_SINGLE_THREAD
    return mapping.get(k);
#else
    return GlobalAddress::Null();
#endif
}

inline bool Tree::try_lock_addr(GlobalAddress lock_addr,
                                uint64_t tag,
                                uint64_t *buf,
                                CoroContext *cxt,
                                int coro_id)
{
    auto &pattern_cnt = pattern[dsm->getMyThreadID()][lock_addr.nodeID];

#ifdef CONFIG_ENABLE_HIERARCHIAL_LOCK
    bool hand_over = acquire_local_lock(lock_addr, cxt, coro_id);
    if (hand_over)
    {
        return true;
    }
#endif

#ifdef CONFIG_EABLE_BAKERY_LOCK
    {
        auto current_addr = GADD(lock_addr, sizeof(uint32_t));

#ifdef CONFIG_ENABLE_ON_CHIP_LOCK
        dsm->faa_dm_boundary_sync(lock_addr, 1, buf, 31, cxt);
#else
        dsm->faa_boundary_sync(lock_addr, 1, buf, 31, cxt);
#endif
        pattern_cnt++;
        uint32_t ticket = (*buf) << 32 >> 32;
        uint32_t current = (*buf) >> 32;

    re_read:
        if (ticket != current)
        {  // fail
            lock_fail[dsm->getMyThreadID()][0]++;

            assert(ticket > current);

            if (ticket != current + 1)
            {
                static const uint64_t wait_unit = 5000;
                uint64_t wait_time = wait_unit * (ticket - current - 1);

                if (cxt == nullptr)
                {
                    timer.sleep(wait_time);
                }
                else
                {
                    CoroDeadline task;
                    task.coro_id = coro_id;
                    task.deadline = timer.get_time_ns() + wait_time;
                    deadline_queue.push(task);
                    (*cxt->yield)(*cxt->master);
                }
            }
#ifdef CONFIG_ENABLE_ON_CHIP_LOCK
            dsm->read_dm_sync(
                (char *) buf, current_addr, sizeof(uint32_t), cxt);
#else
            dsm->read_sync((char *) buf, current_addr, sizeof(uint32_t), cxt);
#endif
            pattern_cnt++;
            current = (*buf);
            goto re_read;
        }
    }

#else
    {
        uint64_t retry_cnt = 0;
        uint64_t pre_tag = 0;
        uint64_t conflict_tag = 0;
    retry:
        retry_cnt++;
        if (retry_cnt > 1000000)
        {
            std::cout << "Deadlock " << lock_addr << std::endl;

            std::cout << dsm->getMyNodeID() << ", " << dsm->getMyThreadID()
                      << " locked by " << (conflict_tag >> 32) << ", "
                      << (conflict_tag << 32 >> 32) << std::endl;
            assert(false);
        }

#ifdef CONFIG_ENABLE_ON_CHIP_LOCK
        bool res = dsm->cas_dm_sync(lock_addr, 0, tag, buf, cxt);
#else
        bool res = dsm->cas_sync(lock_addr, 0, tag, buf, cxt);
#endif
        pattern_cnt++;
        if (!res)
        {
            conflict_tag = *buf - 1;
            if (conflict_tag != pre_tag)
            {
                retry_cnt = 0;
                pre_tag = conflict_tag;
            }
            lock_fail[dsm->getMyThreadID()][0]++;
            goto retry;
        }
    }

#endif

    return true;
}

inline void Tree::unlock_addr(GlobalAddress lock_addr,
                              uint64_t tag,
                              uint64_t *buf,
                              CoroContext *cxt,
                              int coro_id,
                              bool async)
{
#ifdef CONFIG_ENABLE_HIERARCHIAL_LOCK
    bool hand_over_other = can_hand_over(lock_addr);
    if (hand_over_other)
    {
        releases_local_lock(lock_addr);
        return;
    }
#endif

    auto cas_buf = dsm->get_rbuf(coro_id).get_cas_buffer();

#ifdef CONFIG_EABLE_BAKERY_LOCK

#ifdef CONFIG_ENABLE_ON_CHIP_LOCK
    if (async)
    {
        dsm->faa_dm_boundary(lock_addr, (1ull << 32), cas_buf, 63, false);
    }
    else
    {
        dsm->faa_dm_boundary_sync(lock_addr, (1ull << 32), cas_buf, 63, cxt);
    }
#else
    if (async)
    {
        dsm->faa_boundary(lock_addr, (1ull << 32), cas_buf, 63, false);
    }
    else
    {
        dsm->faa_boundary_sync(lock_addr, (1ull << 32), cas_buf, 63, cxt);
    }
#endif

#else

    *cas_buf = 0;
#ifdef CONFIG_ENABLE_ON_CHIP_LOCK
    if (async)
    {
        dsm->write_dm((char *) cas_buf, lock_addr, sizeof(uint64_t), false);
    }
    else
    {
        dsm->write_dm_sync((char *) cas_buf, lock_addr, sizeof(uint64_t), cxt);
    }
// if (async) { // TODO, tag in hand-over
//   dsm->cas_dm(lock_addr, tag, 0, cas_buf, false);
// } else {
//   dsm->cas_dm_sync(lock_addr, tag, 0, cas_buf, cxt);
// }
#else
    if (async)
    {
        dsm->write((char *) cas_buf, lock_addr, sizeof(uint64_t), false);
    }
    else
    {
        dsm->write_sync((char *) cas_buf, lock_addr, sizeof(uint64_t), cxt);
    }
// if (async) {
//   dsm->cas(lock_addr, tag, 0, cas_buf, false);
// } else {
//   dsm->cas_sync(lock_addr, tag, 0, cas_buf, cxt);
// }
#endif

#endif

#ifdef CONFIG_ENABLE_HIERARCHIAL_LOCK
    releases_local_lock(lock_addr);
#endif
}

void Tree::write_page_and_unlock(char *page_buffer,
                                 GlobalAddress page_addr,
                                 int page_size,
                                 uint64_t *cas_buffer,
                                 GlobalAddress lock_addr,
                                 uint64_t tag,
                                 CoroContext *cxt,
                                 int coro_id,
                                 bool async)
{
#ifdef CONFIG_ENABLE_OP_COUPLE

#ifdef CONFIG_ENABLE_HIERARCHIAL_LOCK
    bool hand_over_other = can_hand_over(lock_addr);
    if (hand_over_other)
    {
        dsm->write_sync(page_buffer, page_addr, page_size, cxt);
        releases_local_lock(lock_addr);
        return;
    }
#endif

    RdmaOpRegion rs[2];
    rs[0].source = (uint64_t) page_buffer;
    rs[0].dest = page_addr;
    rs[0].size = page_size;
    rs[0].is_on_chip = false;

    rs[1].source = (uint64_t) dsm->get_rbuf(coro_id).get_cas_buffer();
    rs[1].dest = lock_addr;
    rs[1].size = sizeof(uint64_t);

#ifdef CONFIG_ENABLE_ON_CHIP_LOCK
    rs[1].is_on_chip = true;
#else
    rs[1].is_on_chip = false;
#endif

#ifdef CONFIG_EABLE_BAKERY_LOCK
    if (async)
    {
        dsm->write_faa(rs[0], rs[1], (1ull << 32), false);
    }
    else
    {
        dsm->write_faa_sync(rs[0], rs[1], (1ull << 32), cxt);
    }
#else
    *(uint64_t *) rs[1].source = 0;
    if (async)
    {
        dsm->write_batch(rs, 2, false);
    }
    else
    {
        dsm->write_batch_sync(rs, 2, cxt);
    }
// if (async) {
//   dsm->write_cas(rs[0], rs[1], tag, 0, false);
// } else {
//   dsm->write_cas_sync(rs[0], rs[1], tag, 0, cxt);
// }
#endif

#ifdef CONFIG_ENABLE_HIERARCHIAL_LOCK
    releases_local_lock(lock_addr);
#endif

#else
    dsm->write_sync(page_buffer, page_addr, page_size, cxt);
    this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, async);
#endif
}

void Tree::lock_and_read_page(char *page_buffer,
                              GlobalAddress page_addr,
                              int page_size,
                              uint64_t *cas_buffer,
                              GlobalAddress lock_addr,
                              uint64_t tag,
                              CoroContext *cxt,
                              int coro_id)
{
// auto &pattern_cnt = pattern[dsm->getMyThreadID()][page_addr.nodeID];
#ifdef CONFIG_ENABLE_OP_COUPLE_D
    uint64_t retry_cnt = 0;
retry:
    if (retry_cnt++ > 1000)
    {
        std::cout << "Deadlock " << lock_addr.offset << std::endl;
        uint64_t conflict_tag = *cas_buffer - 1;
        std::cout << dsm->getMyNodeID() << ", " << dsm->getMyThreadID()
                  << " locked by " << (conflict_tag >> 32) << ", "
                  << (conflict_tag << 32 >> 32) << std::endl;
        assert(false);
    }

    if (retry_cnt == 0)
    {
        RdmaOpRegion cas_ror;
        RdmaOpRegion read_ror;
        cas_ror.source = (uint64_t) cas_buffer;
        cas_ror.dest = lock_addr;
        cas_ror.size = sizeof(uint64_t);
#ifdef CONFIG_ENABLE_ON_CHIP_LOCK
        cas_ror.is_on_chip = true;
#else
        cas_ror.is_on_chip = false;
#endif

        read_ror.source = (uint64_t) page_buffer;
        read_ror.dest = page_addr;
        read_ror.size = page_size;
        read_ror.is_on_chip = false;
        bool res = dsm->cas_read_sync(cas_ror, read_ror, 0, tag, cxt);
        if (!res)
        {
            goto retry;
        }
    }
    else
    {
        bool res = try_lock_addr(lock_addr, tag, cas_buffer, cxt, coro_id);
        if (!res)
        {
            goto retry;
        }
        dsm->read_sync(page_buffer, page_addr, page_size, cxt);
    }

#else

    try_lock_addr(lock_addr, tag, cas_buffer, cxt, coro_id);

    dsm->read_sync(page_buffer, page_addr, page_size, cxt);
    pattern[dsm->getMyThreadID()][page_addr.nodeID]++;

#endif
}

void Tree::lock_bench(const Key &k, CoroContext *cxt, int coro_id)
{
    uint64_t lock_index =
        CityHash64((char *) &k, sizeof(k)) % define::kNumOfLock;

    GlobalAddress lock_addr;
    lock_addr.nodeID = 0;
    lock_addr.offset = lock_index * sizeof(uint64_t);
    auto cas_buffer = dsm->get_rbuf(coro_id).get_cas_buffer();

    try_lock_addr(lock_addr, 1, cas_buffer, cxt, coro_id);
    unlock_addr(lock_addr, 1, cas_buffer, cxt, coro_id, true);
}

void Tree::insert_internal(
    const Key &k, GlobalAddress v, CoroContext *cxt, int coro_id, int level)
{
    auto root = get_root_ptr(cxt, coro_id);
    SearchResult result;

    GlobalAddress p = root;

next:

    if (!page_search(p, k, result, cxt, coro_id))
    {
        std::cout << "SEARCH WARNING insert" << std::endl;
        p = get_root_ptr(cxt, coro_id);
        sleep(1);
        goto next;
    }

    assert(result.level != 0);
    if (result.slibing != GlobalAddress::Null())
    {
        p = result.slibing;
        goto next;
    }

    p = result.next_level;
    if (result.level != level + 1)
    {
        goto next;
    }

    internal_page_store(p, k, v, root, level, cxt, coro_id);
}

void Tree::insert(const Key &k, const Value &v, CoroContext *cxt, int coro_id)
{
    assert(dsm->is_register());

    before_operation(cxt, coro_id);

#ifdef CONFIG_ENABLE_HOT_FILTER
    auto res = hot_buf.set(k);

    if (res == HotResult::OCCUPIED)
    {
        hot_filter_count[dsm->getMyThreadID()][0]++;
        if (cxt == nullptr)
        {
            while (!hot_buf.wait(k))
                ;
        }
        else
        {
            hot_wait_queue.push(coro_id);
            (*cxt->yield)(*cxt->master);
        }
    }
#endif

#ifdef TEST_SINGLE_THREAD

    GlobalAddress cache_addr = mapping.get(k);
    if (cache_addr != GlobalAddress::Null())
    {
        cache_hit[dsm->getMyThreadID()][0]++;
        auto root = get_root_ptr(cxt, coro_id);
        leaf_page_store(cache_addr, k, v, root, 0, cxt, coro_id);

#ifdef CONFIG_ENABLE_HOT_FILTER
        if (res == HotResult::SUCC)
        {
            hot_buf.clear(k);
        }
#endif
        return;
    }
    else
    {
        cache_miss[dsm->getMyThreadID()][0]++;
    }

#endif

    if (enable_cache)
    {
        GlobalAddress cache_addr;
        auto entry = index_cache->search_from_cache(k, &cache_addr);
        if (entry)
        {  // cache hit
            auto root = get_root_ptr(cxt, coro_id);
            if (leaf_page_store(cache_addr, k, v, root, 0, cxt, coro_id, true))
            {
                cache_hit[dsm->getMyThreadID()][0]++;
#ifdef CONFIG_ENABLE_HOT_FILTER
                if (res == HotResult::SUCC)
                {
                    hot_buf.clear(k);
                }
#endif
                return;
            }
            // cache stale, from root,
            index_cache->invalidate(entry);
        }
        cache_miss[dsm->getMyThreadID()][0]++;
    }

    auto root = get_root_ptr(cxt, coro_id);
    SearchResult result;

    GlobalAddress p = root;

next:

    if (!page_search(p, k, result, cxt, coro_id))
    {
        std::cout << "SEARCH WARNING insert" << std::endl;
        p = get_root_ptr(cxt, coro_id);
        sleep(1);
        goto next;
    }

    if (!result.is_leaf)
    {
        assert(result.level != 0);
        if (result.slibing != GlobalAddress::Null())
        {
            p = result.slibing;
            goto next;
        }

        p = result.next_level;
        if (result.level != 1)
        {
            goto next;
        }
    }

#ifdef TEST_SINGLE_THREAD
// mapping.set(k, p);
#endif

    leaf_page_store(p, k, v, root, 0, cxt, coro_id);

#ifdef CONFIG_ENABLE_HOT_FILTER
    if (res == HotResult::SUCC)
    {
        hot_buf.clear(k);
    }
#endif
}

bool Tree::search(const Key &k, Value &v, CoroContext *cxt, int coro_id)
{
    assert(dsm->is_register());

    auto root = get_root_ptr(cxt, coro_id);
    SearchResult result;

    GlobalAddress p = root;
#ifdef TEST_SINGLE_THREAD
    auto tmp = mapping.get(k);
    if (tmp != GlobalAddress::Null())
    {
        p = tmp;
    }
#endif

    bool from_cache = false;
    const CacheEntry *entry = nullptr;
    if (enable_cache)
    {
        GlobalAddress cache_addr;
        entry = index_cache->search_from_cache(k, &cache_addr);
        if (entry)
        {  // cache hit
            cache_hit[dsm->getMyThreadID()][0]++;
            from_cache = true;
            p = cache_addr;
        }
        else
        {
            cache_miss[dsm->getMyThreadID()][0]++;
        }
    }

next:
    if (!page_search(p, k, result, cxt, coro_id, from_cache))
    {
        if (from_cache)
        {  // cache stale
            index_cache->invalidate(entry);
            cache_hit[dsm->getMyThreadID()][0]--;
            cache_miss[dsm->getMyThreadID()][0]++;
            from_cache = false;

            p = root;
        }
        else
        {
            std::cout << "SEARCH WARNING search" << std::endl;
            sleep(1);
        }
        goto next;
    }
    if (result.is_leaf)
    {
        if (result.val != kValueNull)
        {  // find
            v = result.val;
            return true;
        }
        if (result.slibing != GlobalAddress::Null())
        {  // turn right
            p = result.slibing;
            goto next;
        }
        return false;  // not found
    }
    else
    {  // internal
        p = result.slibing != GlobalAddress::Null() ? result.slibing
                                                    : result.next_level;
        goto next;
    }
}

void Tree::del(const Key &k, CoroContext *cxt, int coro_id)
{
    assert(dsm->is_register());
    before_operation(cxt, coro_id);

    auto root = get_root_ptr(cxt, coro_id);
    SearchResult result;

    GlobalAddress p = root;

next:
    if (!page_search(p, k, result, cxt, coro_id))
    {
        std::cout << "SEARCH WARNING" << std::endl;
        goto next;
    }

    if (!result.is_leaf)
    {
        assert(result.level != 0);
        if (result.slibing != GlobalAddress::Null())
        {
            p = result.slibing;
            goto next;
        }

        p = result.next_level;
        if (result.level != 1)
        {
            goto next;
        }
    }

    leaf_page_del(p, k, 0, cxt, coro_id);
}

bool Tree::page_search(GlobalAddress page_addr,
                       const Key &k,
                       SearchResult &result,
                       CoroContext *cxt,
                       int coro_id,
                       bool from_cache)
{
    auto page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
    auto header = (Header *) (page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
    auto &pattern_cnt = pattern[dsm->getMyThreadID()][page_addr.nodeID];

    int counter = 0;
re_read:
    if (++counter > 100)
    {
        printf("re read too many times\n");
        sleep(1);
    }
    dsm->read_sync(page_buffer, page_addr, kLeafPageSize, cxt);
    pattern_cnt++;

    memset(&result, 0, sizeof(result));
    result.is_leaf = header->leftmost_ptr == GlobalAddress::Null();
    result.level = header->level;
    path_stack[coro_id][result.level] = page_addr;
    // std::cout << "level " << (int)result.level << " " << page_addr <<
    // std::endl;

    if (result.is_leaf)
    {
        auto page = (LeafPage *) page_buffer;
        if (!page->check_consistent())
        {
            goto re_read;
        }

        if (from_cache && (k < page->hdr.lowest || k >= page->hdr.highest))
        {  // cache is stale
            return false;
        }

        assert(result.level == 0);
        if (k >= page->hdr.highest)
        {  // should turn right
            result.slibing = page->hdr.sibling_ptr;
            return true;
        }
        if (k < page->hdr.lowest)
        {
            assert(false);
            return false;
        }
        leaf_page_search(page, k, result);
    }
    else
    {
        assert(result.level != 0);
        assert(!from_cache);
        auto page = (InternalPage *) page_buffer;

        if (!page->check_consistent())
        {
            goto re_read;
        }

        if (result.level == 1 && enable_cache)
        {
            index_cache->add_to_cache(page);
            // if (enter_debug) {
            //   printf("add %lud [%lud %lud]\n", k, page->hdr.lowest,
            //          page->hdr.highest);
            // }
        }

        if (k >= page->hdr.highest)
        {  // should turn right
            result.slibing = page->hdr.sibling_ptr;
            return true;
        }
        if (k < page->hdr.lowest)
        {
            printf("key %ld error in level %d\n", k, page->hdr.level);
            sleep(10);
            print_and_check_tree();
            assert(false);
            return false;
        }
        internal_page_search(page, k, result);
    }

    return true;
}

void Tree::internal_page_search(InternalPage *page,
                                const Key &k,
                                SearchResult &result)
{
    assert(k >= page->hdr.lowest);
    assert(k < page->hdr.highest);

    auto cnt = page->hdr.last_index + 1;
    // page->debug();
    if (k < page->records[0].key)
    {
        result.next_level = page->hdr.leftmost_ptr;
        return;
    }

    for (int i = 1; i < cnt; ++i)
    {
        if (k < page->records[i].key)
        {
            result.next_level = page->records[i - 1].ptr;
            return;
        }
    }
    result.next_level = page->records[cnt - 1].ptr;
}

void Tree::leaf_page_search(LeafPage *page, const Key &k, SearchResult &result)
{
#ifdef CONFIG_ENABLE_FINER_VERSION
    for (int i = 0; i < kLeafCardinality; ++i)
    {
        auto &r = page->records[i];
        if (r.key == k && r.value != kValueNull && r.f_version == r.r_version)
        {
            result.val = r.value;
            break;
        }
    }
#else
    auto cnt = page->hdr.last_index + 1;
    for (int i = 0; i < cnt; ++i)
    {
        if (page->records[i].key == k)
        {
            result.val = page->records[i].value;
            break;
        }
    }
#endif
}

void Tree::internal_page_store(GlobalAddress page_addr,
                               const Key &k,
                               GlobalAddress v,
                               GlobalAddress root,
                               int level,
                               CoroContext *cxt,
                               int coro_id)
{
    uint64_t lock_index =
        CityHash64((char *) &page_addr, sizeof(page_addr)) % define::kNumOfLock;

    GlobalAddress lock_addr;
    lock_addr.nodeID = page_addr.nodeID;
    lock_addr.offset = lock_index * sizeof(uint64_t);

    auto &rbuf = dsm->get_rbuf(coro_id);
    uint64_t *cas_buffer = rbuf.get_cas_buffer();
    auto page_buffer = rbuf.get_page_buffer();

    auto tag = dsm->getThreadTag();
    assert(tag != 0);

    lock_and_read_page(page_buffer,
                       page_addr,
                       kInternalPageSize,
                       cas_buffer,
                       lock_addr,
                       tag,
                       cxt,
                       coro_id);

    auto page = (InternalPage *) page_buffer;

    assert(page->hdr.level == level);
    assert(page->check_consistent());
    if (k >= page->hdr.highest)
    {
        this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, true);

        assert(page->hdr.sibling_ptr != GlobalAddress::Null());

        this->internal_page_store(
            page->hdr.sibling_ptr, k, v, root, level, cxt, coro_id);

        return;
    }
    assert(k >= page->hdr.lowest);

    auto cnt = page->hdr.last_index + 1;

    bool is_update = false;
    uint16_t insert_index = 0;
    for (int i = cnt - 1; i >= 0; --i)
    {
        if (page->records[i].key == k)
        {  // find and update
            page->records[i].ptr = v;
            // assert(false);
            printf("KKKKK\n");
            is_update = true;
            break;
        }
        if (page->records[i].key < k)
        {
            insert_index = i + 1;
            break;
        }
    }

    assert(cnt != kInternalCardinality);

    if (!is_update)
    {  // insert and shift
        for (int i = cnt; i > insert_index; --i)
        {
            page->records[i].key = page->records[i - 1].key;
            page->records[i].ptr = page->records[i - 1].ptr;
        }
        page->records[insert_index].key = k;
        page->records[insert_index].ptr = v;

        page->hdr.last_index++;
    }

    cnt = page->hdr.last_index + 1;
    bool need_split = cnt == kInternalCardinality;
    Key split_key;
    GlobalAddress sibling_addr;
    if (need_split)
    {  // need split
        sibling_addr = dsm->alloc(kInternalPageSize);
        auto sibling_buf = rbuf.get_sibling_buffer();

        auto sibling = new (sibling_buf) InternalPage(page->hdr.level);

        //    std::cout << "addr " <<  sibling_addr << " | level " <<
        //    (int)(page->hdr.level) << std::endl;

        int m = cnt / 2;
        split_key = page->records[m].key;
        assert(split_key > page->hdr.lowest);
        assert(split_key < page->hdr.highest);
        for (int i = m + 1; i < cnt; ++i)
        {  // move
            sibling->records[i - m - 1].key = page->records[i].key;
            sibling->records[i - m - 1].ptr = page->records[i].ptr;
        }
        page->hdr.last_index -= (cnt - m);
        sibling->hdr.last_index += (cnt - m - 1);

        sibling->hdr.leftmost_ptr = page->records[m].ptr;
        sibling->hdr.lowest = page->records[m].key;
        sibling->hdr.highest = page->hdr.highest;
        page->hdr.highest = page->records[m].key;

        // link
        sibling->hdr.sibling_ptr = page->hdr.sibling_ptr;
        page->hdr.sibling_ptr = sibling_addr;

        sibling->set_consistent();
        dsm->write_sync(sibling_buf, sibling_addr, kInternalPageSize, cxt);
    }

    page->set_consistent();
    write_page_and_unlock(page_buffer,
                          page_addr,
                          kInternalPageSize,
                          cas_buffer,
                          lock_addr,
                          tag,
                          cxt,
                          coro_id,
                          need_split);

    if (!need_split)
        return;

    if (root == page_addr)
    {  // update root

        if (update_new_root(page_addr,
                            split_key,
                            sibling_addr,
                            level + 1,
                            root,
                            cxt,
                            coro_id))
        {
            return;
        }
    }

    auto up_level = path_stack[coro_id][level + 1];

    if (up_level != GlobalAddress::Null())
    {
        internal_page_store(
            up_level, split_key, sibling_addr, root, level + 1, cxt, coro_id);
    }
    else
    {
        assert(false);
    }
}

bool Tree::leaf_page_store(GlobalAddress page_addr,
                           const Key &k,
                           const Value &v,
                           GlobalAddress root,
                           int level,
                           CoroContext *cxt,
                           int coro_id,
                           bool from_cache)
{
    uint64_t lock_index =
        CityHash64((char *) &page_addr, sizeof(page_addr)) % define::kNumOfLock;

    GlobalAddress lock_addr;

#ifdef CONFIG_ENABLE_EMBEDDING_LOCK
    lock_addr = page_addr;
#else
    lock_addr.nodeID = page_addr.nodeID;
    lock_addr.offset = lock_index * sizeof(uint64_t);
#endif

    auto &rbuf = dsm->get_rbuf(coro_id);
    uint64_t *cas_buffer = rbuf.get_cas_buffer();
    auto page_buffer = rbuf.get_page_buffer();

    auto tag = dsm->getThreadTag();
    assert(tag != 0);

    lock_and_read_page(page_buffer,
                       page_addr,
                       kLeafPageSize,
                       cas_buffer,
                       lock_addr,
                       tag,
                       cxt,
                       coro_id);

    auto page = (LeafPage *) page_buffer;

    assert(page->hdr.level == level);
    assert(page->check_consistent());

    if (from_cache && (k < page->hdr.lowest || k >= page->hdr.highest))
    {  // cache is stale
        this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, true);

        // if (enter_debug) {
        //   printf("cache {%lu} %lu [%lu %lu]\n", page_addr.val, k,
        //   page->hdr.lowest,
        //          page->hdr.highest);
        // }

        return false;
    }

    // if (enter_debug) {
    //   printf("{%lu} %lu [%lu %lu]\n", page_addr.val, k, page->hdr.lowest,
    //          page->hdr.highest);
    // }

    if (k >= page->hdr.highest)
    {
        this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, true);

        assert(page->hdr.sibling_ptr != GlobalAddress::Null());

#ifdef TEST_SINGLE_THREAD
// mapping.set(k, page->hdr.sibling_ptr);
#endif
        this->leaf_page_store(
            page->hdr.sibling_ptr, k, v, root, level, cxt, coro_id);

        return true;
    }
    assert(k >= page->hdr.lowest);

#ifdef CONFIG_ENABLE_FINER_VERSION
    int cnt = 0;
    int empty_index = -1;
    char *update_addr = nullptr;
    for (int i = 0; i < kLeafCardinality; ++i)
    {
        auto &r = page->records[i];
        if (r.value != kValueNull)
        {
            cnt++;
            if (r.key == k)
            {
                r.value = v;
                r.f_version++;
                r.r_version = r.f_version;
                update_addr = (char *) &r;
                break;
            }
        }
        else if (empty_index == -1)
        {
            empty_index = i;
        }
    }

    assert(cnt != kLeafCardinality);

    if (update_addr == nullptr)
    {  // insert new item
        if (empty_index == -1)
        {
            printf("%d cnt\n", cnt);
            assert(false);
        }

        auto &r = page->records[empty_index];
        r.key = k;
        r.value = v;
        r.f_version++;
        r.r_version = r.f_version;

        update_addr = (char *) &r;

        cnt++;
    }

    bool need_split = cnt == kLeafCardinality;
    if (!need_split)
    {
        assert(update_addr);
        write_page_and_unlock(update_addr,
                              GADD(page_addr, (update_addr - (char *) page)),
                              sizeof(LeafEntry),
                              cas_buffer,
                              lock_addr,
                              tag,
                              cxt,
                              coro_id,
                              false);

        return true;
    }
    else
    {
        std::sort(page->records,
                  page->records + kLeafCardinality,
                  [](const LeafEntry &a, const LeafEntry &b) {
                      return a.key < b.key;
                  });
    }
#else
    auto cnt = page->hdr.last_index + 1;
    bool is_update = false;
    uint16_t insert_index = 0;
    for (int i = cnt - 1; i >= 0; --i)
    {
        if (page->records[i].key == k)
        {  // find and update
            page->records[i].value = v;

            is_update = true;
            break;
        }
        if (page->records[i].key < k)
        {
            insert_index = i + 1;
            break;
        }
    }

    assert(cnt != kLeafCardinality);

    if (!is_update)
    {  // insert and shift
        for (int i = cnt; i > insert_index; --i)
        {
            page->records[i].key = page->records[i - 1].key;
            page->records[i].value = page->records[i - 1].value;
        }
        page->records[insert_index].key = k;
        page->records[insert_index].value = v;

        page->hdr.last_index++;
    }

    cnt = page->hdr.last_index + 1;
    bool need_split = cnt == kLeafCardinality;
#endif

    Key split_key;
    GlobalAddress sibling_addr;
    if (need_split)
    {  // need split
        sibling_addr = dsm->alloc(kLeafPageSize);
        auto sibling_buf = rbuf.get_sibling_buffer();

        auto sibling = new (sibling_buf) LeafPage(page->hdr.level);

        // std::cout << "addr " <<  sibling_addr << " | level " <<
        // (int)(page->hdr.level) << std::endl;

        int m = cnt / 2;
        split_key = page->records[m].key;
        assert(split_key > page->hdr.lowest);
        assert(split_key < page->hdr.highest);

        for (int i = m; i < cnt; ++i)
        {  // move
            sibling->records[i - m].key = page->records[i].key;
            sibling->records[i - m].value = page->records[i].value;
#ifdef CONFIG_ENABLE_FINER_VERSION
            page->records[i].key = 0;
            page->records[i].value = kValueNull;
#endif
        }
        page->hdr.last_index -= (cnt - m);
        sibling->hdr.last_index += (cnt - m);

        sibling->hdr.lowest = split_key;
        sibling->hdr.highest = page->hdr.highest;
        page->hdr.highest = split_key;

        // link
        sibling->hdr.sibling_ptr = page->hdr.sibling_ptr;
        page->hdr.sibling_ptr = sibling_addr;

        sibling->set_consistent();
        dsm->write_sync(sibling_buf, sibling_addr, kLeafPageSize, cxt);
    }

    page->set_consistent();

    write_page_and_unlock(page_buffer,
                          page_addr,
                          kLeafPageSize,
                          cas_buffer,
                          lock_addr,
                          tag,
                          cxt,
                          coro_id,
                          need_split);

    if (!need_split)
        return true;

    if (root == page_addr)
    {  // update root
        if (update_new_root(page_addr,
                            split_key,
                            sibling_addr,
                            level + 1,
                            root,
                            cxt,
                            coro_id))
        {
            return true;
        }
    }

    auto up_level = path_stack[coro_id][level + 1];

    if (up_level != GlobalAddress::Null())
    {
        internal_page_store(
            up_level, split_key, sibling_addr, root, level + 1, cxt, coro_id);
    }
    else
    {
        assert(from_cache);
        insert_internal(split_key, sibling_addr, cxt, coro_id, level + 1);
    }

    return true;
}

// Need BIG FIX
void Tree::leaf_page_del(GlobalAddress page_addr,
                         const Key &k,
                         int level,
                         CoroContext *cxt,
                         int coro_id)
{
    uint64_t lock_index =
        CityHash64((char *) &page_addr, sizeof(page_addr)) % define::kNumOfLock;

    GlobalAddress lock_addr;
    lock_addr.nodeID = page_addr.nodeID;
    lock_addr.offset = lock_index * sizeof(uint64_t);

    uint64_t *cas_buffer = dsm->get_rbuf(coro_id).get_cas_buffer();

    auto tag = dsm->getThreadTag();
    try_lock_addr(lock_addr, tag, cas_buffer, cxt, coro_id);

    auto page_buffer = dsm->get_rbuf(coro_id).get_page_buffer();
    dsm->read_sync(page_buffer, page_addr, kLeafPageSize, cxt);
    auto page = (LeafPage *) page_buffer;

    assert(page->hdr.level == level);
    assert(page->check_consistent());
    if (k >= page->hdr.highest)
    {
        this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, true);

        assert(page->hdr.sibling_ptr != GlobalAddress::Null());

        this->leaf_page_del(page->hdr.sibling_ptr, k, level, cxt, coro_id);
    }

    auto cnt = page->hdr.last_index + 1;

    int del_index = -1;
    for (int i = 0; i < cnt; ++i)
    {
        if (page->records[i].key == k)
        {  // find and update
            del_index = i;
            break;
        }
    }

    if (del_index != -1)
    {  // remove and shift
        for (int i = del_index + 1; i < cnt; ++i)
        {
            page->records[i - 1].key = page->records[i].key;
            page->records[i - 1].value = page->records[i].value;
        }

        page->hdr.last_index--;

        page->set_consistent();
        dsm->write_sync(page_buffer, page_addr, kLeafPageSize, cxt);
    }
    this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, false);
}

void Tree::run_coroutine(CoroFunc func, int id, int coro_cnt)
{
    using namespace std::placeholders;

    assert(coro_cnt <= define::kMaxCoro);
    for (int i = 0; i < coro_cnt; ++i)
    {
        auto gen = func(i, dsm.get(), id);
        worker[i] = CoroCall(std::bind(&Tree::coro_worker, this, _1, gen, i));
    }

    master = CoroCall(std::bind(&Tree::coro_master, this, _1, coro_cnt));

    master();
}

void Tree::coro_worker(CoroYield &yield, RequstGen *gen, int coro_id)
{
    CoroContext ctx;
    ctx.coro_id = coro_id;
    ctx.master = &master;
    ctx.yield = &yield;

    Timer coro_timer;
    auto thread_id = dsm->getMyThreadID();

    while (true)
    {
        auto r = gen->next();

        coro_timer.begin();
        if (r.is_search)
        {
            Value v;
            this->search(r.k, v, &ctx, coro_id);
        }
        else
        {
            this->insert(r.k, r.v, &ctx, coro_id);
        }
        auto us_10 = coro_timer.end() / 100;
        if (us_10 >= LATENCY_WINDOWS)
        {
            us_10 = LATENCY_WINDOWS - 1;
        }
        latency[thread_id][us_10]++;
    }
}

void Tree::coro_master(CoroYield &yield, int coro_cnt)
{
    for (int i = 0; i < coro_cnt; ++i)
    {
        yield(worker[i]);
    }

    while (true)
    {
        uint64_t next_coro_id;

        if (dsm->poll_rdma_cq_once(next_coro_id))
        {
            yield(worker[next_coro_id]);
        }

        if (!hot_wait_queue.empty())
        {
            next_coro_id = hot_wait_queue.front();
            hot_wait_queue.pop();
            yield(worker[next_coro_id]);
        }

        if (!deadline_queue.empty())
        {
            auto now = timer.get_time_ns();
            auto task = deadline_queue.top();
            if (now > task.deadline)
            {
                deadline_queue.pop();
                yield(worker[task.coro_id]);
            }
        }
    }
}

// Local Locks
inline bool Tree::acquire_local_lock(GlobalAddress lock_addr,
                                     CoroContext *cxt,
                                     int coro_id)
{
    auto &node = local_locks[lock_addr.nodeID][lock_addr.offset / 8];
    bool is_local_locked = false;

    uint64_t lock_val = node.ticket_lock.fetch_add(1);

    uint32_t ticket = lock_val << 32 >> 32;
    uint32_t current = lock_val >> 32;

    // printf("%ud %ud\n", ticket, current);
    while (ticket != current)
    {  // lock failed
        is_local_locked = true;

        if (cxt != nullptr)
        {
            hot_wait_queue.push(coro_id);
            (*cxt->yield)(*cxt->master);
        }

        current = node.ticket_lock.load(std::memory_order_relaxed) >> 32;
    }

    if (is_local_locked)
    {
        hierarchy_lock[dsm->getMyThreadID()][0]++;
    }

    node.hand_time++;
    return node.hand_over;
}

inline bool Tree::can_hand_over(GlobalAddress lock_addr)
{
    auto &node = local_locks[lock_addr.nodeID][lock_addr.offset / 8];
    uint64_t lock_val = node.ticket_lock.load(std::memory_order_relaxed);

    uint32_t ticket = lock_val << 32 >> 32;
    uint32_t current = lock_val >> 32;

    if (ticket <= current + 1)
    {  // no pending locks
        node.hand_over = false;
    }
    else
    {
        node.hand_over = node.hand_time < define::kMaxHandOverTime;
    }
    if (!node.hand_over)
    {
        node.hand_time = 0;
    }
    else
    {
        handover_count[dsm->getMyThreadID()][0]++;
    }

    return node.hand_over;
}

inline void Tree::releases_local_lock(GlobalAddress lock_addr)
{
    auto &node = local_locks[lock_addr.nodeID][lock_addr.offset / 8];
    node.ticket_lock.fetch_add((1ull << 32));
}

void Tree::index_cache_statistics()
{
    index_cache->statistics();
    index_cache->bench();
}

void Tree::clear_statistics()
{
    for (int i = 0; i < MAX_APP_THREAD; ++i)
    {
        cache_hit[i][0] = 0;
        cache_miss[i][0] = 0;
    }
}