#include <algorithm>
#include <random>

#include "HugePageAlloc.h"
#include "Timer.h"
#include "patronus/Patronus.h"
#include "patronus/memory/direct_allocator.h"
#include "patronus/memory/slab_allocator.h"
#include "thirdparty/memory/nginx/ngx_palloc.h"
#include "util/monitor.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

using namespace patronus::mem;

constexpr static auto kMemoryPoolSize = 4_GB;

void test_use_large_buffer_pool(void *addr)
{
    auto *ngx_pool =
        CHECK_NOTNULL(ngx_create_pool(addr, kMemoryPoolSize, nullptr));

    std::set<void *> s;
    while (true)
    {
        size_t size = rand() % 2_MB;
        auto *a = ngx_palloc(ngx_pool, size);
        if (a == nullptr)
        {
            break;
        }
        CHECK_EQ(s.count(a), 0);
        s.insert(a);
    }
    LOG(INFO) << "inserted: " << s.size() << ", effectiveness: "
              << 1.0 * s.size() / (kMemoryPoolSize / (2_MB / 2));
    // for (void *addr : s)
    // {
    //     CHECK_EQ(ngx_pfree(ngx_pool, addr), NGX_OK);
    // }
    LOG(WARNING)
        << "Currently nginx pool does not support free-ing small objects";

    ngx_destroy_pool(ngx_pool);
}

void test_use_slab_allocator(void *addr)
{
    SlabAllocatorConfig conf;
    conf.block_class = {1_GB};
    conf.block_ratio = {1};
    auto allocator =
        std::make_shared<SlabAllocator>(addr, kMemoryPoolSize, conf);

    LOG(INFO) << "[bench] allocator: " << *allocator;

    auto *ngx_pool = CHECK_NOTNULL(ngx_create_pool(nullptr, 1_GB, allocator));

    std::set<void *> s;
    while (true)
    {
        size_t size = rand() % 2_MB;
        auto *a = ngx_palloc(ngx_pool, size);
        if (a == nullptr)
        {
            break;
        }
        CHECK_EQ(s.count(a), 0);
        s.insert(a);
    }
    LOG(INFO) << "inserted: " << s.size() << ", effectiveness: "
              << 1.0 * s.size() * (2_MB / 2) /
                     allocator->debug_allocated_bytes();
    // for (void *addr : s)
    // {
    //     CHECK_EQ(ngx_pfree(ngx_pool, addr), NGX_OK);
    // }

    LOG(WARNING)
        << "Currently nginx pool does not support free-ing small objects";

    ngx_destroy_pool(ngx_pool);

    LOG(INFO) << "[bench] " << *allocator;
}

void test_use_raw_allocator()
{
    // SlabAllocatorConfig conf;
    // conf.block_class = {1_GB};
    // conf.block_ratio = {1};
    // auto allocator =
    //     std::make_shared<SlabAllocator>(addr, kMemoryPoolSize, conf);
    auto allocator = std::make_shared<DirectAllocator>();

    auto *ngx_pool = CHECK_NOTNULL(ngx_create_pool(nullptr, 1_GB, allocator));

    std::set<void *> s;
    while (true)
    {
        size_t size = rand() % 2_MB;
        auto *a = ngx_palloc(ngx_pool, size);
        if (a == nullptr)
        {
            break;
        }
        CHECK_EQ(s.count(a), 0);
        s.insert(a);
    }
    LOG(INFO) << "inserted: " << s.size() << ", effectiveness: "
              << 1.0 * s.size() * (2_MB / 2) /
                     allocator->debug_allocated_bytes();
    // for (void *addr : s)
    // {
    //     CHECK_EQ(ngx_pfree(ngx_pool, addr), NGX_OK);
    // }

    LOG(WARNING)
        << "Currently nginx pool does not support free-ing small objects";

    ngx_destroy_pool(ngx_pool);
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    LOG(INFO) << "[bench] begin test raw allocator";
    test_use_raw_allocator();

    auto *addr = CHECK_NOTNULL(hugePageAlloc(kMemoryPoolSize));
    LOG(INFO) << "[bench] begin test use allocator";
    test_use_slab_allocator(addr);

    LOG(INFO) << "[bench] begin test use buffer pool";
    test_use_large_buffer_pool(addr);

    LOG(INFO) << "finished. ctrl+C to quit.";
}