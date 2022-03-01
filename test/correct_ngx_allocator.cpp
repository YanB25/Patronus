#include <algorithm>
#include <random>

#include "HugePageAlloc.h"
#include "Timer.h"
#include "patronus/Patronus.h"
#include "thirdparty/memory/nginx/ngx_palloc.h"
#include "util/monitor.h"

using namespace patronus::mem;

constexpr static auto kMemoryPoolSize = 4_GB;

void test_use_large_buffer_pool(void *addr)
{
    auto *ngx_pool = CHECK_NOTNULL(ngx_create_pool(addr, 1_GB, nullptr));

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
    LOG(INFO) << "inserted: " << s.size()
              << ", effectiveness: " << 1.0 * s.size() / (1_GB / (2_MB / 2));
    for (void *addr : s)
    {
        ngx_pfree(ngx_pool, addr);
    }

    ngx_destroy_pool(ngx_pool);
}

void test_use_allocator(void *addr)
{
    BlockAllocatorConfig conf;
    conf.block_class = {1_GB};
    conf.block_ratio = {1};
    auto allocator =
        std::make_shared<BlockAllocator>(addr, kMemoryPoolSize, conf);

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
    for (void *addr : s)
    {
        ngx_pfree(ngx_pool, addr);
    }

    ngx_destroy_pool(ngx_pool);

    LOG(INFO) << "[bench] " << *allocator;
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    auto *addr = hugePageAlloc(kMemoryPoolSize);
    LOG(INFO) << "[bench] begin test use allocator";
    test_use_allocator(addr);

    LOG(INFO) << "[bench] begin test use buffer pool";
    test_use_large_buffer_pool(addr);

    LOG(INFO) << "finished. ctrl+C to quit.";
}