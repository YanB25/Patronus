#include <algorithm>
#include <random>

#include "HugePageAlloc.h"
#include "Timer.h"
#include "patronus/Patronus.h"
#include "patronus/memory/block_allocator.h"
#include "util/monitor.h"

using namespace patronus::mem;

void test_4kb_2mb(BlockAllocator &allocator)
{
    std::set<void *> addr_4kb;
    std::set<void *> addr_2mb;

    while (true)
    {
        auto size = rand() % 4_KB;
        auto *a = allocator.alloc(size);
        if (a == nullptr)
        {
            break;
        }
        CHECK(addr_4kb.count(a) == 0);
        CHECK_EQ((uint64_t) a % 4_KB, 0);
        addr_4kb.insert(a);
    }
    while (true)
    {
        auto size = rand() % 2_MB;
        if (size <= 4_KB)
        {
            continue;
        }

        auto *a = allocator.alloc(size);
        if (a == nullptr)
        {
            break;
        }
        CHECK(addr_2mb.count(a) == 0);
        CHECK_EQ((uint64_t) a % 2_MB, 0);

        addr_2mb.insert(a);
    }

    for (auto *addr : addr_4kb)
    {
        allocator.free(addr);
    }
    for (auto *addr : addr_2mb)
    {
        allocator.free(addr);
    }
}

void test_burn(BlockAllocator &allocator)
{
    std::set<void *> s;
    for (size_t i = 0; i < 5_M; ++i)
    {
        int op = rand() % 2;
        if (op == 0)
        {
            // alloc
            size_t size = rand() % 2_MB;
            auto *a = allocator.alloc(size);
            CHECK_EQ(s.count(a), 0);
            s.insert(a);
        }
        else
        {
            if (s.empty())
            {
                continue;
            }
            auto it = s.begin();
            allocator.free(*it);
            s.erase(it);
        }
    }
    for (void *addr : s)
    {
        allocator.free(addr);
    }
}

void test_complex(BlockAllocator &allocator)
{
    std::set<void *> s;
    size_t set_max_size = 0;
    for (size_t i = 0; i < 1_M; ++i)
    {
        int op = rand() % 3;
        if (op != 0)
        {
            // alloc
            auto clz = rand() % 6;
            size_t size = 0;
            if (clz == 0)
            {
                size = rand() % 64;
            }
            else if (clz == 1)
            {
                size = rand() % 256;
            }
            else if (clz == 2)
            {
                size = rand() % 4_KB;
            }
            else if (clz == 3)
            {
                size = rand() % 2_MB;
            }
            else if (clz == 4)
            {
                size = rand() % 512_MB;
            }
            else if (clz == 5)
            {
                size = rand() % 1_GB;
            }
            auto *a = allocator.alloc(size);
            if (a != nullptr)
            {
                CHECK_EQ(s.count(a), 0);
                s.insert(a);
            }
        }
        else
        {
            if (s.empty())
            {
                continue;
            }
            auto it = s.begin();
            allocator.free(*it);
            s.erase(it);
        }
        set_max_size = std::max(set_max_size, s.size());
    }
    for (void *addr : s)
    {
        allocator.free(addr);
    }
    LOG(INFO) << "set_max_size: " << set_max_size;
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    auto *addr = hugePageAlloc(16_GB);

    BlockAllocatorConfig conf;
    conf.block_class = {4_KB, 2_MB};
    conf.block_ratio = {0.5, 0.5};
    BlockAllocator allocator(addr, 16_GB, conf);

    LOG(INFO) << "[bench] allocator: " << allocator;

    test_4kb_2mb(allocator);
    LOG(INFO) << "Pass basic check. start burning";
    test_burn(allocator);

    LOG(INFO) << "Pass burning, start complex test";
    BlockAllocatorConfig complex_conf;
    complex_conf.block_class = {4_KB, 2_MB, 512_MB, 1_GB};
    complex_conf.block_ratio = {1.0 / 8, 1.0 / 8, 1.0 / 8, 3.0 / 8};
    BlockAllocator complex_alloc(addr, 16_GB, complex_conf);
    LOG(INFO) << "[bench] complex allocator: " << complex_alloc;
    test_complex(complex_alloc);

    LOG(INFO) << "finished. ctrl+C to quit.";
}