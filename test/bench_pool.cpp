#include <algorithm>
#include <random>
#include <set>

#include "DSM.h"
#include "Timer.h"
#include "util/monitor.h"

// Two nodes
// one node issues cas operations

struct Object
{
    uint64_t lower;
    uint64_t upper;
};

constexpr static size_t kTestTime = 10 * define::M;

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    
    {
        char __buffer[sizeof(Object) * 1024];
        ThreadUnsafeBufferPool<sizeof(Object)> buff_pool(__buffer,
                                                         sizeof(Object) * 1024);
        auto now = std::chrono::steady_clock::now();
        std::vector<void *> addrs;
        addrs.reserve(1024);
        for (size_t i = 0; i < kTestTime; ++i)
        {
            bool get;
            if (addrs.size() == 0)
            {
                get = true;
            }
            else if (addrs.size() == 1024)
            {
                get = false;
            }
            else
            {
                get = rand() % 2 == 0;
            }
            if (get)
            {
                addrs.push_back(buff_pool.get());
            }
            else
            {
                auto *addr = addrs.back();
                addrs.pop_back();
                buff_pool.put(addr);
            }
        }
        auto then = std::chrono::steady_clock::now();
        auto ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(then - now)
                .count();
        LOG(INFO) << "[bench] buf pool, ns: " << ns << ", op: " << kTestTime
                  << ", ops: " << 1.0 * 1e9 * kTestTime / ns;
        for (auto* addr: addrs)
        {
            buff_pool.put(addr);
        }
    }

    {
        ThreadUnsafePool<Object, 1024> obj_pool;
        std::vector<Object *> addrs;
        addrs.reserve(1024);

        auto now = std::chrono::steady_clock::now();
        for (size_t i = 0; i < kTestTime; ++i)
        {
            bool get;
            if (addrs.size() == 0)
            {
                get = true;
            }
            else if (addrs.size() == 1024)
            {
                get = false;
            }
            else
            {
                get = rand() % 2 == 0;
            }
            if (get)
            {
                addrs.push_back(obj_pool.get());
            }
            else
            {
                auto *addr = addrs.back();
                addrs.pop_back();
                obj_pool.put(addr);
            }
        }
        auto then = std::chrono::steady_clock::now();
        auto ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(then - now)
                .count();
        LOG(INFO) << "[bench] obj pool, ns: " << ns << ", op: " << kTestTime
                  << ", ops: " << 1.0 * 1e9 * kTestTime / ns;
        for (auto* addr: addrs)
        {
            obj_pool.put(addr);
        }

    }

}