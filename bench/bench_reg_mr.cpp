#include <algorithm>
#include <random>
#include <set>

#include "Common.h"
#include "Rdma.h"
#include "glog/logging.h"

using namespace define::literals;

void bench_alloc(size_t total_size, size_t alloc_size)
{
    std::vector<void *> addrs;
    size_t alloc_nr = total_size / alloc_size;
    addrs.reserve(alloc_nr + 1);

    ChronoTimer timer;
    for (size_t i = 0; i < alloc_nr; ++i)
    {
        addrs.push_back(hugePageAlloc(alloc_size));
    }
    auto alloc_ns = timer.pin();

    for (void *addr : addrs)
    {
        hugePageFree(addr, alloc_size);
    }
    auto free_ns = timer.pin();

    LOG(INFO) << "[bench-alloc] alloc nr: " << alloc_nr;
    LOG(INFO) << "[bench-alloc] hugePageAlloc: " << alloc_nr << " times, takes "
              << alloc_ns << " ns, avg-latency: " << 1.0 * alloc_ns / alloc_nr
              << " ns, throughput: " << 1e9 * alloc_nr / alloc_ns << " OPS";
    LOG(INFO) << "[bench-alloc] hugeFree: " << alloc_nr << " times, takes "
              << free_ns << " ns, avg-latency: " << 1.0 * free_ns / alloc_nr
              << " ns, throughput: " << 1e9 * alloc_nr / free_ns << " OPS";
}

void bench_alloc_reg_mr(size_t total_size, size_t alloc_size)
{
    RdmaContext rdma_context;
    CHECK(createContext(&rdma_context));

    std::vector<void *> addrs;
    std::vector<ibv_mr *> mrs;
    size_t alloc_nr = total_size / alloc_size;
    addrs.reserve(alloc_nr + 1);
    mrs.reserve(alloc_nr + 1);

    ChronoTimer timer;
    for (size_t i = 0; i < alloc_nr; ++i)
    {
        addrs.push_back(hugePageAlloc(alloc_size));
        mrs.push_back(CHECK_NOTNULL(createMemoryRegion(
            (uint64_t) addrs.back(), alloc_size, &rdma_context)));
    }
    auto alloc_ns = timer.pin();

    CHECK_EQ(mrs.size(), addrs.size());
    for (size_t i = 0; i < mrs.size(); ++i)
    {
        destroyMemoryRegion(mrs[i]);
        hugePageFree(addrs[i], alloc_size);
    }

    auto free_ns = timer.pin();

    LOG(INFO) << "[alloc-reg-mr] alloc nr: " << alloc_nr;
    LOG(INFO) << "[alloc-reg-mr] hugePageAlloc: " << alloc_nr
              << " times, takes " << alloc_ns
              << " ns, avg-latency: " << 1.0 * alloc_ns / alloc_nr
              << " ns, throughput: " << 1e9 * alloc_nr / alloc_ns << " OPS";
    LOG(INFO) << "[alloc-reg-mr] hugeFree: " << alloc_nr << " times, takes "
              << free_ns << " ns, avg-latency: " << 1.0 * free_ns / alloc_nr
              << " ns, throughput: " << 1e9 * alloc_nr / free_ns << " OPS";
}

void bench_reg_mr(size_t total_size, size_t alloc_size)
{
    RdmaContext rdma_context;
    CHECK(createContext(&rdma_context));

    std::vector<void *> addrs;
    std::vector<ibv_mr *> mrs;
    size_t alloc_nr = total_size / alloc_size;
    addrs.reserve(alloc_nr + 1);
    mrs.reserve(alloc_nr + 1);

    auto *addr = hugePageAlloc(total_size);

    ChronoTimer timer;
    for (size_t i = 0; i < alloc_nr; ++i)
    {
        mrs.push_back(CHECK_NOTNULL(createMemoryRegion(
            (uint64_t) addr + i * alloc_size, alloc_size, &rdma_context)));
    }
    auto alloc_ns = timer.pin();

    for (size_t i = 0; i < mrs.size(); ++i)
    {
        destroyMemoryRegion(mrs[i]);
    }
    auto free_ns = timer.pin();

    hugePageFree(addr, total_size);

    LOG(INFO) << "[reg-mr] alloc nr: " << alloc_nr;
    LOG(INFO) << "[reg-mr] hugePageAlloc: " << alloc_nr << " times, takes "
              << alloc_ns << " ns, avg-latency: " << 1.0 * alloc_ns / alloc_nr
              << " ns, throughput: " << 1e9 * alloc_nr / alloc_ns << " OPS";
    LOG(INFO) << "[reg-mr] hugeFree: " << alloc_nr << " times, takes "
              << free_ns << " ns, avg-latency: " << 1.0 * free_ns / alloc_nr
              << " ns, throughput: " << 1e9 * alloc_nr / free_ns << " OPS";
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    bench_alloc(4_GB, 2_MB);
    bench_alloc_reg_mr(4_GB, 2_MB);
    bench_reg_mr(4_GB, 2_MB);
    bench_reg_mr(1_GB, 4_KB);
}
