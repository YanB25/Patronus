#include <algorithm>
#include <iostream>
#include <queue>
#include <random>
#include <set>

#include "Common.h"
#include "PerThread.h"
#include "Rdma.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "util/DataFrameF.h"
#include "util/Rand.h"

using namespace define::literals;

std::vector<std::string> col_idx;
std::vector<size_t> col_x_alloc_size;
std::vector<size_t> col_x_thread_nr;
std::vector<size_t> col_alloc_nr;
std::vector<size_t> col_alloc_ns;

using namespace hmdf;

DEFINE_string(exec_meta, "", "The meta data of this execution");

void *alloc(size_t size)
{
    if (size >= 2_MB)
    {
        return hugePageAlloc(size);
    }
    else
    {
        return malloc(size);
    }
}
void free(void *addr, size_t size)
{
    if (size >= 2_MB)
    {
        hugePageFree(addr, size);
    }
    else
    {
        free(addr);
    }
}

void bench_alloc(size_t test_times,
                 size_t alloc_size,
                 size_t memory_limit,
                 size_t thread_nr)
{
    std::vector<std::thread> threads;

    std::atomic<ssize_t> work_nr{ssize_t(test_times)};

    ChronoTimer timer;
    size_t alloc_limit = memory_limit / thread_nr / alloc_size;
    for (size_t i = 0; i < thread_nr; ++i)
    {
        threads.emplace_back([alloc_size, alloc_limit, &work_nr, tid = i]() {
            std::queue<void *> addrs;
            ssize_t remain = work_nr.load(std::memory_order_relaxed);
            bindCore(tid + 1);
            while (remain > 0)
            {
                size_t task_nr = std::min(remain, (ssize_t) 50);
                for (size_t i = 0; i < task_nr; ++i)
                {
                    int op = 0;
                    if (addrs.size() >= alloc_limit)
                    {
                        op = 0;
                    }
                    else if (addrs.empty())
                    {
                        op = 1;
                    }
                    else
                    {
                        op = fast_pseudo_rand_int(0, 1);
                    }
                    if (op == 0)
                    {
                        free(addrs.front(), alloc_size);
                        addrs.pop();
                    }
                    else
                    {
                        addrs.push(CHECK_NOTNULL(alloc(alloc_size)));
                    }
                }
                work_nr.fetch_sub(task_nr);
                remain = work_nr.load(std::memory_order_relaxed);
            }
            while (!addrs.empty())
            {
                free(addrs.front(), alloc_size);
                addrs.pop();
            }
        });
    }
    for (auto &t : threads)
    {
        t.join();
    }
    auto total_ns = timer.pin();

    col_idx.push_back("alloc(huge page)");
    col_x_alloc_size.push_back(alloc_size);
    col_x_thread_nr.push_back(thread_nr);
    col_alloc_nr.push_back(test_times);
    col_alloc_ns.push_back(total_ns);
}

void bench_alloc_reg_mr(size_t test_times,
                        size_t alloc_size,
                        size_t memory_limit,
                        size_t thread_nr)
{
    std::vector<RdmaContext> rdma_contexts;
    rdma_contexts.resize(thread_nr);
    for (size_t i = 0; i < thread_nr; ++i)
    {
        CHECK(createContext(&rdma_contexts[i]));
    }

    std::atomic<ssize_t> remain{ssize_t(test_times)};
    size_t alloc_limit = memory_limit / thread_nr / alloc_size;

    std::vector<std::thread> threads;
    ChronoTimer timer;
    for (size_t i = 0; i < thread_nr; ++i)
    {
        threads.emplace_back([tid = i,
                              &remain,
                              alloc_limit,
                              alloc_size,
                              &ctx = rdma_contexts[i]]() {
            bindCore(tid + 1);
            ssize_t r = remain.load(std::memory_order_relaxed);

            std::queue<void *> addrs;
            std::queue<ibv_mr *> mrs;

            while (r > 0)
            {
                auto task_nr = std::min(r, ssize_t(50));
                for (ssize_t i = 0; i < task_nr; ++i)
                {
                    int op = 0;
                    if (addrs.size() >= alloc_limit)
                    {
                        op = 0;
                    }
                    else if (addrs.empty())
                    {
                        op = 1;
                    }
                    else
                    {
                        op = fast_pseudo_rand_int(0, 1);
                    }
                    if (op == 0)
                    {
                        auto *free_addr = addrs.front();
                        auto *free_mr = mrs.front();
                        destroyMemoryRegion(free_mr);
                        free(free_addr, alloc_size);
                        addrs.pop();
                        mrs.pop();
                    }
                    else
                    {
                        auto *alloc_addr = CHECK_NOTNULL(alloc(alloc_size));
                        addrs.push(alloc_addr);
                        mrs.push(CHECK_NOTNULL(createMemoryRegion(
                            (uint64_t) alloc_addr, alloc_size, &ctx)));
                    }
                }

                remain.fetch_sub(task_nr);
                r = remain.load(std::memory_order_relaxed);
            }
            while (!addrs.empty())
            {
                auto *free_addr = addrs.front();
                auto *free_mr = mrs.front();
                destroyMemoryRegion(free_mr);
                free(free_addr, alloc_size);
                addrs.pop();
                mrs.pop();
            }
        });
    }

    for (auto &t : threads)
    {
        t.join();
    }
    auto total_ns = timer.pin();

    col_idx.push_back("alloc(huge page) + register mr");
    col_x_alloc_size.push_back(alloc_size);
    col_x_thread_nr.push_back(thread_nr);
    col_alloc_nr.push_back(test_times);
    col_alloc_ns.push_back(total_ns);
}

void bench_reg_mr(size_t test_times,
                  size_t alloc_size,
                  size_t memory_limit,
                  size_t thread_nr)
{
    if (memory_limit < 2_MB)
    {
        return;
    }
    std::vector<RdmaContext> rdma_contexts;
    rdma_contexts.resize(thread_nr);
    for (size_t i = 0; i < thread_nr; ++i)
    {
        CHECK(createContext(&rdma_contexts[i]));
    }

    std::atomic<ssize_t> remain{ssize_t(test_times)};
    size_t alloc_limit = memory_limit / thread_nr / alloc_size;

    std::vector<std::thread> threads;
    ChronoTimer timer;

    auto *global_addr = CHECK_NOTNULL(alloc(memory_limit));

    for (size_t i = 0; i < thread_nr; ++i)
    {
        threads.emplace_back([tid = i,
                              &remain,
                              alloc_limit,
                              memory_limit,
                              global_addr,
                              alloc_size,
                              &ctx = rdma_contexts[i]]() {
            bindCore(tid + 1);
            ssize_t r = remain.load(std::memory_order_relaxed);

            std::queue<ibv_mr *> mrs;

            size_t block_nr = memory_limit / alloc_size;

            while (r > 0)
            {
                auto task_nr = std::min(r, ssize_t(50));
                for (ssize_t i = 0; i < task_nr; ++i)
                {
                    int op = 0;
                    if (mrs.size() >= alloc_limit)
                    {
                        op = 0;
                    }
                    else if (mrs.empty())
                    {
                        op = 1;
                    }
                    else
                    {
                        op = fast_pseudo_rand_int(0, 1);
                    }
                    if (op == 0)
                    {
                        auto *free_mr = mrs.front();
                        CHECK(destroyMemoryRegion(free_mr));
                        mrs.pop();
                    }
                    else
                    {
                        auto block_id = fast_pseudo_rand_int(0, block_nr - 1);
                        auto *alloc_addr =
                            ((char *) global_addr + block_id * alloc_size);
                        mrs.push(CHECK_NOTNULL(createMemoryRegion(
                            (uint64_t) alloc_addr, alloc_size, &ctx)));
                    }
                }

                remain.fetch_sub(task_nr);
                r = remain.load(std::memory_order_relaxed);
            }
            while (!mrs.empty())
            {
                auto *free_mr = mrs.front();
                CHECK(destroyMemoryRegion(free_mr));
                mrs.pop();
            }
        });
    }

    for (auto &t : threads)
    {
        t.join();
    }
    auto total_ns = timer.pin();

    col_idx.push_back("register mr");
    col_x_alloc_size.push_back(alloc_size);
    col_x_thread_nr.push_back(thread_nr);
    col_alloc_nr.push_back(test_times);
    col_alloc_ns.push_back(total_ns);

    free(global_addr, memory_limit);
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    LOG(INFO)
        << "This benchmark test against three cases: 1) bench_alloc: the "
           "performance of malloc() (for small allocation) and mmap() "
           "(for huge page allocation). 2) reg_mr: the performance of "
           "registering memory region to RNIC, and 3) alloc_reg_mr: the "
           "combination of 1) and 2).\nThe throughput and latency is reported";

    for (size_t thread_nr : {1, 2, 4, 8, 16})
    {
        for (size_t block_size : {2_MB, 256_MB})
        {
            LOG(INFO) << "[bench] benching thread " << thread_nr
                      << ", block_size: " << block_size << " for bench_alloc";
            bench_alloc(1_M, block_size, 4_GB, thread_nr);

            LOG(INFO) << "[bench] benching thread " << thread_nr
                      << ", block_size: " << block_size << " for bench_reg_mr";
            bench_reg_mr(1_M / 20, block_size, 4_GB, thread_nr);

            LOG(INFO) << "[bench] benching thread " << thread_nr
                      << ", block_size: " << block_size
                      << " for bench_alloc_reg_mr";
            bench_alloc_reg_mr(1_M / 20, block_size, 4_GB, thread_nr);
        }
    }

    std::vector<std::string> a = {"1", "2", "3"};
    std::vector<int> b = {12, 14, 156};
    std::vector<int> c = {12312, 543, 9};

    StrDataFrame df;
    df.load_index(std::move(col_idx));
    df.load_column<size_t>("x_alloc_size", std::move(col_x_alloc_size));
    df.load_column<size_t>("x_thread_nr", std::move(col_x_thread_nr));
    df.load_column<size_t>("alloc_nr(total)", std::move(col_alloc_nr));
    df.load_column<size_t>("alloc_ns(total)", std::move(col_alloc_ns));

    auto div_f = gen_F_div<size_t, size_t, double>();
    auto div_f2 = gen_F_div<double, size_t, double>();
    auto ops_f = gen_F_ops<size_t, size_t, double>();
    auto mul_f = gen_F_mul<double, size_t, double>();
    df.consolidate<size_t, size_t, double>(
        "alloc_ns(total)", "alloc_nr(total)", "alloc lat", div_f, false);
    df.consolidate<size_t, size_t, double>(
        "alloc_nr(total)", "alloc_ns(total)", "alloc ops(total)", ops_f, false);
    df.consolidate<double, size_t, double>(
        "alloc ops(total)", "x_thread_nr", "alloc ops(thread)", div_f2, false);

    auto filename = binary_to_csv_filename(argv[0], FLAGS_exec_meta);
    df.write<std::ostream, std::string, size_t, double>(std::cout,
                                                        io_format::csv2);
    df.write<std::string, size_t, double>(filename.c_str(), io_format::csv2);
}
