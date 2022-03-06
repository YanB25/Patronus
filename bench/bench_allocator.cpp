#include <algorithm>
#include <iostream>
#include <queue>
#include <set>

#include "Common.h"
#include "PerThread.h"
#include "Rdma.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "patronus/memory/allocator.h"
#include "patronus/memory/direct_allocator.h"
#include "patronus/memory/mr_allocator.h"
#include "patronus/memory/mw_allocator.h"
#include "patronus/memory/ngx_allocator.h"
#include "patronus/memory/nothing_allocator.h"
#include "util/DataFrameF.h"
#include "util/Rand.h"

using namespace define::literals;
using namespace patronus;

constexpr uint32_t kMachineNr = 2;
constexpr static size_t kMwPoolTotalSize = 8192;

std::vector<std::string> col_idx;
std::vector<size_t> col_x_alloc_size;
std::vector<size_t> col_x_thread_nr;
std::vector<size_t> col_alloc_nr;
std::vector<size_t> col_alloc_ns;

using namespace hmdf;

DEFINE_string(exec_meta, "", "The meta data of this execution");

void bench_alloc_thread(size_t tid,
                        size_t alloc_size,
                        size_t alloc_limit_thread,
                        std::shared_ptr<mem::IAllocator> allocator,
                        size_t test_times,
                        std::atomic<ssize_t> &work_nr)
{
    bindCore(tid + 1);

    std::queue<void *> addrs;
    ssize_t remain = work_nr.load(std::memory_order_relaxed);

    ssize_t task_per_sync = test_times / 100;
    task_per_sync = std::max(task_per_sync, ssize_t(1));  // at least 1

    while (remain > 0)
    {
        size_t task_nr = std::min(remain, task_per_sync);
        for (size_t i = 0; i < task_nr; ++i)
        {
            int op = 0;
            if (addrs.size() + 1 > alloc_limit_thread)
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
                auto *addr = addrs.front();
                DVLOG(1) << "free " << (void *) addr << " from tid " << tid;
                allocator->free(addr, alloc_size);
                addrs.pop();
            }
            else
            {
                auto *addr = CHECK_NOTNULL(allocator->alloc(alloc_size));
                DVLOG(1) << "allocating " << (void *) addr << " from tid "
                         << tid;
                addrs.push(addr);
            }
        }
        remain = work_nr.fetch_sub(task_nr) - task_nr;
    }
    while (!addrs.empty())
    {
        allocator->free(addrs.front(), alloc_size);
        addrs.pop();
    }
}

void bench_alloc_mw_thread(size_t tid,
                           size_t alloc_size,
                           size_t alloc_limit_thread,
                           std::shared_ptr<mem::IAllocator> allocator,
                           std::shared_ptr<mem::MWPool> mw_pool,
                           std::atomic<ssize_t> &work_nr,
                           mem::MWAllocatorConfig conf)
{
    constexpr static size_t kTaskBatchSize = 50;
    constexpr static size_t kCqPollBatch = 5;
    CHECK_EQ(kTaskBatchSize % kCqPollBatch, 0);

    std::queue<void *> addrs;
    std::map<void *, ibv_mw *> addr_to_mw;
    ssize_t remain = work_nr.load(std::memory_order_relaxed);
    bindCore(tid + 1);

    // these are never change, so place them to the very begining
    auto dir_id = conf.dir_id;
    ibv_qp *qp = conf.dsm->get_dir_qp(conf.node_id, conf.thread_id, dir_id);
    auto wr_id = WRID(WRID_PREFIX_BENCHMARK_ONLY, 0);
    auto *dsm_mr = conf.dsm->get_dir_mr(dir_id);

    while (remain > 0)
    {
        size_t task_nr = std::min(remain, (ssize_t) kTaskBatchSize);
        size_t batch_nr = task_nr / kCqPollBatch;
        for (size_t batch_id = 0; batch_id < batch_nr; ++batch_id)
        {
            int op = 0;
            if (addrs.size() + kCqPollBatch > alloc_limit_thread)
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
                // free
                for (size_t i = 0; i < kCqPollBatch; ++i)
                {
                    CHECK(!addrs.empty());
                    auto *addr = CHECK_NOTNULL(addrs.front());
                    addrs.pop();

                    auto it = addr_to_mw.find(addr);
                    if (it == addr_to_mw.end())
                    {
                        LOG(FATAL) << "[mw-alloc] failed to free addr " << addr
                                   << ", not allocated by me.";
                    }
                    // bind_mw in unbinding process
                    auto *mw = it->second;

                    bool signal = (i + 1) == kCqPollBatch;
                    uint32_t rkey = rdmaAsyncBindMemoryWindow(
                        qp, mw, dsm_mr, (uint64_t) addr, 1, signal, wr_id.val);
                    CHECK_NE(rkey, 0);

                    // bind_mw_end
                    addr_to_mw.erase(addr);
                    mw_pool->free(mw);

                    allocator->free(addr, alloc_size);
                }
                struct ibv_wc wc;
                int ret = pollWithCQ(conf.dsm->get_dir_cq(dir_id), 1, &wc);
                CHECK_GE(ret, 0);
            }
            else
            {
                // alloc
                for (size_t i = 0; i < kCqPollBatch; ++i)
                {
                    auto *addr = CHECK_NOTNULL(allocator->alloc(alloc_size));

                    // bind mw
                    auto *mw = CHECK_NOTNULL(mw_pool->alloc());
                    bool signal = i == (kCqPollBatch - 1);
                    uint32_t rkey = rdmaAsyncBindMemoryWindow(qp,
                                                              mw,
                                                              dsm_mr,
                                                              (uint64_t) addr,
                                                              alloc_size,
                                                              signal,
                                                              wr_id.val);
                    CHECK_NE(rkey, 0);

                    addr_to_mw[addr] = mw;
                    // end bind mw

                    addrs.push(addr);
                }

                struct ibv_wc wc;
                int ret = pollWithCQ(conf.dsm->get_dir_cq(dir_id), 1, &wc);
                CHECK_GE(ret, 0);
            }
        }

        work_nr.fetch_sub(task_nr);
        remain = work_nr.load(std::memory_order_relaxed);
    }

    while (!addrs.empty())
    {
        for (size_t i = 0; i < kCqPollBatch; ++i)
        {
            auto *addr = CHECK_NOTNULL(addrs.front());
            addrs.pop();

            auto it = addr_to_mw.find(addr);
            if (it == addr_to_mw.end())
            {
                LOG(FATAL) << "[mw-alloc] failed to free addr " << addr
                           << ", not allocated by me.";
            }
            // bind_mw in unbinding process
            auto *mw = it->second;

            bool signal = i == (kCqPollBatch - 1);
            uint32_t rkey = rdmaAsyncBindMemoryWindow(
                qp, mw, dsm_mr, (uint64_t) addr, 1, signal, wr_id.val);
            CHECK_NE(rkey, 0);

            // bind_mw_end
            addr_to_mw.erase(it);
            mw_pool->free(mw);

            allocator->free(addr, alloc_size);
        }
        struct ibv_wc wc;
        int ret = pollWithCQ(conf.dsm->get_dir_cq(dir_id), 1, &wc);
        CHECK_GE(ret, 0);
    }
}

void bench_template(const std::string &bench_name,
                    size_t test_times,
                    size_t alloc_size,
                    size_t alloc_limit_thread,
                    size_t thread_nr,
                    std::vector<std::shared_ptr<mem::IAllocator>> allocators,
                    bool report)
{
    CHECK_EQ(allocators.size(), thread_nr);
    std::vector<std::thread> threads;

    std::atomic<ssize_t> work_nr{ssize_t(test_times)};

    ChronoTimer timer;
    for (size_t i = 0; i < thread_nr; ++i)
    {
        threads.emplace_back([i,
                              alloc_size,
                              alloc_limit_thread,
                              allocators,
                              test_times,
                              &work_nr]() {
            bench_alloc_thread(i,
                               alloc_size,
                               alloc_limit_thread,
                               allocators[i],
                               test_times,
                               work_nr);
        });
    }
    for (auto &t : threads)
    {
        t.join();
    }
    auto total_ns = timer.pin();

    if (report)
    {
        col_idx.push_back(bench_name);
        col_x_alloc_size.push_back(alloc_size);
        col_x_thread_nr.push_back(thread_nr);
        col_alloc_nr.push_back(test_times);
        col_alloc_ns.push_back(total_ns);
    }
}
void bench_alloc(size_t test_times,
                 size_t alloc_size,
                 size_t memory_limit,
                 size_t thread_nr,
                 bool report)
{
    std::vector<std::shared_ptr<mem::IAllocator>> allocators;
    for (size_t i = 0; i < thread_nr; ++i)
    {
        allocators.push_back(std::make_shared<mem::RawAllocator>());
    }
    size_t alloc_limit_total = memory_limit / alloc_size;
    size_t alloc_limit_thread = alloc_limit_total / thread_nr;
    return bench_template("alloc (syscall)",
                          test_times,
                          alloc_size,
                          alloc_limit_thread,
                          thread_nr,
                          allocators,
                          report);
}

void bench_ngx_alloc(size_t test_times,
                     size_t alloc_size,
                     size_t memory_limit,
                     size_t thread_nr,
                     bool report)
{
    CHECK(false) << "bench_ngx_alloc is unable to run, because ngxin's memory "
                    "pool does not NGX_DECLINED free-ing small objects";
    std::vector<std::shared_ptr<mem::IAllocator>> allocators;
    for (size_t i = 0; i < thread_nr; ++i)
    {
        auto raw_allocator = std::make_shared<mem::RawAllocator>();
        allocators.push_back(
            std::make_shared<mem::NginxAllocator>(1_GB, raw_allocator));
    }
    size_t alloc_limit_total = memory_limit / alloc_size;
    size_t alloc_limit_thread = alloc_limit_total / thread_nr;

    return bench_template("alloc (nginx)",
                          test_times,
                          alloc_size,
                          alloc_limit_thread,
                          thread_nr,
                          allocators,
                          report);
}

void bench_nothing_alloc(size_t test_times,
                         size_t alloc_size,
                         size_t memory_limit,
                         size_t thread_nr,
                         bool report)
{
    size_t alloc_limit_total = memory_limit / alloc_size;
    size_t alloc_limit_thread = alloc_limit_total / thread_nr;

    std::vector<std::shared_ptr<mem::IAllocator>> allocators;
    for (size_t i = 0; i < thread_nr; ++i)
    {
        allocators.push_back(std::make_shared<mem::NothingAllocator>());
    }
    bench_template("alloc (nothing)",
                   test_times,
                   alloc_size,
                   alloc_limit_thread,
                   thread_nr,
                   allocators,
                   report);
}

void bench_slab_alloc(size_t test_times,
                      size_t alloc_size,
                      size_t memory_limit,
                      size_t thread_nr,
                      bool report)
{
    void *global_addr = hugePageAlloc(memory_limit);
    size_t memory_limit_thread = memory_limit / thread_nr;
    size_t alloc_limit_total = memory_limit / alloc_size;
    size_t alloc_limit_thread = alloc_limit_total / thread_nr;

    std::vector<std::shared_ptr<mem::IAllocator>> allocators;
    for (size_t i = 0; i < thread_nr; ++i)
    {
        void *start_addr_thread =
            (char *) global_addr + i * memory_limit_thread;
        mem::SlabAllocatorConfig conf;
        conf.block_class = {alloc_size};
        conf.block_ratio = {1};
        allocators.push_back(std::make_shared<mem::SlabAllocator>(
            start_addr_thread, memory_limit_thread, conf));
    }
    bench_template("alloc (slab)",
                   test_times,
                   alloc_size,
                   alloc_limit_thread,
                   thread_nr,
                   allocators,
                   report);

    CHECK(hugePageFree(global_addr, memory_limit));
}

void bench_slab_alloc_reg_mr(size_t test_times,
                             size_t alloc_size,
                             size_t memory_limit,
                             size_t thread_nr,
                             bool report)
{
    void *global_addr = hugePageAlloc(memory_limit);
    size_t memory_limit_thread = memory_limit / thread_nr;
    size_t alloc_limit_total = memory_limit / alloc_size;
    size_t alloc_limit_thread = alloc_limit_total / thread_nr;

    std::vector<std::shared_ptr<mem::IAllocator>> allocators;
    std::vector<RdmaContext> rdma_contexts;
    rdma_contexts.resize(thread_nr);

    for (size_t i = 0; i < thread_nr; ++i)
    {
        mem::SlabAllocatorConfig slab_conf;
        slab_conf.block_class = {alloc_size};
        slab_conf.block_ratio = {1};
        void *start_addr_thread =
            (char *) global_addr + memory_limit_thread * i;
        auto slab_allocator = std::make_shared<mem::SlabAllocator>(
            start_addr_thread, memory_limit_thread, slab_conf);

        CHECK(createContext(&rdma_contexts[i]));
        mem::MRAllocatorConfig mr_conf;
        mr_conf.rdma_context = &rdma_contexts[i];

        mr_conf.allocator = slab_allocator;
        allocators.push_back(std::make_shared<mem::MRAllocator>(mr_conf));
    }
    bench_template("alloc (slab) + MR",
                   test_times,
                   alloc_size,
                   alloc_limit_thread,
                   thread_nr,
                   allocators,
                   report);

    for (size_t i = 0; i < thread_nr; ++i)
    {
        CHECK(destroyContext(&rdma_contexts[i]));
    }

    CHECK(hugePageFree(global_addr, memory_limit));
}

void bench_alloc_reg_mr(size_t test_times,
                        size_t alloc_size,
                        size_t memory_limit,
                        size_t thread_nr,
                        bool report)
{
    std::vector<std::shared_ptr<mem::IAllocator>> allocators;
    std::vector<RdmaContext> rdma_contexts;
    rdma_contexts.resize(thread_nr);

    size_t alloc_limit_total = memory_limit / alloc_size;
    size_t alloc_limit_thread = alloc_limit_total / thread_nr;

    for (size_t i = 0; i < thread_nr; ++i)
    {
        CHECK(createContext(&rdma_contexts[i]));
        mem::MRAllocatorConfig conf;
        conf.rdma_context = &rdma_contexts[i];
        conf.allocator = std::make_shared<mem::RawAllocator>();
        allocators.push_back(std::make_shared<mem::MRAllocator>(conf));
    }
    bench_template("alloc (syscall) + MR",
                   test_times,
                   alloc_size,
                   alloc_limit_thread,
                   thread_nr,
                   allocators,
                   report);

    for (size_t i = 0; i < thread_nr; ++i)
    {
        CHECK(destroyContext(&rdma_contexts[i]));
    }
}

void bench_slab_alloc_reg_mw(size_t test_times,
                             size_t alloc_size,
                             size_t memory_limit,
                             size_t thread_nr,
                             DSM::pointer dsm,
                             bool report)
{
    void *global_addr = hugePageAlloc(memory_limit);
    size_t mw_pool_size_thread = kMwPoolTotalSize / thread_nr;
    size_t alloc_limit_total = memory_limit / alloc_size;
    size_t alloc_limit_thread = alloc_limit_total / thread_nr;
    alloc_limit_thread = std::min(alloc_limit_thread, mw_pool_size_thread);
    size_t memory_limit_thread = memory_limit / thread_nr;

    std::vector<std::shared_ptr<mem::IAllocator>> allocators;
    std::vector<RdmaContext> rdma_contexts;
    rdma_contexts.resize(thread_nr);

    for (size_t i = 0; i < thread_nr; ++i)
    {
        void *start_addr = (char *) global_addr + memory_limit_thread * i;
        mem::SlabAllocatorConfig slab_conf;
        slab_conf.block_class = {alloc_size};
        slab_conf.block_ratio = {1};

        CHECK(createContext(&rdma_contexts[i]));
        mem::MWAllocatorConfig conf;
        conf.allocator = std::make_shared<mem::SlabAllocator>(
            start_addr, memory_limit_thread, slab_conf);
        conf.dir_id = i % NR_DIRECTORY;
        conf.dsm = dsm;
        conf.node_id = 0;
        conf.thread_id = i;
        conf.mw_pool = std::make_shared<mem::MWPool>(
            dsm, conf.dir_id, mw_pool_size_thread);
        allocators.push_back(std::make_shared<mem::MWAllocator>(conf));
    }
    bench_template("alloc (slab) + MW (allocator without MR)",
                   test_times,
                   alloc_size,
                   alloc_limit_thread,
                   thread_nr,
                   allocators,
                   report);
    for (size_t i = 0; i < thread_nr; ++i)
    {
        CHECK(destroyContext(&rdma_contexts[i]));
    }

    CHECK(hugePageFree(global_addr, memory_limit));
}

void bench_alloc_reg_mw_with_mw_allocator(size_t test_times,
                                          size_t alloc_size,
                                          size_t memory_limit,
                                          size_t thread_nr,
                                          DSM::pointer dsm,
                                          bool report)
{
    std::vector<std::shared_ptr<mem::IAllocator>> allocators;
    std::vector<RdmaContext> rdma_contexts;
    rdma_contexts.resize(thread_nr);

    size_t mw_pool_thread = kMwPoolTotalSize / thread_nr;

    for (size_t i = 0; i < thread_nr; ++i)
    {
        CHECK(createContext(&rdma_contexts[i]));
        mem::MWAllocatorConfig conf;
        conf.allocator = std::make_shared<mem::RawAllocator>();
        conf.dir_id = i % NR_DIRECTORY;
        conf.dsm = dsm;
        conf.node_id = 0;
        conf.thread_id = i;
        conf.mw_pool =
            std::make_shared<mem::MWPool>(dsm, conf.dir_id, mw_pool_thread);
        allocators.push_back(std::make_shared<mem::MWAllocator>(conf));
    }

    size_t alloc_limit_total = memory_limit / alloc_size;
    size_t alloc_limit_thread = alloc_limit_total / thread_nr;
    alloc_limit_thread = std::min(alloc_limit_thread, mw_pool_thread);

    bench_template("alloc (syscall) + MW (allocator without MR)",
                   test_times,
                   alloc_size,
                   alloc_limit_thread,
                   thread_nr,
                   allocators,
                   report);
    for (size_t i = 0; i < thread_nr; ++i)
    {
        CHECK(destroyContext(&rdma_contexts[i]));
    }
}
void bench_alloc_reg_mw_signal_batching(size_t test_times,
                                        size_t alloc_size,
                                        size_t memory_limit,
                                        size_t thread_nr,
                                        DSM::pointer dsm,
                                        bool report)
{
    std::vector<std::shared_ptr<mem::IAllocator>> allocators;
    std::vector<std::shared_ptr<mem::MWPool>> mw_pools;

    size_t mw_pool_size_thread = kMwPoolTotalSize / thread_nr;

    for (size_t i = 0; i < thread_nr; ++i)
    {
        auto dir_id = i % NR_DIRECTORY;
        allocators.push_back(std::make_shared<mem::RawAllocator>());
        mw_pools.push_back(
            std::make_shared<mem::MWPool>(dsm, dir_id, mw_pool_size_thread));
    }

    CHECK_EQ(allocators.size(), thread_nr);
    std::vector<std::thread> threads;

    std::atomic<ssize_t> work_nr{ssize_t(test_times)};

    size_t alloc_limit_total = memory_limit / alloc_size;
    size_t alloc_limit_thread = alloc_limit_total / thread_nr;
    alloc_limit_thread = std::min(alloc_limit_thread, mw_pool_size_thread);

    ChronoTimer timer;
    for (size_t i = 0; i < thread_nr; ++i)
    {
        mem::MWAllocatorConfig conf;
        conf.dsm = dsm;
        conf.allocator = nullptr;
        conf.dir_id = i % NR_DIRECTORY;
        conf.node_id = 0;
        conf.thread_id = i;
        threads.emplace_back([i,
                              alloc_size,
                              alloc_limit_thread,
                              &allocator = allocators[i],
                              &mw_pool = mw_pools[i],
                              &work_nr,
                              conf]() {
            bench_alloc_mw_thread(i,
                                  alloc_size,
                                  alloc_limit_thread,
                                  allocator,
                                  mw_pool,
                                  work_nr,
                                  conf);
        });
    }
    for (auto &t : threads)
    {
        t.join();
    }
    auto total_ns = timer.pin();

    if (report)
    {
        col_idx.push_back("alloc (syscall) + MW (batching without MR)");
        col_x_alloc_size.push_back(alloc_size);
        col_x_thread_nr.push_back(thread_nr);
        col_alloc_nr.push_back(test_times);
        col_alloc_ns.push_back(total_ns);
    }
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    DSMConfig config;
    config.machineNR = kMachineNr;
    config.dsmSize = 4_GB;
    config.dsmReserveSize = 0;

    auto dsm = DSM::getInstance(config);
    sleep(1);
    dsm->registerThread();

    bindCore(0);

    LOG(INFO) << "This executable is a comprehensive performance comparison "
                 "between each memory allocator";
    LOG(INFO) << "alloc (@type) + @pem (@detail)";
    LOG(INFO) << "@type syscall: using hugePageAlloc() or malloc()";
    LOG(INFO) << "@type slab: using slab allocator";
    LOG(INFO) << "@type ngx: using Nginx memory pool";
    LOG(INFO) << "@pem MR: registering memory region";
    LOG(INFO) << "@pem MW: registering memory window";
    LOG(INFO)
        << "@detail allocator: using MWAllocator, which does not batch signal";
    LOG(INFO) << "@detail batch: enabling batch signal";
    LOG(INFO)
        << "@detail without MR: the memory window is not deployed over MR";

    // for (size_t thread_nr : {1, 2, 4, 8})
    for (size_t thread_nr : {1, 2, 4, 8, 16})
    {
        // for (size_t block_size : {2_MB, 128_MB})
        for (size_t block_size : {2_MB})
        {
            LOG(INFO) << "thread_nr: " << thread_nr
                      << ", block_size: " << block_size;
            // LOG(INFO) << "[bench] bench_alloc()";
            // bench_alloc(1_M, block_size, 16_GB, thread_nr);

            // LOG(INFO) << "[bench] bench_alloc_reg_mw_signal_batching()";
            // bench_alloc_reg_mw_signal_batching(
            //     1_M / 20, block_size, 16_GB, thread_nr, dsm);

            // LOG(INFO) << "[bench] bench_alloc_reg_mw_with_mw_allocator()";
            // bench_alloc_reg_mw_with_mw_allocator(
            //     1_M, block_size, 16_GB, thread_nr, dsm);

            // LOG(INFO) << "[bench] bench_alloc_reg_mr()";
            // bench_alloc_reg_mr(1_K, block_size, 16_GB, thread_nr);

            // LOG(INFO) << "[bench] skipping bench_ngx_allocator()";
            // // bench_ngx_alloc(1_M, block_size, 16_GB, thread_nr);

            LOG(INFO) << "[bench] bench slab_allocator()";
            bench_slab_alloc(
                10_M * thread_nr, block_size, 16_GB, thread_nr, false);

            bench_slab_alloc(
                10_M * thread_nr, block_size, 16_GB, thread_nr, true);

            LOG(INFO) << "[bench] bench_alloc_slab_reg_mr()";
            bench_slab_alloc_reg_mr(
                1_M / 100, block_size, 16_GB, thread_nr, false);
            bench_slab_alloc_reg_mr(
                1_M / 100, block_size, 16_GB, thread_nr, true);

            LOG(INFO) << "[bench] bench_alloc_slab_reg_mw()";
            bench_slab_alloc_reg_mw(
                1_M, block_size, 16_GB, thread_nr, dsm, false);
            bench_slab_alloc_reg_mw(
                1_M, block_size, 16_GB, thread_nr, dsm, true);

            LOG(INFO) << "[bench] bench nothing_allocator()";
            bench_nothing_alloc(
                1_M * thread_nr, block_size, 16_GB, thread_nr, false);
            bench_nothing_alloc(
                1_M * thread_nr, block_size, 16_GB, thread_nr, true);
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
