#include <algorithm>
#include <chrono>
#include <random>
#include <thread>

#include "Timer.h"
#include "gflags/gflags.h"
#include "patronus/Patronus.h"
#include "patronus/memory/patronus_wrapper_allocator.h"
#include "util/Rand.h"
#include "util/monitor.h"

using namespace std::chrono_literals;

DEFINE_string(exec_meta, "", "The meta data of this execution");

constexpr static size_t kAllocBufferSize = 64;

using namespace patronus;
constexpr static size_t kCoroCnt = 8;
thread_local CoroCall workers[kCoroCnt];
thread_local CoroCall master;

constexpr static uint64_t kWaitKey = 0;

// for the specific experiments for refill
constexpr static size_t kReserveBuffersize = 1_GB;
constexpr static size_t kRefillBlockSize = 4_MB;
constexpr static size_t kAllocSize = 1_MB;

// constexpr static size_t kTestTime =
//     Patronus::kMwPoolSizePerThread / kCoroCnt / NR_DIRECTORY;
// constexpr static size_t kTestTime = 100_K;
constexpr static size_t kTestTime = 1_K;

constexpr static uint64_t kSpecificHint = 3;

using namespace std::chrono_literals;

void client_worker(Patronus::pointer p,
                   coro_t coro_id,
                   CoroExecutionContextWith<kCoroCnt, uint64_t> &ex,
                   CoroYield &yield)
{
    auto server_nid = ::config::get_server_nids().front();
    auto tid = p->get_thread_id();
    auto dir_id = tid % NR_DIRECTORY;

    CoroContext ctx(tid, &yield, &master, coro_id);

    LOG(INFO) << "[bench] client " << ctx << " started to bench.";

    std::vector<GlobalAddress> allocated_gaddrs;

    for (size_t time = 0; time < kTestTime; ++time)
    {
        DVLOG(2) << "[bench] client coro " << ctx << " start to alloc gaddr ";

        auto gaddr =
            p->alloc(server_nid, dir_id, kAllocBufferSize, 0 /* hint */, &ctx);
        CHECK(!gaddr.is_null());

        DVLOG(2) << "[bench] client coro " << ctx << " got gaddr " << gaddr;

        auto rdma_buf = p->get_rdma_buffer(kAllocBufferSize);
        memset(rdma_buf.buffer, 0, kAllocBufferSize);

        allocated_gaddrs.push_back(gaddr);

        p->put_rdma_buffer(std::move(rdma_buf));
    }

    auto rdma_buf = p->get_rdma_buffer(kAllocBufferSize);
    CHECK_GE(rdma_buf.size, kAllocBufferSize);
    for (size_t time = 0; time < kTestTime; ++time)
    {
        auto addr_idx = fast_pseudo_rand_int(0, allocated_gaddrs.size() - 1);
        auto gaddr = allocated_gaddrs[addr_idx];

        auto ac_flag = (flag_t) AcquireRequestFlag::kNoGc;
        auto lease = p->get_wlease(server_nid,
                                   dir_id,
                                   gaddr,
                                   0 /* alloc_hint */,
                                   kAllocBufferSize,
                                   0ns,
                                   ac_flag,
                                   &ctx);
        if (!lease.success())
        {
            CHECK_EQ(lease.ec(), AcquireRequestStatus::kMagicMwErr)
                << "** lease failed. Unexpected failure: " << lease.ec()
                << ". Lease: " << lease << ". at " << time << "-th";
            continue;
        }

        CHECK_GE(rdma_buf.size, kAllocBufferSize);
        memset(CHECK_NOTNULL(rdma_buf.buffer), 0, kAllocBufferSize);
        auto w_flag = (flag_t) RWFlag::kNoLocalExpireCheck;
        auto ec =
            p->write(lease, rdma_buf.buffer, kAllocBufferSize, 0, w_flag, &ctx);
        CHECK_EQ(ec, kOk) << "Failed to write. lease: " << lease << " at test "
                          << time << "-th";

        auto rel_flag = (flag_t) 0;
        p->relinquish(lease, 0 /* hint */, rel_flag, &ctx);
    }
    p->put_rdma_buffer(std::move(rdma_buf));

    for (auto gaddr : allocated_gaddrs)
    {
        CHECK(!gaddr.is_null());
        p->dealloc(
            gaddr, dir_id, kAllocBufferSize /* size */, 0 /* hint */, &ctx);
    }

    // now test the refill allocator
    {
        LOG(INFO) << "[bench] begin to test refillable allocator...";
        mem::RefillableSlabAllocatorConfig refill_slab_conf;
        refill_slab_conf.block_class = {kAllocSize};
        refill_slab_conf.block_ratio = {1.0};
        refill_slab_conf.refill_allocator =
            mem::PatronusWrapperAllocator::new_instance(
                p, server_nid, dir_id, kSpecificHint);
        refill_slab_conf.refill_block_size = kRefillBlockSize;
        auto refill_allocator =
            mem::RefillableSlabAllocator::new_instance(refill_slab_conf);
        size_t allocated_nr = 0;
        std::unordered_set<void *> allocated_buffers;
        while (true)
        {
            auto *ret = refill_allocator->alloc(kAllocSize, &ctx);
            if (ret != nullptr)
            {
                allocated_nr++;
                allocated_buffers.insert(ret);
                LOG_IF(INFO, allocated_nr % 1_K == 0)
                    << "[bench] allocated 1_K. now: " << allocated_nr
                    << ". coro: " << ctx;
            }
            else
            {
                LOG(INFO) << "[bench] refill allocator allocates "
                          << allocated_nr << " buffers";
                break;
            }
        }
        LOG(INFO) << "[bench] finish allocation. freeing... coro: " << ctx;
        ex.get_private_data() += allocated_nr * kAllocSize;
        LOG(WARNING) << "[bench] not freeing slab allocator. Strict mode does"
                        "not allowing we to do this.";
        // for (auto *buf : allocated_buffers)
        // {
        //     refill_allocator->free(buf, kAllocSize, &ctx);
        // }
        allocated_buffers.clear();
    }

    LOG(WARNING) << "worker coro " << (int) coro_id
                 << " finished ALL THE TASK. yield to master.";
    ex.worker_finished(coro_id);
    ctx.yield_to_master();
}
void client_master(Patronus::pointer p,
                   CoroExecutionContextWith<kCoroCnt, uint64_t> &ex,
                   CoroYield &yield)
{
    auto tid = p->get_thread_id();

    CoroContext mctx(tid, &yield, workers);
    CHECK(mctx.is_master());

    for (size_t i = 0; i < kCoroCnt; ++i)
    {
        mctx.yield_to_worker(i);
    }
    LOG(INFO) << "Return back to master. start to recv messages";
    coro_t coro_buf[2 * kCoroCnt];
    while (!ex.is_finished_all())
    {
        // try to see if messages arrived
        auto nr = p->try_get_client_continue_coros(coro_buf, 2 * kCoroCnt);
        for (size_t i = 0; i < nr; ++i)
        {
            auto coro_id = coro_buf[i];
            DVLOG(1) << "[bench] yielding due to CQE";
            mctx.yield_to_worker(coro_id);
        }
    }
    auto specific_test_allocated_bytes = ex.get_private_data();
    LOG(INFO) << "[bench] refillable allocator test finished. allocated bytes: "
              << specific_test_allocated_bytes;
}

void client(Patronus::pointer p)
{
    auto tid = p->get_thread_id();
    LOG(INFO) << "I am client. tid " << tid;

    CoroExecutionContextWith<kCoroCnt, uint64_t> ex;
    ex.get_private_data() = 0;

    for (size_t i = 0; i < kCoroCnt; ++i)
    {
        workers[i] = CoroCall(
            [p, i, &ex](CoroYield &yield) { client_worker(p, i, ex, yield); });
    }
    master =
        CoroCall([p, &ex](CoroYield &yield) { client_master(p, ex, yield); });
    master();
}

void server(Patronus::pointer p)
{
    auto tid = p->get_thread_id();

    LOG(INFO) << "I am server. tid " << tid;

    // This is a specific experiments to test the correctness of
    // refill_allocator.
    // Init at the server side.
    {
        auto reserved_buffer = p->get_user_reserved_buffer();
        CHECK_GE(reserved_buffer.size, kReserveBuffersize);

        mem::SlabAllocatorConfig slab_conf;
        slab_conf.block_class = {kRefillBlockSize};
        slab_conf.block_ratio = {1.0};
        auto slab_allocator = mem::SlabAllocator::new_instance(
            reserved_buffer.buffer, reserved_buffer.size, slab_conf);
        p->reg_allocator(kSpecificHint, slab_allocator);
    }

    p->server_serve(kWaitKey);
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    bindCore(0);

    rdmaQueryDevice();

    PatronusConfig config;
    config.machine_nr = ::config::kMachineNr;
    config.block_class = {kAllocBufferSize};
    config.block_ratio = {1.0};

    LOG(INFO) << "[time] before patronus instance";
    auto patronus = Patronus::ins(config);
    LOG(INFO) << "[time] after patronus instance";

    // let client spining
    auto nid = patronus->get_node_id();
    if (::config::is_client(nid))
    {
        patronus->registerClientThread();
        patronus->keeper_barrier("begin", 100ms);
        client(patronus);
        patronus->finished(kWaitKey);
        LOG(WARNING) << "[bench] all worker finish their work. exiting...";
    }
    else
    {
        patronus->registerServerThread();
        patronus->finished(kWaitKey);
        patronus->keeper_barrier("begin", 100ms);
        server(patronus);
    }

    LOG(INFO) << "finished. ctrl+C to quit.";
}