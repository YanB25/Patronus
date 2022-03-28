#include <algorithm>
#include <chrono>
#include <random>
#include <thread>

#include "Timer.h"
#include "patronus/Patronus.h"
#include "util/Rand.h"
#include "util/monitor.h"

using namespace std::chrono_literals;

DEFINE_string(exec_meta, "", "The meta data of this execution");

// Two nodes
// one node issues cas operations

constexpr uint16_t kClientNodeId = 0;
[[maybe_unused]] constexpr uint16_t kServerNodeId = 1;
constexpr uint32_t kMachineNr = 2;
constexpr static size_t kAllocBufferSize = 64;

using namespace patronus;
constexpr static size_t kCoroCnt = 8;
thread_local CoroCall workers[kCoroCnt];
thread_local CoroCall master;

constexpr static size_t kDirID = 0;

// constexpr static size_t kTestTime =
//     Patronus::kMwPoolSizePerThread / kCoroCnt / NR_DIRECTORY;
constexpr static size_t kTestTime = 100_K;
// constexpr static size_t kTestTime = 1000;

using namespace std::chrono_literals;

struct ClientCommunication
{
    bool finish_all_task[kCoroCnt];
} client_comm;

void client_worker(Patronus::pointer p, coro_t coro_id, CoroYield &yield)
{
    client_comm.finish_all_task[coro_id] = false;

    auto tid = p->get_thread_id();

    CoroContext ctx(tid, &yield, &master, coro_id);

    std::vector<GlobalAddress> allocated_gaddrs;

    for (size_t time = 0; time < kTestTime; ++time)
    {
        DVLOG(2) << "[bench] client coro " << ctx << " start to got lease ";

        auto lease = p->alloc(
            kServerNodeId, kDirID, kAllocBufferSize, 0 /* hint */, &ctx);
        CHECK(lease.success())
            << "** got lease failed. lease: " << lease << ", " << ctx
            << ". Fail reason: " << lease.ec() << " at " << time
            << " -th for coro: " << (int) coro_id;

        DVLOG(2) << "[bench] client coro " << ctx << " got lease " << lease;

        auto rdma_buf = p->get_rdma_buffer(kAllocBufferSize);
        memset(rdma_buf.buffer, 0, kAllocBufferSize);

        auto gaddr = p->get_gaddr(lease);
        CHECK(!gaddr.is_null());
        allocated_gaddrs.push_back(gaddr);

        p->put_rdma_buffer(rdma_buf);
    }

    auto rdma_buf = p->get_rdma_buffer(kAllocBufferSize);
    CHECK_GE(rdma_buf.size, kAllocBufferSize);
    for (size_t time = 0; time < kTestTime; ++time)
    {
        auto addr_idx = fast_pseudo_rand_int(0, allocated_gaddrs.size() - 1);
        auto gaddr = allocated_gaddrs[addr_idx];

        auto ac_flag = (uint8_t) AcquireRequestFlag::kNoGc;
        auto lease =
            p->get_wlease(gaddr, kDirID, kAllocBufferSize, 0ns, ac_flag, &ctx);
        if (!lease.success())
        {
            CHECK_EQ(lease.ec(), AcquireRequestStatus::kMagicMwErr)
                << "** lease failed. Unexpected failure: " << lease.ec()
                << ". Lease: " << lease;
            continue;
        }

        CHECK_GE(rdma_buf.size, kAllocBufferSize);
        memset(CHECK_NOTNULL(rdma_buf.buffer), 0, kAllocBufferSize);
        auto w_flag = (uint8_t) RWFlag::kNoLocalExpireCheck;
        auto ec =
            p->write(lease, rdma_buf.buffer, kAllocBufferSize, 0, w_flag, &ctx);
        CHECK_EQ(ec, kOk);

        auto rel_flag = (uint8_t) 0;
        p->relinquish(lease, 0 /* hint */, rel_flag, &ctx);
    }
    p->put_rdma_buffer(rdma_buf);

    for (auto gaddr : allocated_gaddrs)
    {
        CHECK(!gaddr.is_null());
        p->dealloc(
            gaddr, kDirID, kAllocBufferSize /* size */, 0 /* hint */, &ctx);
    }

    LOG(WARNING) << "worker coro " << (int) coro_id
                 << " finished ALL THE TASK. yield to master.";

    client_comm.finish_all_task[coro_id] = true;
    ctx.yield_to_master();
}
void client_master(Patronus::pointer p, CoroYield &yield)
{
    auto tid = p->get_thread_id();
    auto mid = tid;

    CoroContext mctx(tid, &yield, workers);
    CHECK(mctx.is_master());

    for (size_t i = 0; i < kCoroCnt; ++i)
    {
        mctx.yield_to_worker(i);
    }
    LOG(INFO) << "Return back to master. start to recv messages";
    coro_t coro_buf[2 * kCoroCnt];
    while (!std::all_of(std::begin(client_comm.finish_all_task),
                        std::end(client_comm.finish_all_task),
                        [](bool i) { return i; }))
    {
        // try to see if messages arrived

        auto nr = p->try_get_client_continue_coros(mid, coro_buf, 2 * kCoroCnt);
        for (size_t i = 0; i < nr; ++i)
        {
            auto coro_id = coro_buf[i];
            DVLOG(1) << "[bench] yielding due to CQE";
            mctx.yield_to_worker(coro_id);
        }
    }

    p->finished();
    LOG(WARNING) << "[bench] all worker finish their work. exiting...";
}

void client(Patronus::pointer p)
{
    auto tid = p->get_thread_id();
    LOG(INFO) << "I am client. tid " << tid;
    for (size_t i = 0; i < kCoroCnt; ++i)
    {
        workers[i] =
            CoroCall([p, i](CoroYield &yield) { client_worker(p, i, yield); });
    }
    master = CoroCall([p](CoroYield &yield) { client_master(p, yield); });
    master();
}

void server(Patronus::pointer p)
{
    auto tid = p->get_thread_id();

    LOG(INFO) << "I am server. tid " << tid;

    p->server_serve(tid);
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    bindCore(0);

    rdmaQueryDevice();

    PatronusConfig config;
    config.machine_nr = kMachineNr;
    config.block_class = {kAllocBufferSize};
    config.block_ratio = {1.0};

    auto patronus = Patronus::ins(config);

    sleep(1);

    // let client spining
    auto nid = patronus->get_node_id();
    if (nid == kClientNodeId)
    {
        patronus->registerClientThread();
        sleep(1);
        client(patronus);
    }
    else
    {
        patronus->registerServerThread();
        patronus->finished();
        server(patronus);
    }

    LOG(INFO) << "finished. ctrl+C to quit.";
}