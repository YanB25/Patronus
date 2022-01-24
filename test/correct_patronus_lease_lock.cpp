#include <algorithm>
#include <chrono>
#include <random>
#include <thread>

#include "Timer.h"
#include "patronus/Patronus.h"
#include "util/PerformanceReporter.h"
#include "util/monitor.h"
using namespace std::chrono_literals;
using namespace define::literals;

// Two nodes
// one node issues cas operations

constexpr uint16_t kClientNodeId = 0;
[[maybe_unused]] constexpr uint16_t kServerNodeId = 1;
constexpr uint32_t kMachineNr = 2;
constexpr static size_t kTestTime = 10_K;

using namespace patronus;
constexpr static size_t kCoroCnt = 1;
thread_local CoroCall workers[kCoroCnt];
thread_local CoroCall master;

constexpr static uint64_t kMagic = 0xaabbccdd11223344;
constexpr static uint64_t kKey = 0;
constexpr static size_t kKeyLimit = 10240;

using namespace std::chrono_literals;

struct Object
{
    uint64_t target;
    uint64_t unused_1;
    uint64_t unused_2;
    uint64_t unused_3;
};

uint64_t bench_locator(key_t key)
{
    return key * sizeof(Object);
}

struct ClientCommunication
{
    bool still_has_work[kCoroCnt];
    bool finish_cur_task[kCoroCnt];
    bool finish_all_task[kCoroCnt];
} client_comm;

void client_worker(Patronus::pointer p, coro_t coro_id, CoroYield &yield)
{
    auto tid = p->get_thread_id();

    CoroContext ctx(tid, &yield, &master, coro_id);

    client_comm.still_has_work[coro_id] = true;
    client_comm.finish_cur_task[coro_id] = false;
    client_comm.finish_all_task[coro_id] = false;

    OnePassMonitor lock_success_m;
    size_t lock_success_nr{0};

    for (size_t i = 0; i < kTestTime; ++i)
    {
        DVLOG(2) << "[bench] client coro " << ctx << " start to got lease ";
        auto key = rand() % kKeyLimit;
        auto [bucket_id, slot_id] = p->locate_key(key);
        auto dir_id = bucket_id;
        DCHECK_LT(dir_id, NR_DIRECTORY);
        Lease lease =
            p->get_rlease(kServerNodeId,
                          dir_id,
                          key,
                          sizeof(Object),
                          0ns,
                          (uint8_t) AcquireRequestFlag::kWithConflictDetect |
                              (uint8_t) AcquireRequestFlag::kNoGc,
                          &ctx);
        if (lease.success())
        {
            lock_success_m.collect(1);
            lock_success_nr++;
        }
        else
        {
            lock_success_m.collect(0);
        }

        DVLOG(2) << "[bench] client coro " << ctx << " got lease " << lease;

        auto rdma_buf = p->get_rdma_buffer();
        memset(rdma_buf.buffer, 0, sizeof(Object));

        DVLOG(2) << "[bench] client coro " << ctx
                 << " start to relinquish lease ";

        // make sure this will take no harm.
        p->relinquish(lease, 0, &ctx);

        p->put_rdma_buffer(rdma_buf.buffer);
    }
    LOG(INFO) << "[bench] lock_success_m: " << lock_success_m << " at ctx "
              << ctx;
    LOG(INFO) << "[bench] lock_success_nr: " << lock_success_nr;

    client_comm.still_has_work[coro_id] = false;
    client_comm.finish_cur_task[coro_id] = true;
    client_comm.finish_all_task[coro_id] = true;

    LOG(WARNING) << "worker coro " << (int) coro_id
                 << " finished ALL THE TASK. yield to master.";

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

        for (size_t i = 0; i < kCoroCnt; ++i)
        {
            if (client_comm.finish_cur_task[i] &&
                !client_comm.finish_all_task[i])
            {
                DVLOG(1) << "[bench] yielding to coro " << (int) i
                         << " for new task";
                mctx.yield_to_worker(i);
            }
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
    auto internal_buffer = p->get_server_internal_buffer();
    auto *buffer = internal_buffer.buffer;
    auto offset = bench_locator(kKey);
    auto &object = *(Object *) &buffer[offset];
    object.target = kMagic;

    p->server_serve();
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    bindCore(0);

    rdmaQueryDevice();

    PatronusConfig config;
    config.machine_nr = kMachineNr;
    config.key_locator = bench_locator;

    auto patronus = Patronus::ins(config);

    sleep(1);

    // let client spining
    auto nid = patronus->get_node_id();
    if (nid == kClientNodeId)
    {
        patronus->registerClientThread();
        sleep(2);
        client(patronus);
        patronus->finished();
    }
    else
    {
        patronus->registerServerThread();
        patronus->finished();
        server(patronus);
    }

    LOG(INFO) << "finished. ctrl+C to quit.";
}