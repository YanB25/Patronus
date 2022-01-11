#include <algorithm>
#include <random>

#include "Timer.h"
#include "patronus/Patronus.h"
#include "util/monitor.h"

// Two nodes
// one node issues cas operations

constexpr uint16_t kClientNodeId = 0;
[[maybe_unused]] constexpr uint16_t kServerNodeId = 1;
constexpr uint32_t kMachineNr = 2;

using namespace patronus;
constexpr static size_t kCoroCnt = 16;
static_assert(kCoroCnt <= Patronus::kMaxCoroNr);
thread_local CoroCall workers[kCoroCnt];
thread_local CoroCall master;

constexpr static uint64_t kMagic = 0xaabbccdd11223344;
constexpr static size_t kCoroStartKey = 1024;
constexpr static size_t kDirID = 0;

// constexpr static size_t kTestTime =
//     Patronus::kMwPoolSizePerThread / kCoroCnt / NR_DIRECTORY;
constexpr static size_t kTestTime = 1 * define::M;

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

struct BenchInformation
{
    size_t success_nr{0};
    size_t fail_nr{0};
} bench_info;

void client_worker(Patronus::pointer p, coro_t coro_id, CoroYield &yield)
{
    auto tid = p->get_thread_id();

    CoroContext ctx(tid, &yield, &master, coro_id);

    size_t coro_key = kCoroStartKey + coro_id;
    size_t coro_magic = kMagic + coro_id;

    auto rdma_buf = p->get_rdma_buffer();
    memset(rdma_buf.buffer, 0, sizeof(Object));

    for (size_t time = 0; time < kTestTime; ++time)
    {
        trace_t trace = 0;
        bool enable_trace = false;
        if constexpr (config::kEnableTrace)
        {
            enable_trace = (rand() % config::kTraceRate) == 0;
            if (unlikely(enable_trace))
            {
                while (trace == 0)
                {
                    trace = rand();
                }
                auto &timer = ctx.timer();
                timer.init(std::to_string(trace));
                ctx.set_trace(trace);
            }
        }

        client_comm.still_has_work[coro_id] = true;
        client_comm.finish_cur_task[coro_id] = false;
        client_comm.finish_all_task[coro_id] = false;

        DVLOG(2) << "[bench] client coro " << ctx << " start to got lease ";
        Lease lease = p->get_rlease(kServerNodeId,
                                    kDirID,
                                    coro_key /* key */,
                                    sizeof(Object),
                                    100,
                                    &ctx);
        if (unlikely(!lease.success()))
        {
            DLOG(WARNING) << "[bench] client coro " << ctx
                          << " get_rlease failed. retry.";
            bench_info.fail_nr++;
            continue;
        }

        if (unlikely(enable_trace))
        {
            ctx.timer().pin("[client] get lease");
        }

        // p->pingpong(kServerNodeId, kDirID, coro_key, sizeof(Object), 100,
        // &ctx); timer.pin("pingpong finished");

        DVLOG(2) << "[bench] client coro " << ctx << " got lease " << lease;

        DVLOG(2) << "[bench] client coro " << ctx << " start to read";
        DCHECK_LT(sizeof(Object), rdma_buf.size);
        bool succ = p->read(
            lease, rdma_buf.buffer, sizeof(Object), 0 /* offset */, &ctx);
        if (unlikely(!succ))
        {
            DVLOG(1) << "[bench] client coro " << ctx
                     << " read FAILED. retry. ";
            bench_info.fail_nr++;
            p->relinquish(lease, &ctx);
            continue;
        }

        if (unlikely(enable_trace))
        {
            ctx.timer().pin("[client] read finished");
        }


        DVLOG(2) << "[bench] client coro " << ctx << " read finished";
        Object magic_object = *(Object *) rdma_buf.buffer;
        DCHECK_EQ(magic_object.target, coro_magic)
            << "coro_id " << ctx << ", Read at key " << coro_key
            << ", lease.base: " << (void *) lease.base_addr();

        p->relinquish(lease, &ctx);

        if (unlikely(enable_trace))
        {
            ctx.timer().pin("[client] relinquish");
        }
        
        DVLOG(2) << "[bench] client coro " << ctx << " relinquish ";

        DVLOG(2) << "[bench] client coro " << ctx << " finished current task.";
        bench_info.success_nr++;
        client_comm.still_has_work[coro_id] = true;
        client_comm.finish_cur_task[coro_id] = true;
        client_comm.finish_all_task[coro_id] = false;

        if constexpr (config::kEnableTrace)
        {
            if (unlikely(enable_trace))
            {
                LOG(INFO) << "[bench] " << ctx.timer();
            }
        }

        ctx.yield_to_master();
    }
    client_comm.still_has_work[coro_id] = false;
    client_comm.finish_cur_task[coro_id] = true;
    client_comm.finish_all_task[coro_id] = true;

    p->put_rdma_buffer(rdma_buf.buffer);

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

    std::atomic<bool> finish{false};
    std::thread monitor_thread(
        [&finish]()
        {
            while (!finish.load(std::memory_order_relaxed))
            {
                auto cur_success = bench_info.success_nr;
                auto cur_fail = bench_info.fail_nr;
                auto now = std::chrono::steady_clock::now();
                usleep(1000 * 1000);
                auto then_success = bench_info.success_nr;
                auto then_fail = bench_info.fail_nr;
                auto then = std::chrono::steady_clock::now();
                auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              then - now)
                              .count();

                auto success_op = then_success - cur_success;
                auto fail_op = then_fail - cur_fail;
                LOG_IF(INFO, cur_success > 0)
                    << "[bench] Op: " << success_op << ", fail Op: " << fail_op
                    << " for " << ns
                    << " ns. OPS: " << 1.0 * 1e9 * success_op / ns;
            }
        });

    for (size_t i = 0; i < kCoroCnt; ++i)
    {
        workers[i] =
            CoroCall([p, i](CoroYield &yield) { client_worker(p, i, yield); });
    }
    master = CoroCall([p](CoroYield &yield) { client_master(p, yield); });
    master();

    finish.store(true);
    monitor_thread.join();
}

void server(Patronus::pointer p)
{
    auto tid = p->get_thread_id();
    // auto mid = tid;

    LOG(INFO) << "I am server. tid " << tid;

    auto internal_buf = p->get_server_internal_buffer();
    for (size_t i = 0; i < kCoroCnt; ++i)
    {
        auto coro_magic = kMagic + i;
        auto coro_offset = bench_locator(kCoroStartKey + i);

        auto *server_internal_buf = internal_buf.buffer;
        Object *where = (Object *) &server_internal_buf[coro_offset];
        where->target = coro_magic;

        DVLOG(1) << "[bench] server setting " << coro_magic << " to offset "
                 << coro_offset
                 << ". actual addr: " << (void *) &(where->target)
                 << " for coro " << i;
    }

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

    // let client spining
    auto nid = patronus->get_node_id();
    if (nid == kClientNodeId)
    {
        patronus->registerClientThread();
        sleep(2);
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