#include <algorithm>
#include <random>

#include "PerThread.h"
#include "Timer.h"
#include "boost/thread/barrier.hpp"
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
constexpr static uint64_t kMagic = 0xaabbccdd11223344;
constexpr static size_t kCoroStartKey = 1024;

constexpr static size_t kTestTime = 1 * define::M;
constexpr static size_t kThreadNr = 4;
static_assert(kThreadNr <= MAX_APP_THREAD);
static_assert(kThreadNr <= RMSG_MULTIPLEXING);

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

struct ClientCoro
{
    CoroCall workers[kCoroCnt];
    CoroCall master;
};
thread_local ClientCoro client_coro;

struct ClientCommunication
{
    bool still_has_work[kCoroCnt];
    bool finish_cur_task[kCoroCnt];
    bool finish_all_task[kCoroCnt];
};
thread_local ClientCommunication client_comm;

inline size_t gen_coro_key(size_t thread_id, size_t coro_id)
{
    return kCoroStartKey + thread_id * kCoroCnt + coro_id;
}
inline uint64_t gen_magic(size_t thread_id, size_t coro_id)
{
    return kMagic + thread_id * kCoroCnt + coro_id;
}

struct BenchInformation
{
    size_t success_nr{0};
    size_t fail_nr{0};
};

Perthread<BenchInformation> bench_infos;

void client_worker(Patronus::pointer p, coro_t coro_id, CoroYield &yield)
{
    auto tid = p->get_thread_id();
    auto mid = tid;
    auto dir_id = mid;
    CHECK_NE(mid, 0) << "mid 0 is reserved for admin requests";

    CoroContext ctx(tid, &yield, &client_coro.master, coro_id);

    auto coro_key = gen_coro_key(tid, coro_id);
    auto coro_magic = gen_magic(tid, coro_id);

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
                                    dir_id,
                                    coro_key /* key */,
                                    sizeof(Object),
                                    100,
                                    &ctx);
        if (unlikely(!lease.success()))
        {
            DLOG(WARNING) << "[bench] client coro " << ctx
                          << " get_rlease failed. retry.";
            bench_infos[tid].fail_nr++;
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
            bench_infos[tid].fail_nr++;
            // p->relinquish_write(lease, &ctx);
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
            << ", lease.base: " << (void *) lease.base_addr()
            << ", offset: " << bench_locator(coro_key);

        // p->relinquish_write(lease, &ctx);
        p->relinquish(lease, &ctx);

        if (unlikely(enable_trace))
        {
            ctx.timer().pin("[client] relinquish");
        }

        DVLOG(2) << "[bench] client coro " << ctx << " relinquish ";

        DVLOG(2) << "[bench] client coro " << ctx << " finished current task.";
        bench_infos[tid].success_nr++;
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

    CoroContext mctx(tid, &yield, client_coro.workers);
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

    LOG(WARNING) << "[bench] all worker finish their work. exiting...";
}

std::pair<uint64_t, uint64_t> get_success_fair_nr()
{
    std::pair<uint64_t, uint64_t> ret;
    for (size_t i = 0; i < std::min(int(kThreadNr + 1), MAX_APP_THREAD); ++i)
    {
        ret.first += bench_infos[i].success_nr;
        ret.second += bench_infos[i].fail_nr;
    }
    return ret;
}

void client(Patronus::pointer p)
{
    auto tid = p->get_thread_id();
    LOG(INFO) << "I am client. tid " << tid;

    std::atomic<bool> finish{false};
    std::thread monitor_thread([&finish]() {
        while (!finish.load(std::memory_order_relaxed))
        {
            auto [cur_success, cur_fail] = get_success_fair_nr();
            auto now = std::chrono::steady_clock::now();
            usleep(1000 * 1000);
            auto [then_success, then_fail] = get_success_fair_nr();
            auto then = std::chrono::steady_clock::now();
            auto ns =
                std::chrono::duration_cast<std::chrono::nanoseconds>(then - now)
                    .count();

            auto success_op = then_success - cur_success;
            auto fail_op = then_fail - cur_fail;
            double ops = 1.0 * 1e9 * success_op / ns;
            double ops_thread = ops / kThreadNr;
            LOG_IF(INFO, cur_success > 0)
                << "[bench] Op: " << success_op << ", fail Op: " << fail_op
                << " for " << ns << " ns. OPS: " << ops
                << ", OPS/thread: " << ops_thread;
        }
    });

    for (size_t i = 0; i < kCoroCnt; ++i)
    {
        client_coro.workers[i] =
            CoroCall([p, i](CoroYield &yield) { client_worker(p, i, yield); });
    }
    client_coro.master =
        CoroCall([p](CoroYield &yield) { client_master(p, yield); });
    client_coro.master();

    finish.store(true);
    monitor_thread.join();
}

void server(Patronus::pointer p)
{
    auto tid = p->get_thread_id();
    // auto mid = tid;

    LOG(INFO) << "I am server. tid " << tid;

    p->server_serve();
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    rdmaQueryDevice();

    PatronusConfig config;
    config.machine_nr = kMachineNr;
    config.key_locator = bench_locator;

    auto patronus = Patronus::ins(config);

    boost::barrier bar(kThreadNr);
    std::vector<std::thread> threads;
    // let client spining
    auto nid = patronus->get_node_id();
    if (nid == kClientNodeId)
    {
        for (size_t i = 0; i < kThreadNr; ++i)
        {
            threads.emplace_back([patronus, &bar, i]() {
                patronus->registerClientThread();
                auto tid = patronus->get_thread_id();
                sleep(2);
                client(patronus);
                DLOG(INFO) << "[bench] thread " << tid << " finish it work";
                bar.wait();
                if (i == 0)
                {
                    patronus->finished();
                }
            });
        }
    }
    else
    {
        for (size_t i = 0; i < kThreadNr; ++i)
        {
            threads.emplace_back([patronus, i]() {
                patronus->registerServerThread();
                if (i == 0)
                {
                    patronus->finished();
                }

                auto internal_buf = patronus->get_server_internal_buffer();
                for (size_t t = 0; t < kThreadNr; ++t)
                {
                    for (size_t i = 0; i < kCoroCnt; ++i)
                    {
                        auto thread_id = t + 1;
                        auto coro_id = i;
                        auto coro_magic = gen_magic(thread_id, coro_id);
                        auto coro_key = gen_coro_key(thread_id, coro_id);
                        auto coro_offset = bench_locator(coro_key);

                        auto *server_internal_buf = internal_buf.buffer;
                        Object *where =
                            (Object *) &server_internal_buf[coro_offset];
                        where->target = coro_magic;

                        DLOG(INFO)
                            << "[bench] server setting " << coro_magic
                            << " to offset " << coro_offset
                            << ". actual addr: " << (void *) &(where->target)
                            << " for coro " << i << ", thread " << thread_id;
                    }
                }

                server(patronus);
            });
        }
    }

    patronus->wait_join(threads);

    LOG(INFO) << "finished. ctrl+C to quit.";
}