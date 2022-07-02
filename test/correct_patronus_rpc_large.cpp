#include <algorithm>
#include <random>

#include "Timer.h"
#include "boost/thread/barrier.hpp"
#include "gflags/gflags.h"
#include "patronus/Patronus.h"
#include "util/Hexdump.hpp"
#include "util/monitor.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

using namespace patronus;
constexpr static size_t kServerThreadNr = NR_DIRECTORY;
constexpr static size_t kClientThreadNr = kMaxAppThread;
// constexpr static size_t kClientThreadNr = 2;

static_assert(kClientThreadNr <= kMaxAppThread);
static_assert(kServerThreadNr <= NR_DIRECTORY);
constexpr static size_t kCoroCnt = 16;
// constexpr static size_t kCoroCnt = 1;

constexpr static size_t kTestTime = 1_K;

constexpr static size_t kWaitKey = 0;

using namespace std::chrono_literals;

constexpr static size_t kIOSize = MemoryMessagePayload();

uint64_t bench_locator(uint64_t key)
{
    return key * kIOSize;
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

inline size_t gen_coro_key(size_t nid, size_t thread_id, size_t coro_id)
{
    return nid * kCoroCnt * kMaxAppThread + thread_id * kCoroCnt + coro_id;
}

void client_worker(Patronus::pointer p, coro_t coro_id, CoroYield &yield)
{
    auto nid = p->get_node_id();
    auto tid = p->get_thread_id();
    auto server_nid = ::config::get_server_nids().front();

    auto dir_id = tid % kServerThreadNr;
    CHECK_LT(dir_id, kServerThreadNr);

    CoroContext ctx(tid, &yield, &client_coro.master, coro_id);

    LOG(INFO) << "[bench] tid " << tid << ", coro: " << ctx;

    size_t coro_key = gen_coro_key(nid, tid, coro_id);

    for (size_t time = 0; time < kTestTime; ++time)
    {
        client_comm.still_has_work[coro_id] = true;
        client_comm.finish_cur_task[coro_id] = false;
        client_comm.finish_all_task[coro_id] = false;

        DVLOG(2) << "[bench] client coro " << ctx << " start to got lease ";
        auto locate_offset = bench_locator(coro_key);

        auto ac_flag = (flag_t) AcquireRequestFlag::kNoRpc |
                       (flag_t) AcquireRequestFlag::kNoGc;
        auto lease = p->get_wlease(server_nid,
                                   dir_id,
                                   GlobalAddress(0, locate_offset),
                                   0 /* alloc_hint */,
                                   kIOSize,
                                   0ns,
                                   ac_flag,
                                   &ctx);

        auto rdma_buf = p->get_rdma_buffer(kIOSize);
        fast_pseudo_fill_buf(rdma_buf.buffer, kIOSize);

        p->rpc_write(lease,
                     rdma_buf.buffer,
                     kIOSize,
                     0 /* offset */,
                     0 /* flag */,
                     &ctx)
            .expect(RC::kOk);

        auto rdma_buf2 = p->get_rdma_buffer(kIOSize);
        memset(rdma_buf2.buffer, 0, kIOSize);
        p->rpc_read(lease,
                    rdma_buf2.buffer,
                    kIOSize,
                    0 /* offset */,
                    0 /* flag */,
                    &ctx)
            .expect(RC::kOk);
        CHECK_EQ(memcmp(rdma_buf.buffer, rdma_buf2.buffer, kIOSize), 0)
            << "** not equal. Write: " << std::endl
            << util::Hexdump(rdma_buf.buffer, kIOSize)
            << ", Read: " << std::endl
            << util::Hexdump(rdma_buf2.buffer, kIOSize);

        DVLOG(2) << "[bench] client coro " << ctx
                 << " start to relinquish lease ";
        auto rel_flag = (flag_t) LeaseModifyFlag::kNoRpc;
        p->relinquish(lease, 0, rel_flag, &ctx);

        p->put_rdma_buffer(std::move(rdma_buf));
        p->put_rdma_buffer(std::move(rdma_buf2));

        DVLOG(2) << "[bench] client coro " << ctx << " finished current task.";
        client_comm.still_has_work[coro_id] = true;
        client_comm.finish_cur_task[coro_id] = true;
        client_comm.finish_all_task[coro_id] = false;
        ctx.yield_to_master();
    }
    client_comm.still_has_work[coro_id] = false;
    client_comm.finish_cur_task[coro_id] = true;
    client_comm.finish_all_task[coro_id] = true;

    LOG(WARNING) << "worker coro " << (int) coro_id << ", thread " << tid
                 << " finished ALL THE TASK. yield to master.";

    ctx.yield_to_master();
}
void client_master(Patronus::pointer p, CoroYield &yield)
{
    auto tid = p->get_thread_id();

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
        auto nr = p->try_get_client_continue_coros(coro_buf, 2 * kCoroCnt);
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

    LOG(WARNING) << "[bench] all worker finish their work. at tid " << tid
                 << " thread exiting...";
}

void client(Patronus::pointer p)
{
    auto tid = p->get_thread_id();
    LOG(INFO) << "I am client. tid " << tid;
    for (size_t i = 0; i < kCoroCnt; ++i)
    {
        client_coro.workers[i] =
            CoroCall([p, i](CoroYield &yield) { client_worker(p, i, yield); });
    }
    client_coro.master =
        CoroCall([p](CoroYield &yield) { client_master(p, yield); });
    client_coro.master();
    LOG(INFO) << "[bench] thread " << tid << " going to leave client()";
}

void server(Patronus::pointer p)
{
    auto tid = p->get_thread_id();

    LOG(INFO) << "I am server. tid " << tid;

    p->server_serve(kWaitKey);
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    rdmaQueryDevice();

    PatronusConfig config;
    config.machine_nr = ::config::kMachineNr;

    auto patronus = Patronus::ins(config);

    std::vector<std::thread> threads;
    // let client spining
    auto nid = patronus->get_node_id();

    boost::barrier client_bar(kClientThreadNr);
    if (::config::is_client(nid))
    {
        auto dsm = patronus->get_dsm();
        patronus->keeper_barrier("begin", 100ms);

        for (size_t i = 0; i < kClientThreadNr - 1; ++i)
        {
            threads.emplace_back([patronus, &bar = client_bar]() {
                patronus->registerClientThread();
                auto tid = patronus->get_thread_id();
                client(patronus);
                LOG(INFO) << "[bench] thread " << tid << " finish its work";
                bar.wait();
            });
        }
        patronus->registerClientThread();
        auto tid = patronus->get_thread_id();
        client(patronus);
        LOG(INFO) << "[bench] thread " << tid << " finish its work";
        client_bar.wait();
        LOG(INFO) << "[bench] joined. thread " << tid << " call p->finished()";
        patronus->finished(kWaitKey);
    }
    else
    {
        for (size_t i = 0; i < kServerThreadNr - 1; ++i)
        {
            threads.emplace_back([patronus]() {
                patronus->registerServerThread();

                server(patronus);
            });
        }

        patronus->registerServerThread();

        auto dsm = patronus->get_dsm();
        patronus->finished(kWaitKey);

        patronus->keeper_barrier("begin", 100ms);
        server(patronus);
    }

    for (auto &t : threads)
    {
        t.join();
    }

    patronus->keeper_barrier("finished", 100ms);
    LOG(INFO) << "finished. ctrl+C to quit.";
}