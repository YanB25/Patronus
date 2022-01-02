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
constexpr static size_t kCoroCnt = 8;
thread_local CoroCall workers[kCoroCnt];
thread_local CoroCall master;

constexpr static uint64_t kMagic = 0xaabbccdd11223344;
constexpr static size_t kCoroStartKey = 1024;
constexpr static size_t kDirID = 0;

constexpr static size_t kTestTime =
    Patronus::kMwPoolSizePerThread / kCoroCnt / NR_DIRECTORY;

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
    CoroContext ctx(&yield, &master, coro_id);

    size_t coro_key = kCoroStartKey + coro_id;
    size_t coro_magic = kMagic + coro_id;

    for (size_t time = 0; time < kTestTime; ++time)
    {
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

        DVLOG(2) << "[bench] client coro " << ctx << " got lease " << lease;

        auto rdma_buf = p->get_rdma_buffer();
        memset(rdma_buf.buffer, 0, sizeof(Object));

        DVLOG(2) << "[bench] client coro " << ctx << " start to read";
        CHECK_LT(sizeof(Object), rdma_buf.size);
        bool succ = p->read(lease,
                            rdma_buf.buffer,
                            sizeof(Object),
                            0 /* offset */,
                            kDirID,
                            &ctx);
        if (!succ)
        {
            VLOG(1) << "[bench] client coro " << ctx << " read FAILED. retry. ";
            p->put_rdma_buffer(rdma_buf.buffer);
            bench_info.fail_nr++;
            continue;
        }
        DVLOG(2) << "[bench] client coro " << ctx << " read finished";
        Object magic_object = *(Object *) rdma_buf.buffer;
        CHECK_EQ(magic_object.target, coro_magic)
            << "coro_id " << ctx << ", Read at key " << coro_key
            << ", lease.base: " << (void *) lease.base_addr();

        p->put_rdma_buffer(rdma_buf.buffer);

        DVLOG(2) << "[bench] client coro " << ctx << " finished current task.";
        bench_info.success_nr++;
        client_comm.still_has_work[coro_id] = true;
        client_comm.finish_cur_task[coro_id] = true;
        client_comm.finish_all_task[coro_id] = false;
        ctx.yield_to_master();
    }
    client_comm.still_has_work[coro_id] = false;
    client_comm.finish_cur_task[coro_id] = true;
    client_comm.finish_all_task[coro_id] = true;

    LOG(WARNING) << "worker coro " << (int) coro_id
                 << " finished ALL THE TASK. yield to master.";

    ctx.yield_to_master();
}
void client_master(Patronus::pointer p, CoroYield &yield)
{
    CoroContext mctx(&yield, workers);
    CHECK(mctx.is_master());

    auto tid = p->get_thread_id();
    auto mid = tid;

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
                usleep(10 * 1000);
                auto then_success = bench_info.success_nr;
                auto then_fail = bench_info.fail_nr;
                auto then = std::chrono::steady_clock::now();
                auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              then - now)
                              .count();

                auto success_op = then_success - cur_success;
                auto fail_op = then_fail - cur_fail;
                LOG_IF(INFO, cur_success > 0) << "[bench] Op: " << success_op
                          << ", fail Op: " << fail_op << " for " << ns
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

thread_local CoroCall server_workers[kCoroCnt];
thread_local CoroCall server_master_coro;
// thread_local bool server_first[kCoroCnt];

struct Task
{
    const char *buf{nullptr};
    size_t msg_nr{0};
    size_t fetched_nr{0};
    size_t finished_nr{0};
    // because we need to call p->put_rdma_buffer(buf) when all the things get
    // done.
    std::function<void()> call_back_on_finish;
};
struct ServerCoroutineCommunicationContext
{
    bool finished[kCoroCnt];
    std::queue<std::shared_ptr<Task>> task_queue;
} server_comm;

void server_worker(Patronus::pointer p, coro_t coro_id, CoroYield &yield)
{
    CoroContext ctx(&yield, &server_master_coro, coro_id);

    while (!p->should_exit())
    {
        auto task = server_comm.task_queue.front();
        CHECK_NE(task->msg_nr, 0);
        CHECK_LT(task->fetched_nr, task->msg_nr);
        auto cur_nr = task->fetched_nr;
        const char *cur_msg =
            task->buf + ReliableConnection::kMessageSize * cur_nr;
        task->fetched_nr++;
        if (task->fetched_nr == task->msg_nr)
        {
            server_comm.task_queue.pop();
        }
        DVLOG(1) << "[bench] server handling task @" << (void *) task.get()
                 << ", message_nr " << task->msg_nr << ", cur_fetched "
                 << cur_nr << ", cur_finished: " << task->finished_nr << " "
                 << ctx;
        p->handle_request_messages(cur_msg, 1, &ctx);
        task->finished_nr++;
        if (task->finished_nr == task->msg_nr)
        {
            DVLOG(1) << "[bench] server handling callback of task @"
                     << (void *) task.get();
            task->call_back_on_finish();
        }

        DVLOG(1) << "[bench] server " << ctx
                 << " finished current task. yield to master.";
        server_comm.finished[coro_id] = true;
        ctx.yield_to_master();
    }

    LOG(WARNING) << "[bench] server coro: " << ctx << " exit.";
    ctx.yield_to_master();
}

void server_master(Patronus::pointer p, CoroYield &yield)
{
    CoroContext mctx(&yield, server_workers);
    CHECK(mctx.is_master());

    auto tid = p->get_thread_id();
    auto mid = tid;

    for (size_t i = 0; i < kCoroCnt; ++i)
    {
        server_comm.finished[i] = true;
    }

    constexpr static size_t kServerBufferNr = 1024;
    char *__buffer =
        (char *) malloc(ReliableConnection::kMaxRecvBuffer * kServerBufferNr);
    ThreadUnsafeBufferPool<ReliableConnection::kMaxRecvBuffer> buffer_pool(
        __buffer, ReliableConnection::kMaxRecvBuffer * kServerBufferNr);
    coro_t coro_buf[kCoroCnt * 2];
    while (!p->should_exit())
    {
        char *buffer = (char *) CHECK_NOTNULL(buffer_pool.get());
        // DVLOG(3) << "[bench] buffer get. remain size: " <<
        // buffer_pool.size();
        size_t nr =
            p->reliable_try_recv(mid, buffer, ReliableConnection::kRecvLimit);
        if (likely(nr > 0))
        {
            DVLOG(1) << "[bench] server recv messages " << nr
                     << ". Dispatch to workers, avg get " << (nr / kCoroCnt);
            std::shared_ptr<Task> task = std::make_shared<Task>();
            task->buf = CHECK_NOTNULL(buffer);
            task->msg_nr = nr;
            task->fetched_nr = 0;
            task->finished_nr = 0;
            task->call_back_on_finish = [&buffer_pool, buffer]()
            {
                buffer_pool.put(buffer);
                // DVLOG(3) << "[bench] buffer put. remain size: "
                //         << buffer_pool.size();
            };
            server_comm.task_queue.push(task);
        }
        else
        {
            buffer_pool.put(buffer);
        }

        if (!server_comm.task_queue.empty())
        {
            for (size_t i = 0; i < kCoroCnt; ++i)
            {
                if (!server_comm.task_queue.empty())
                {
                    // it finished its last reqeust.
                    if (server_comm.finished[i])
                    {
                        server_comm.finished[i] = false;
                        DVLOG(3) << "[bench] yield to " << (int) i
                                 << " because has task";
                        mctx.yield_to_worker(i);
                    }
                }
            }
        }

        nr = p->try_get_server_finished_coros(coro_buf, kDirID, 2 * kCoroCnt);
        if (likely(nr > 0))
        {
            for (size_t i = 0; i < nr; ++i)
            {
                coro_t coro_id = coro_buf[i];
                DVLOG(3) << "[bench] yield to " << (int) coro_id
                         << " because CQE arrvied.";
                DCHECK(!server_comm.finished[coro_id])
                    << "coro " << (int) coro_id
                    << " already finish. should not receive CQE. ";

                mctx.yield_to_worker(coro_id);
            }
        }
    }

    free(__buffer);
}

void server(Patronus::pointer p)
{
    auto tid = p->get_thread_id();
    // auto mid = tid;

    LOG(INFO) << "I am server. tid " << tid;

    auto dsm = p->get_dsm();
    auto internal_buf = dsm->get_server_internal_buffer();
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

    for (size_t i = 0; i < kCoroCnt; ++i)
    {
        server_workers[i] =
            CoroCall([p, i](CoroYield &yield) { server_worker(p, i, yield); });
    }
    server_master_coro =
        CoroCall([p](CoroYield &yield) { server_master(p, yield); });

    server_master_coro();
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    bindCore(0);

    rdmaQueryDevice();

    DSMConfig config;
    config.machineNR = kMachineNr;

    // auto dsm = DSM::getInstance(config);
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
        patronus->reg_locator(bench_locator);
        patronus->finished();
        server(patronus);
    }

    LOG(INFO) << "finished. ctrl+C to quit.";
}