#include <algorithm>
#include <random>

#include "Timer.h"
#include "boost/thread/barrier.hpp"
#include "patronus/Patronus.h"
#include "util/monitor.h"

// Two nodes
constexpr uint16_t kClientNodeId = 0;
[[maybe_unused]] constexpr uint16_t kServerNodeId = 1;
constexpr uint32_t kMachineNr = 2;

using namespace patronus;
constexpr static size_t kThreadNr = 4;
static_assert(kThreadNr <= RMSG_MULTIPLEXING);
static_assert(kThreadNr <= MAX_APP_THREAD);
constexpr static size_t kCoroCnt = 8;

constexpr static uint64_t kMagic = 0xaabbccdd11223344;
constexpr static size_t kCoroStartKey = 1024;

constexpr static size_t kTestTime =
    Patronus::kMwPoolSizePerThread / kCoroCnt / NR_DIRECTORY / 2;

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

void client_worker(Patronus::pointer p, coro_t coro_id, CoroYield &yield)
{
    auto tid = p->get_thread_id();
    auto mid = tid;
    auto dir_id = tid;
    CHECK_LT(dir_id, NR_DIRECTORY);

    CoroContext ctx(tid, &yield, &client_coro.master, coro_id);

    LOG(INFO) << "[bench] tid " << tid << ", mid: " << mid << ", coro: " << ctx;

    size_t coro_key = gen_coro_key(tid, coro_id);
    size_t coro_expect_magic = gen_magic(tid, coro_id);

    for (size_t time = 0; time < kTestTime; ++time)
    {
        client_comm.still_has_work[coro_id] = true;
        client_comm.finish_cur_task[coro_id] = false;
        client_comm.finish_all_task[coro_id] = false;

        DVLOG(2) << "[bench] client coro " << ctx << " start to got lease ";
        Lease lease = p->get_wlease(kServerNodeId,
                                    dir_id,
                                    coro_key /* key */,
                                    sizeof(Object),
                                    0ns,
                                    (uint8_t) AcquireRequestFlag::kNoGc,
                                    &ctx);
        if (unlikely(!lease.success()))
        {
            LOG(WARNING) << "[bench] client coro " << ctx
                         << " get_rlease failed. retry.";
            continue;
        }

        DVLOG(2) << "[bench] client coro " << ctx << " got lease " << lease;

        auto rdma_buf = p->get_rdma_buffer();
        memset(rdma_buf.buffer, 0, sizeof(Object));

        DVLOG(2) << "[bench] client coro " << ctx << " start to read";
        CHECK_LT(sizeof(Object), rdma_buf.size);
        auto ec = p->read(lease,
                          rdma_buf.buffer,
                          sizeof(Object),
                          0 /* offset */,
                          0 /* flag */,
                          &ctx);
        CHECK_EQ(ec, ErrCode::kSuccess)
            << "[bench] client coro " << ctx
            << " read FAILED. This should not happen, because we "
               "filter out the invalid mws.";

        DVLOG(2) << "[bench] client coro " << ctx << " read finished";
        Object &magic_object = *(Object *) rdma_buf.buffer;
        CHECK_EQ(magic_object.target, coro_expect_magic)
            << "coro_id " << ctx << ", Read at key " << coro_key
            << " expect magic " << coro_expect_magic
            << ", lease.base: " << (void *) lease.base_addr()
            << ", actual offset: " << bench_locator(coro_key);
        DVLOG(2) << "[bench] client coro " << ctx
                 << " start to relinquish lease ";

        // now, execute two writes
        coro_expect_magic++;
        magic_object.target = coro_expect_magic;
        ec = p->write(lease,
                      rdma_buf.buffer,
                      sizeof(Object),
                      0 /* offset */,
                      0 /* flag */,
                      &ctx);
        CHECK_EQ(ec, ErrCode::kSuccess)
            << "[bench] client coro " << ctx
            << ", write failed. This should not happend, because we filter out "
               "the invalid mws and no lease expiration";

        p->relinquish_write(lease, &ctx);
        p->relinquish(lease, 0, &ctx);

        p->put_rdma_buffer(rdma_buf.buffer);

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
    auto mid = tid;

    LOG(INFO) << "I am server. tid " << tid << " handling mid " << mid;

    p->server_serve(mid);
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

    std::vector<std::thread> threads;
    // let client spining
    auto nid = patronus->get_node_id();

    boost::barrier bar(kThreadNr);
    if (nid == kClientNodeId)
    {
        auto dsm = patronus->get_dsm();
        dsm->reliable_recv(0, nullptr, 1);

        for (size_t i = 0; i < kThreadNr - 1; ++i)
        {
            threads.emplace_back([patronus, &bar]() {
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
        bar.wait();
        LOG(INFO) << "[bench] joined. thread " << tid << " call p->finished()";
        patronus->finished();
    }
    else
    {
        for (size_t i = 0; i < kThreadNr - 1; ++i)
        {
            threads.emplace_back([patronus]() {
                patronus->registerServerThread();

                server(patronus);
            });
        }

        patronus->registerServerThread();

        auto internal_buf = patronus->get_server_internal_buffer();
        for (size_t t = 0; t < kThreadNr + 1; ++t)
        {
            for (size_t i = 0; i < kCoroCnt; ++i)
            {
                auto thread_id = t;
                auto coro_id = i;
                auto coro_magic = gen_magic(thread_id, coro_id);
                auto coro_key = gen_coro_key(thread_id, coro_id);
                auto coro_offset = bench_locator(coro_key);

                auto *server_internal_buf = internal_buf.buffer;
                Object *where = (Object *) &server_internal_buf[coro_offset];
                where->target = coro_magic;

                DLOG(INFO) << "[bench] server setting " << coro_magic
                           << " to offset " << coro_offset
                           << ". actual addr: " << (void *) &(where->target)
                           << " for coro " << coro_id << ", thread "
                           << thread_id;
            }
        }

        auto dsm = patronus->get_dsm();
        // sync
        dsm->reliable_send(nullptr, 0, kClientNodeId, 0);

        patronus->finished();

        server(patronus);
    }

    for (auto &t : threads)
    {
        t.join();
    }

    LOG(INFO) << "finished. ctrl+C to quit.";
}