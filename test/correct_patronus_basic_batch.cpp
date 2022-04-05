#include <algorithm>
#include <random>

#include "Timer.h"
#include "boost/thread/barrier.hpp"
#include "patronus/Patronus.h"
#include "util/monitor.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

// Two nodes
constexpr uint16_t kClientNodeId = 0;
[[maybe_unused]] constexpr uint16_t kServerNodeId = 1;
constexpr uint32_t kMachineNr = 2;

using namespace patronus;
constexpr static size_t kThreadNr = 4;
static_assert(kThreadNr <= RMSG_MULTIPLEXING);
static_assert(kThreadNr <= kMaxAppThread);
constexpr static size_t kCoroCnt = 8;

constexpr static uint64_t kMagic1 = 0xaabbccdd11223344;
constexpr static uint64_t kMagic2 = 0xabcdabcd12341234;
constexpr static size_t kCoroStartKey = 1024;

constexpr static size_t kTestTime =
    Patronus::kMwPoolSizePerThread / kCoroCnt / NR_DIRECTORY / 2;

constexpr static size_t kWaitKey = 0;

using namespace std::chrono_literals;

struct Object
{
    uint64_t target_1;
    uint64_t unused_1;
    uint64_t unused_2;
    uint64_t target_2;
};

uint64_t bench_locator(uint64_t key)
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
inline uint64_t gen_magic_1(size_t thread_id, size_t coro_id)
{
    return kMagic1 + thread_id * kCoroCnt + coro_id;
}
inline uint64_t gen_magic_2(size_t thread_id, size_t coro_id)
{
    return kMagic2 + thread_id * kCoroCnt + coro_id;
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
    size_t coro_magic_1 = gen_magic_1(tid, coro_id);
    size_t coro_magic_2 = gen_magic_2(tid, coro_id);

    for (size_t time = 0; time < kTestTime; ++time)
    {
        PatronusBatchContext batch;
        CHECK(batch.empty());
        client_comm.still_has_work[coro_id] = true;
        client_comm.finish_cur_task[coro_id] = false;
        client_comm.finish_all_task[coro_id] = false;

        DVLOG(2) << "[bench] client coro " << ctx << " start to got lease ";
        auto locate_offset = bench_locator(coro_key);
        Lease lease = p->get_wlease(GlobalAddress(kServerNodeId, locate_offset),
                                    dir_id,
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

        auto flag = (uint8_t) RWFlag::kNoLocalExpireCheck;

        auto rdma_buf_1 = p->get_rdma_buffer(sizeof(uint64_t));
        CHECK_GE(rdma_buf_1.size, sizeof(uint64_t));
        memset(rdma_buf_1.buffer, 0, sizeof(uint64_t));
        CHECK_LE(sizeof(uint64_t), rdma_buf_1.size);
        auto ec = p->prepare_read(batch,
                                  lease,
                                  rdma_buf_1.buffer,
                                  sizeof(uint64_t),
                                  offsetof(Object, target_1),
                                  flag,
                                  &ctx);
        CHECK_EQ(ec, kOk);

        auto rdma_buf_2 = p->get_rdma_buffer(sizeof(uint64_t));
        CHECK_GE(rdma_buf_2.size, sizeof(uint64_t));
        memset(rdma_buf_2.buffer, 0, sizeof(uint64_t));
        CHECK_LE(sizeof(uint64_t), rdma_buf_2.size);
        ec = p->prepare_read(batch,
                             lease,
                             rdma_buf_2.buffer,
                             sizeof(uint64_t),
                             offsetof(Object, target_2),
                             flag,
                             &ctx);
        CHECK_EQ(ec, kOk);

        ec = p->commit(batch, &ctx);
        CHECK_EQ(ec, kOk);

        DVLOG(2) << "[bench] client coro " << ctx << " read finished";
        uint64_t target_1 = *(uint64_t *) rdma_buf_1.buffer;
        CHECK_EQ(target_1, coro_magic_1)
            << "coro_id " << ctx << ", Read at key " << coro_key
            << " expect magic " << coro_magic_1
            << ", lease.base: " << (void *) lease.base_addr()
            << ", actual offset: " << bench_locator(coro_key);
        uint64_t target_2 = *(uint64_t *) rdma_buf_2.buffer;
        CHECK_EQ(target_2, coro_magic_2)
            << "coro_id " << ctx << ", Read at key " << coro_key
            << " expect magic " << coro_magic_2
            << ", lease.base: " << (void *) lease.base_addr()
            << ", actual offset: " << bench_locator(coro_key);

        // write to magic + 1
        uint64_t coro_write_magic_1 = coro_magic_1 + 1;
        memcpy(rdma_buf_1.buffer,
               (const char *) &coro_write_magic_1,
               sizeof(coro_write_magic_1));
        uint64_t coro_write_magic_2 = coro_magic_2 + 1;
        memcpy(rdma_buf_2.buffer,
               (const char *) &coro_write_magic_2,
               sizeof(coro_write_magic_2));
        CHECK(batch.empty());
        ec = p->prepare_write(batch,
                              lease,
                              rdma_buf_1.buffer,
                              sizeof(uint64_t),
                              offsetof(Object, target_1),
                              flag,
                              &ctx);
        CHECK_EQ(ec, kOk);
        ec = p->prepare_write(batch,
                              lease,
                              rdma_buf_2.buffer,
                              sizeof(uint64_t),
                              offsetof(Object, target_2),
                              flag,
                              &ctx);
        CHECK_EQ(ec, kOk);
        ec = p->commit(batch, &ctx);
        CHECK_EQ(ec, kOk);

        // cas to original value.
        memset(rdma_buf_1.buffer, 0, sizeof(uint64_t));
        memset(rdma_buf_2.buffer, 0, sizeof(uint64_t));
        ec = p->prepare_cas(batch,
                            lease,
                            rdma_buf_1.buffer,
                            offsetof(Object, target_1),
                            coro_write_magic_1,
                            coro_magic_1,
                            flag,
                            &ctx);
        CHECK_EQ(ec, kOk);
        ec = p->prepare_cas(batch,
                            lease,
                            rdma_buf_2.buffer,
                            offsetof(Object, target_2),
                            coro_write_magic_2,
                            coro_magic_2,
                            flag,
                            &ctx);
        CHECK_EQ(ec, kOk);
        ec = p->commit(batch, &ctx);
        CHECK_EQ(ec, kOk);
        CHECK_EQ(memcmp(rdma_buf_1.buffer,
                        (const char *) &coro_write_magic_1,
                        sizeof(uint64_t)),
                 0);
        CHECK_EQ(memcmp(rdma_buf_2.buffer,
                        (const char *) &coro_write_magic_2,
                        sizeof(uint64_t)),
                 0);

        DVLOG(2) << "[bench] client coro " << ctx
                 << " start to relinquish lease ";
        p->relinquish(lease, 0, 0, &ctx);

        p->put_rdma_buffer(rdma_buf_1);
        p->put_rdma_buffer(rdma_buf_2);

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

    p->server_serve(mid, kWaitKey);
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    rdmaQueryDevice();

    PatronusConfig config;
    config.machine_nr = kMachineNr;

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
        patronus->finished(kWaitKey);
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
                auto coro_magic_1 = gen_magic_1(thread_id, coro_id);
                auto coro_magic_2 = gen_magic_2(thread_id, coro_id);
                auto coro_key = gen_coro_key(thread_id, coro_id);
                auto coro_offset = bench_locator(coro_key);

                auto *server_internal_buf = internal_buf.buffer;
                Object *where = (Object *) &server_internal_buf[coro_offset];
                where->target_1 = coro_magic_1;
                where->target_2 = coro_magic_2;

                DLOG(INFO) << "[bench] server setting magic_1 " << coro_magic_1
                           << " and magic_2 " << coro_magic_2 << " to offset "
                           << coro_offset << ", and "
                           << coro_offset + offsetof(Object, target_2)
                           << ". actual addr: " << (void *) &(where->target_1)
                           << ", and " << (void *) &(where->target_2)
                           << " for coro " << coro_id << ", thread "
                           << thread_id;
            }
        }

        auto dsm = patronus->get_dsm();
        // sync
        dsm->reliable_send(nullptr, 0, kClientNodeId, 0);

        patronus->finished(kWaitKey);

        server(patronus);
    }

    for (auto &t : threads)
    {
        t.join();
    }

    LOG(INFO) << "finished. ctrl+C to quit.";
}