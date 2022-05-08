#include <algorithm>
#include <random>

#include "Timer.h"
#include "boost/thread/barrier.hpp"
#include "gflags/gflags.h"
#include "patronus/Patronus.h"
#include "util/monitor.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

using namespace patronus;
constexpr static size_t kServerThreadNr = NR_DIRECTORY;
constexpr static size_t kClientThreadNr = kMaxAppThread - 1;

static_assert(kClientThreadNr <= kMaxAppThread);
static_assert(kServerThreadNr <= NR_DIRECTORY);
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

inline size_t gen_coro_key(size_t node_id, size_t thread_id, size_t coro_id)
{
    return kCoroStartKey + node_id * kMaxAppThread * kCoroCnt +
           thread_id * kCoroCnt + coro_id;
}
inline uint64_t gen_magic_1(size_t node_id, size_t thread_id, size_t coro_id)
{
    return kMagic1 + node_id * kCoroCnt * kMaxAppThread + thread_id * kCoroCnt +
           coro_id;
}
inline uint64_t gen_magic_2(size_t node_id, size_t thread_id, size_t coro_id)
{
    return kMagic2 + node_id * kCoroCnt * kMaxAppThread + thread_id * kCoroCnt +
           coro_id;
}

void client_worker(Patronus::pointer p, coro_t coro_id, CoroYield &yield)
{
    auto tid = p->get_thread_id();
    auto nid = p->get_node_id();

    auto server_nid = ::config::get_server_nids().front();

    auto dir_id = tid % kServerThreadNr;
    CHECK_LT(dir_id, NR_DIRECTORY);

    CoroContext ctx(tid, &yield, &client_coro.master, coro_id);

    LOG(INFO) << "[bench] tid " << tid << ", coro: " << ctx;

    size_t coro_key = gen_coro_key(nid, tid, coro_id);
    size_t coro_magic_1 = gen_magic_1(nid, tid, coro_id);
    size_t coro_magic_2 = gen_magic_2(nid, tid, coro_id);

    for (size_t time = 0; time < kTestTime; ++time)
    {
        PatronusBatchContext batch;
        CHECK(batch.empty());
        client_comm.still_has_work[coro_id] = true;
        client_comm.finish_cur_task[coro_id] = false;
        client_comm.finish_all_task[coro_id] = false;

        DVLOG(2) << "[bench] client coro " << ctx << " start to got lease ";
        auto locate_offset = bench_locator(coro_key);
        Lease lease = p->get_wlease(server_nid,
                                    dir_id,
                                    GlobalAddress(0, locate_offset),
                                    0 /* alloc_hint */,
                                    sizeof(Object),
                                    0ns,
                                    (flag_t) AcquireRequestFlag::kNoGc,
                                    &ctx);
        if (unlikely(!lease.success()))
        {
            // LOG(WARNING) << "[bench] client coro " << ctx
            //              << " get_rlease failed. retry.";
            CHECK_EQ(lease.ec(), AcquireRequestStatus::kMagicMwErr);
            continue;
        }

        DVLOG(2) << "[bench] client coro " << ctx << " got lease " << lease;

        auto flag = (flag_t) RWFlag::kNoLocalExpireCheck;

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

        auto internal_buf = patronus->get_server_internal_buffer();
        for (size_t m = 0; m < MAX_MACHINE; ++m)
        {
            for (size_t t = 0; t < kClientThreadNr + 1; ++t)
            {
                for (size_t i = 0; i < kCoroCnt; ++i)
                {
                    auto thread_id = t;
                    auto coro_id = i;
                    auto coro_magic_1 = gen_magic_1(m, thread_id, coro_id);
                    auto coro_magic_2 = gen_magic_2(m, thread_id, coro_id);
                    auto coro_key = gen_coro_key(m, thread_id, coro_id);
                    auto coro_offset = bench_locator(coro_key);

                    auto *server_internal_buf = internal_buf.buffer;
                    Object *where =
                        (Object *) &server_internal_buf[coro_offset];
                    where->target_1 = coro_magic_1;
                    where->target_2 = coro_magic_2;

                    DLOG(INFO)
                        << "[bench] server setting magic_1 " << coro_magic_1
                        << " and magic_2 " << coro_magic_2 << " to offset "
                        << coro_offset << ", and "
                        << coro_offset + offsetof(Object, target_2)
                        << ". actual addr: " << (void *) &(where->target_1)
                        << ", and " << (void *) &(where->target_2)
                        << " for coro " << coro_id << ", thread " << thread_id;
                }
            }
        }

        auto dsm = patronus->get_dsm();
        patronus->keeper_barrier("begin", 100ms);
        patronus->finished(kWaitKey);
        server(patronus);
    }

    for (auto &t : threads)
    {
        t.join();
    }

    LOG(INFO) << "finished. ctrl+C to quit.";
}