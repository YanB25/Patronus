#include <algorithm>
#include <chrono>
#include <random>
#include <thread>

#include "Timer.h"
#include "gflags/gflags.h"
#include "patronus/Patronus.h"
#include "util/monitor.h"

using namespace std::chrono_literals;

DEFINE_string(exec_meta, "", "The meta data of this execution");

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

constexpr static uint64_t kWaitFlag = 0;

using namespace std::chrono_literals;

struct Object
{
    uint64_t target;
    uint64_t unused_1;
    uint64_t unused_2;
    uint64_t unused_3;
};

uint64_t bench_locator(uint64_t key)
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

    size_t coro_key = kCoroStartKey + coro_id;
    size_t coro_magic = kMagic + coro_id;

    for (size_t time = 0; time < kTestTime; ++time)
    {
        client_comm.still_has_work[coro_id] = true;
        client_comm.finish_cur_task[coro_id] = false;
        client_comm.finish_all_task[coro_id] = false;

        DVLOG(2) << "[bench] client coro " << ctx << " start to got lease ";
        auto locate_offset = bench_locator(coro_key);
        Lease lease = p->get_rlease(kServerNodeId,
                                    kDirID,
                                    GlobalAddress(0, locate_offset),
                                    0 /* alloc_hint */,
                                    sizeof(Object),
                                    0ns,
                                    (flag_t) AcquireRequestFlag::kNoGc,
                                    &ctx);
        if (unlikely(!lease.success()))
        {
            LOG(WARNING) << "[bench] client coro " << ctx
                         << " get_rlease failed. retry.";
            continue;
        }

        DVLOG(2) << "[bench] client coro " << ctx << " got lease " << lease;

        auto rdma_buf = p->get_rdma_buffer(sizeof(Object));
        CHECK_GE(rdma_buf.size, sizeof(Object));
        memset(rdma_buf.buffer, 0, sizeof(Object));

        DVLOG(2) << "[bench] client coro " << ctx << " start to read";
        CHECK_LT(sizeof(Object), rdma_buf.size);
        auto ec = p->read(lease,
                          rdma_buf.buffer,
                          sizeof(Object),
                          0 /* offset */,
                          0 /* flag */,
                          &ctx);
        CHECK_EQ(ec, RetCode::kOk)
            << "[bench] client coro " << ctx
            << " read FAILED. This should not happen, because we "
               "filter out the invalid mws.";

        DVLOG(2) << "[bench] client coro " << ctx << " read finished";
        Object magic_object = *(Object *) rdma_buf.buffer;
        CHECK_EQ(magic_object.target, coro_magic)
            << "coro_id " << ctx << ", Read at key " << coro_key
            << ", lease.base: " << (void *) lease.base_addr();

        DVLOG(2) << "[bench] client coro " << ctx
                 << " start to relinquish lease ";
        p->relinquish(lease, 0 /* hint */, 0 /* flag */, &ctx);

        p->put_rdma_buffer(rdma_buf);

        DVLOG(2) << "[bench] client coro " << ctx << " finished current task.";
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
    auto tid = p->get_thread_id();

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

    p->finished(kWaitFlag);
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
    //

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

    p->server_serve(kWaitFlag);
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    bindCore(0);

    rdmaQueryDevice();

    PatronusConfig config;
    config.machine_nr = kMachineNr;

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
        patronus->finished(kWaitFlag);
        server(patronus);
    }

    LOG(INFO) << "finished. ctrl+C to quit.";
}