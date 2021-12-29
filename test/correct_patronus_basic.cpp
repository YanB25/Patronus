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
thread_local bool workers_finished[kCoroCnt];
thread_local CoroCall master;

constexpr static uint64_t kMagic = 0xaabbccdd11223344;
constexpr static size_t kOffset = 1024;
constexpr static size_t kDirID = 0;

void client_worker(Patronus::pointer p, coro_t coro_id, CoroYield &yield)
{
    CoroContext ctx;
    ctx.yield = &yield;
    ctx.coro_id = coro_id;
    ctx.master = &master;

    size_t coro_offset = kOffset + coro_id * sizeof(kMagic);
    size_t coro_magic = kMagic + coro_id;

    Lease lease = p->get_rlease(kServerNodeId,
                                kDirID,
                                coro_offset /* key */,
                                sizeof(kMagic),
                                100,
                                &ctx);
    LOG(WARNING) << "[bench] client coro " << (int) coro_id << " got lease "
                 << lease;

    auto rdma_buf = p->get_rdma_buffer();
    LOG(WARNING) << "[bench] client coro " << (int) coro_id << " start to read";
    CHECK_LT(sizeof(kMagic), rdma_buf.size);
    p->read(
        lease, rdma_buf.buffer, sizeof(kMagic), 0 /* offset */, kDirID, &ctx);
    LOG(WARNING) << "[bench] client coro " << (int) coro_id << " read finished";
    uint64_t magic = *(uint64_t *) rdma_buf.buffer;
    CHECK_EQ(magic, coro_magic) << "coro_id " << (int) coro_id
                                << ", Read at offset " << (void *) coro_offset;

    p->put_rdma_buffer(rdma_buf.buffer);
    LOG(WARNING) << "worker coro " << (int) coro_id
                 << " finished. yield to master.";

    workers_finished[coro_id] = true;
    yield(master);
}
void client_master(Patronus::pointer p, CoroYield &yield)
{
    auto tid = p->get_thread_id();
    auto mid = tid;

    for (size_t i = 0; i < kCoroCnt; ++i)
    {
        workers_finished[i] = false;

        yield(workers[i]);
    }
    LOG(INFO) << "Return back to master. start to recv messages";
    char buf[ReliableConnection::kMaxRecvBuffer];
    coro_t coro_buf[2 * kCoroCnt];
    while (!std::all_of(std::begin(workers_finished),
                        std::end(workers_finished),
                        [](bool i) { return i; }))
    {
        // try to see if messages arrived
        auto nr =
            p->reliable_try_recv(mid, buf, ReliableConnection::kRecvLimit);
        VLOG_IF(1, nr > 0) << "[bench] client recv messages nr : " << nr;
        p->handle_response_messages(buf, nr);
        for (size_t i = 0; i < nr; ++i)
        {
            auto *msg_start = buf + ReliableConnection::kMessageSize * i;
            auto *recv_msg = (BaseMessage *) msg_start;
            auto coro_id = recv_msg->cid.coro_id;
            VLOG(1) << "[bench] yielding to coro " << (int) coro_id;
            yield(workers[coro_id]);
        }
        // try to see if read/write finished

        nr = p->try_get_rdma_finished_coros(coro_buf, 2 * kCoroCnt);
        for (size_t i = 0; i < nr; ++i)
        {
            auto coro_id = coro_buf[i];
            yield(workers[coro_id]);
        }
    }
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
    auto mid = tid;

    LOG(INFO) << "I am server. tid " << tid;

    auto dsm = p->get_dsm();
    auto internal_buf = dsm->get_server_internal_buffer();
    for (size_t i = 0; i < kCoroCnt; ++i)
    {
        auto coro_magic = kMagic + i;
        auto coro_offset = kOffset + sizeof(kMagic) * i;

        auto *server_internal_buf = internal_buf.buffer;
        uint64_t *where = (uint64_t *) &server_internal_buf[coro_offset];
        *where = coro_magic;

        VLOG(1) << "[bench] server setting " << coro_magic << " to "
                << coro_offset << ". actual addr: " << (void *) where
                << " for coro " << i;
    }

    while (true)
    {
        char buf[ReliableConnection::kMaxRecvBuffer];
        size_t nr =
            p->reliable_try_recv(mid, buf, ReliableConnection::kRecvLimit);
        p->handle_request_messages(buf, nr);
    }
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

    sleep(1);

    patronus->registerThread();

    // let client spining
    auto nid = patronus->get_node_id();
    if (nid == kClientNodeId)
    {
        sleep(2);
        client(patronus);
    }
    else
    {
        server(patronus);
    }

    LOG(INFO) << "finished. ctrl+C to quit.";
}