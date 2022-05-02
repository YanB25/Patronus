#include <algorithm>
#include <random>

#include "DSM.h"
#include "Timer.h"
#include "gflags/gflags.h"
#include "util/monitor.h"

// Two nodes
// one node issues cas operations

DEFINE_string(exec_meta, "", "The meta data of this execution");

constexpr uint16_t kClientNodeId = 0;
[[maybe_unused]] constexpr uint16_t kServerNodeId = 1;
constexpr uint32_t kMachineNr = 2;

constexpr static size_t kCoroCnt = 8;
thread_local CoroCall workers[kCoroCnt];
thread_local CoroCall master;

constexpr static size_t kMsgNr = 1 * define::K;

void do_worker(std::shared_ptr<DSM> dsm, CoroYield &yield, coro_t coro_id)
{
    auto tid = dsm->get_thread_id();
    LOG(INFO) << "Enter do_worker. My work finished. I am tid " << tid
              << ", coro_id " << (int) coro_id;
    yield(master);
}

void do_master(std::shared_ptr<DSM> dsm, CoroYield &yield)
{
    auto tid = dsm->get_thread_id();
    LOG(INFO) << "Enter master. tid " << tid << " starting to yield...";
    for (size_t i = 0; i < kCoroCnt; ++i)
    {
        LOG(INFO) << "Yielding to worker " << i;
        yield(workers[i]);
    }

    LOG(INFO) << "master exit...";
}

struct CID
{
    union {
        struct
        {
            uint16_t node_id;
            uint16_t thread_id;
            uint8_t mid;
            coro_t coro_id;
        } __attribute__((packed));
        uint64_t cid;
    };
} __attribute__((packed));

struct EchoMessage
{
    CID cid;
    uint64_t val;
} __attribute__((packed));

size_t send_nr = 0;
bool client_finish = false;

void client_worker(std::shared_ptr<DSM> dsm,
                   coro_t coro_id,
                   CoroYield &yield,
                   std::queue<void *> &messages)
{
    auto tid = dsm->get_thread_id();
    auto mid = tid;

    CoroContext ctx(tid, &yield, &master, coro_id);

    auto *rdma_buf = dsm->get_rdma_buffer().buffer;
    auto *my_buf = rdma_buf + ReliableConnection::kMessageSize * coro_id;
    auto *send_msg = (EchoMessage *) my_buf;
    send_msg->cid.node_id = kClientNodeId;
    send_msg->cid.mid = mid;
    send_msg->cid.thread_id = tid;
    send_msg->cid.coro_id = coro_id;
    send_msg->val = rand();

    for (size_t i = 0; i < kMsgNr; ++i)
    {
        VLOG(2) << "[bench] client tid " << tid << " mid " << mid << " coro "
                << (int) coro_id << " sending val " << send_msg->val;

        dsm->reliable_send(
            (char *) send_msg, sizeof(EchoMessage), kServerNodeId, tid);
        yield(master);
        CHECK(!messages.empty());
        void *recv_msg_addr = messages.front();
        messages.pop();
        auto *recv_msg = (EchoMessage *) recv_msg_addr;
        CHECK_EQ(recv_msg->val, send_msg->val + 1);
        send_nr++;
        if (send_nr == kCoroCnt * kMsgNr)
        {
            client_finish = true;
        }
    }
    LOG(WARNING) << "[bench] tid " << tid << " coro " << (int) coro_id
                 << " exit. sent " << kMsgNr << ". go back to server";
    yield(master);
}

void client_master(std::shared_ptr<DSM> dsm,
                   CoroYield &yield,
                   std::vector<std::queue<void *>> &msg_queues)
{
    auto tid = dsm->get_thread_id();
    auto mid = tid;

    size_t expect_nr = kMsgNr * kCoroCnt;
    size_t recv_nr = 0;

    for (size_t i = 0; i < kCoroCnt; ++i)
    {
        yield(workers[i]);
    }

    while (recv_nr < expect_nr)
    {
        char buffer[ReliableConnection::kMessageSize *
                    ReliableConnection::kRecvLimit];
        size_t nr =
            dsm->reliable_try_recv(mid, buffer, ReliableConnection::kRecvLimit);
        recv_nr += nr;
        VLOG_IF(1, nr > 0) << "[bench] client tid " << tid
                           << " coro master recv_nr: " << recv_nr;
        for (size_t i = 0; i < nr; ++i)
        {
            auto *base_addr = buffer + ReliableConnection::kMessageSize * i;
            VLOG(2) << "[bench] client tid " << tid << " mid " << mid
                    << " coro: master. recv from addr " << (void *) base_addr;
            auto *recv_msg = (EchoMessage *) base_addr;
            auto coro_id = recv_msg->cid.coro_id;
            CHECK_LT(coro_id, msg_queues.size());
            msg_queues[coro_id].push(recv_msg);
        }
        for (size_t i = 0; i < msg_queues.size(); ++i)
        {
            if (!msg_queues[i].empty())
            {
                yield(workers[i]);
            }
        }
    }
    LOG(WARNING) << "recv_nr " << recv_nr << ", expect " << expect_nr
                 << ". master exit";
}

void client(std::shared_ptr<DSM> dsm)
{
    std::vector<std::queue<void *>> msg_queues;
    msg_queues.resize(kCoroCnt);

    for (size_t i = 0; i < kCoroCnt; ++i)
    {
        workers[i] = CoroCall([dsm, i, &msg_queues](CoroYield &yield) {
            client_worker(dsm, i, yield, msg_queues[i]);
        });
    }

    master = CoroCall([dsm, &msg_queues](CoroYield &yield) {
        client_master(dsm, yield, msg_queues);
    });

    master();
}
void server(std::shared_ptr<DSM> dsm)
{
    auto tid = dsm->get_thread_id();
    auto mid = tid;
    auto *rdma_buffer = dsm->get_rdma_buffer().buffer;

    size_t expect_nr = kCoroCnt * kMsgNr;
    size_t recv_nr = 0;
    while (recv_nr < expect_nr)
    {
        char buffer[ReliableConnection::kMessageSize *
                    ReliableConnection::kRecvLimit];
        size_t nr =
            dsm->reliable_try_recv(mid, buffer, ReliableConnection::kRecvLimit);
        recv_nr += nr;
        VLOG_IF(1, nr > 0) << "[bench] server recv from mid " << mid
                           << " current cnt: " << recv_nr;
        for (size_t i = 0; i < nr; ++i)
        {
            char *base_addr = buffer + ReliableConnection::kMessageSize * i;
            auto *recv_msg = (EchoMessage *) base_addr;
            auto *send_buffer_addr =
                rdma_buffer + ReliableConnection::kMessageSize * i;
            memcpy(send_buffer_addr, base_addr, sizeof(EchoMessage));
            auto *send_msg = (EchoMessage *) send_buffer_addr;
            send_msg->val += 1;
            CHECK_LT(recv_msg->cid.coro_id, kCoroCnt);
            VLOG(2) << "[bench] server recv from mid " << mid << ", coro "
                    << (int) recv_msg->cid.coro_id << ", val " << recv_msg->val;
            dsm->reliable_send((char *) send_msg,
                               sizeof(EchoMessage),
                               kClientNodeId,
                               recv_msg->cid.mid);
        }
    }
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    VLOG(1) << "It is 1 vlog";
    VLOG(2) << "It is 2 vlog";
    VLOG(3) << "It is 3 vlog";
    LOG(INFO) << "Support color ? " << getenv("TERM");

    rdmaQueryDevice();

    DSMConfig config;
    config.machineNR = kMachineNr;

    auto dsm = DSM::getInstance(config);

    sleep(1);

    dsm->registerThread();

    // let client spining
    auto nid = dsm->getMyNodeID();
    if (nid == kClientNodeId)
    {
        client(dsm);
    }
    else
    {
        server(dsm);
    }

    LOG(INFO) << "finished. ctrl+C to quit.";
}