#include <glog/logging.h>

#include <algorithm>
#include <random>

#include "DSM.h"
#include "Timer.h"
#include "util/monitor.h"

// Two nodes
// one node issues cas operations

constexpr uint16_t kClientNodeId = 0;
[[maybe_unused]] constexpr uint16_t kServerNodeId = 1;
constexpr uint32_t kMachineNr = 2;

// constexpr static uint64_t kMagic = 0xaabbccddeeff0011;

// constexpr static size_t kRpcNr = 100;
// constexpr static size_t kMsgSize = 16;

constexpr static size_t kPingpoingCnt = 100 * define::K;
constexpr static size_t kBurnCnt = 20 * define::M;
constexpr static size_t kThreadNr = RMSG_MULTIPLEXING;
// constexpr static size_t kThreadNr = 1;
// constexpr static size_t kBenchMsgSize = 16;

struct BenchMsg
{
    uint16_t mid;
} __attribute__((packed));

void client_pingpong_correct(std::shared_ptr<DSM> dsm)
{
    auto *buf = dsm->get_rdma_buffer();
    char recv_buf[1024];
    for (size_t i = 0; i < kPingpoingCnt; ++i)
    {
        uint64_t magic = rand();
        *(uint64_t *) buf = magic;
        dsm->reliable_send(buf, sizeof(uint64_t), kServerNodeId, 0);

        dsm->reliable_recv(0, recv_buf);
        uint64_t get = *(uint64_t *) recv_buf;
        CHECK_EQ(get, magic)
            << "Pingpoing content mismatch for " << i << "-th test";
    }
}

constexpr static size_t kTokenNr = 2;

void client_burn(std::shared_ptr<DSM> dsm, size_t thread_nr)
{
    std::vector<std::thread> threads;

    // @kTokenNr token, each of which get ReliableConnection::kRecvBuffer / @kTokenNr.
    std::array<std::atomic<int64_t>, RMSG_MULTIPLEXING> continue_token;
    for (size_t i = 0; i < RMSG_MULTIPLEXING; ++i)
    {
        continue_token[i] = kTokenNr;
    }
    constexpr size_t msg_each_token = ReliableConnection::kPostRecvBufferBatch / kTokenNr;
    CHECK_EQ(ReliableConnection::kPostRecvBufferBatch % kTokenNr, 0);

    Timer t;
    t.begin();
    for (size_t i = 0; i < thread_nr; ++i)
    {
        threads.emplace_back(
            [dsm, i, &continue_token]()
            {
                bindCore(i + 1);
                dsm->registerThread();
                auto tid = dsm->get_thread_id();
                auto mid = tid % RMSG_MULTIPLEXING;

                auto *buf = dsm->get_rdma_buffer();
                auto *send_msg = (BenchMsg *) buf;
                send_msg->mid = mid;

                size_t sent = 0;
                char buffer[ReliableConnection::kMessageSize * 64];
                for (size_t t = 0; t < kBurnCnt; ++t)
                {
                    dsm->reliable_send( 
                        buf, sizeof(BenchMsg), kServerNodeId, mid);
                    sent++;
                    if (sent % msg_each_token == 0)
                    {
                        continue_token[mid].fetch_sub(1);
                        // auto now = std::chrono::steady_clock::now();
                        VLOG(3)
                            << "[wait] tid " << tid << " sent " << sent
                            << " at " << msg_each_token << ", wait for ack.";
                        do
                        {
                            size_t recv_nr = dsm->reliable_try_recv(mid, buffer, 64);
                            // handle possbile recv token
                            for (size_t r = 0; r < recv_nr; ++r)
                            {
                                void *msg_addr =
                                    buffer +
                                    ReliableConnection::kMessageSize * r;
                                auto *recv_msg = (BenchMsg *) msg_addr;
                                VLOG(3) << "[wait] tid " << tid
                                         << " recv continue msg for mid "
                                         << recv_msg->mid << ". add one token";
                                continue_token[recv_msg->mid].fetch_add(1);
                            }
                        } while (continue_token[mid] <= 0);
                        VLOG(3) << "[wait] tid " << tid << " from mid " << mid
                                 << " has enough token. current: "
                                 << continue_token[mid];
                    }
                }
            });
    }
    for (auto &t : threads)
    {
        t.join();
    }

    auto ns = t.end();
    auto op = thread_nr * kBurnCnt;
    double ops = 1e9 * op / ns;
    LOG(INFO) << "count: " << op << ", ns: " << ns << ", ops: " << ops
              << ", with thread " << thread_nr
              << ". ops/thread: " << (ops / thread_nr);
}
void server_burn(std::shared_ptr<DSM> dsm,
                 size_t total_msg_nr,
                 size_t thread_nr)
{
    std::vector<std::thread> threads;
    std::atomic<size_t> got{0};
    // std::array<std::atomic<int64_t>, RMSG_MULTIPLEXING> recv_mid_msgs{};

    std::array<std::atomic<int64_t>, RMSG_MULTIPLEXING> recv_mid_msgs;

    for (size_t i = 0; i < thread_nr; ++i)
    {
        threads.emplace_back(
            [dsm, i, &got, total_msg_nr, &recv_mid_msgs]()
            {
                bindCore(i + 1);
                dsm->registerThread();
                auto tid = dsm->get_thread_id();
                auto mid = tid % RMSG_MULTIPLEXING;

                char buffer[ReliableConnection::kMessageSize * 64];
                auto *rdma_buf = dsm->get_rdma_buffer();
                memset(rdma_buf, 0, sizeof(BenchMsg));
                while (got.load() < total_msg_nr)
                // while (true)
                {
                    auto get = dsm->reliable_try_recv(mid, buffer, 64);
                    got.fetch_add(get);
                    for (size_t i = 0; i < get; ++i)
                    {
                        auto *recv_msg =
                            (BenchMsg *) ((char *) buffer +
                                          ReliableConnection::kMessageSize * i);
                        auto now = recv_mid_msgs[recv_msg->mid].fetch_add(1) + 1;
                        BenchMsg *send_msg = (BenchMsg *) rdma_buf;
                        memcpy(send_msg, recv_msg, sizeof(BenchMsg));
                        constexpr size_t credit_for_token =
                            ReliableConnection::kPostRecvBufferBatch / kTokenNr;
                        if (now % credit_for_token == 0)
                        {
                            recv_mid_msgs[recv_msg->mid].fetch_sub(credit_for_token);
                            VLOG(3) << "[wait] server tid " << tid
                                    << " let go mid " << recv_msg->mid;
                            dsm->reliable_send((char *) send_msg,
                                               sizeof(BenchMsg),
                                               kClientNodeId,
                                               mid);
                        }
                    }
                    // VLOG(3) << "[bench] get " << get << " for tid " << tid;
                }
            });
    }
    for (auto &t : threads)
    {
        t.join();
    }
}
void server_pingpong_correct(std::shared_ptr<DSM> dsm)
{
    char recv_buf[1024];
    auto *buf = dsm->get_rdma_buffer();
    for (size_t i = 0; i < kPingpoingCnt; ++i)
    {
        dsm->reliable_recv(0, recv_buf, 0);
        uint64_t get = *(uint64_t *) recv_buf;

        *(uint64_t *) buf = get;
        dsm->reliable_send(buf, sizeof(uint64_t), kClientNodeId, 0);
    }
}

void client_wait(std::shared_ptr<DSM> dsm)
{
    // sync
    auto *buf = dsm->get_rdma_buffer();
    dsm->reliable_recv(0, nullptr);
    dsm->reliable_send(buf, 0, kServerNodeId, 0);
}

void client(std::shared_ptr<DSM> dsm)
{
    // client_pingpong_correct(dsm);
    LOG(INFO) << "Begin burn";
    client_burn(dsm, kThreadNr);

    client_wait(dsm);
    LOG(INFO) << "Exiting!!!";
}

void server_wait(std::shared_ptr<DSM> dsm)
{
    auto *buffer = dsm->get_rdma_buffer();
    dsm->reliable_send(buffer, 0, kClientNodeId, 0);
    dsm->reliable_recv(0, nullptr);
}

void server(std::shared_ptr<DSM> dsm)
{
    // server_pingpong_correct(dsm);

    size_t expect_msg_nr = kThreadNr * kBurnCnt;
    LOG(INFO) << "Begin burn";
    server_burn(dsm, expect_msg_nr, kThreadNr);

    server_wait(dsm);
    LOG(INFO) << "Exiting!!!";
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    bindCore(0);

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