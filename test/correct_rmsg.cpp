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
// constexpr static size_t kBurnCnt = 100 * define::M;

constexpr static size_t kBenchMessageBufSize = 16;
struct BenchMessage
{
    uint16_t from_node;
    uint16_t from_mid;
    uint8_t size;
    char buf[kBenchMessageBufSize];
    uint64_t digest;
} __attribute__((packed));

// the kMessageSize from ReliableConnection
static_assert(sizeof(BenchMessage) < 32);

void check_valid(const BenchMessage &msg)
{
    CHECK_LT(msg.from_node, kMachineNr);
    CHECK_LE(msg.from_mid, RMSG_MULTIPLEXING);
    CHECK_LE(msg.size, kBenchMessageBufSize);
    auto calculated_dg = djb2_digest(msg.buf, msg.size);
    CHECK_EQ(msg.digest, calculated_dg)
        << "Digest mismatch! Expect " << std::hex << calculated_dg << ", got "
        << msg.digest;
}


template <class T, class... Args>
inline T &TLS(Args &&...args)
{
    thread_local T _tls_item(std::forward<Args>(args)...);
    return _tls_item;
}

inline std::mt19937 &rand_generator()
{
    return TLS<std::mt19937>();
}

uint64_t rand_int(uint64_t min, uint64_t max)
{
    std::uniform_int_distribution<uint64_t> dist(min, max);
    return dist(rand_generator());
}

void client_varsize_correct(std::shared_ptr<DSM> dsm, size_t mid)
{
    LOG(WARNING) << "[bench] testing varsize for mid = " << mid;
    auto *buf = dsm->get_rdma_buffer();
    char recv_buf[1024];

    for (size_t i = 0; i < kPingpoingCnt; ++i)
    {
        auto size = rand_int(0, 32);
        for (size_t i = 0; i < size; ++i)
        {
            buf[i] = rand();
            recv_buf[i] = 0;
        }
        dsm->reliable_send(buf, size, kServerNodeId, mid);

        dsm->reliable_recv(recv_buf);
        if (memcmp(recv_buf, buf, size) != 0)
        {
            LOG(ERROR) << "Mismatch result at " << i << "-th test. size "
                       << size;
            for (size_t b = 0; b < size; ++b)
            {
                LOG_IF(ERROR, recv_buf[b] != buf[b])
                    << "mismatch at " << b << "-th byte. expect " << std::hex
                    << (uint64_t) buf[b] << ", got " << (uint64_t) recv_buf[b];
            }
        }
        CHECK_EQ(memcmp(recv_buf, buf, size), 0)
            << "Pingpoing content mismatch for " << i << "-th test. size "
            << size << ", recv_buf " << *(uint64_t *) recv_buf << ", buf "
            << *(uint64_t *) buf;
    }
}
void server_varsize_correct(std::shared_ptr<DSM> dsm, size_t mid)
{
    char recv_buf[1024];
    auto *buf = dsm->get_rdma_buffer();
    for (size_t i = 0; i < kPingpoingCnt; ++i)
    {
        dsm->reliable_recv(recv_buf);
        memcpy(buf, recv_buf, 32);
        dsm->reliable_send(buf, 32, kClientNodeId, mid);
    }
}

void client_pingpong_correct(std::shared_ptr<DSM> dsm, size_t mid)
{
    LOG(WARNING) << "[bench] testing mid = " << mid;
    auto *buf = dsm->get_rdma_buffer();
    char recv_buf[1024];
    for (size_t i = 0; i < kPingpoingCnt; ++i)
    {
        uint64_t magic = rand();
        DVLOG(3) << "the " << i << "-th test is " << std::hex << magic;
        *(uint64_t *) buf = magic;
        dsm->reliable_send(buf, sizeof(uint64_t), kServerNodeId, mid);

        dsm->reliable_recv(recv_buf);
        uint64_t get = *(uint64_t *) recv_buf;
        CHECK_EQ(get, magic)
            << "Pingpoing content mismatch for " << i << "-th test. expect "
            << std::hex << magic << ", got " << get;
    }
}
void server_pingpong_correct(std::shared_ptr<DSM> dsm, size_t mid)
{
    char recv_buf[1024];
    auto *buf = dsm->get_rdma_buffer();
    for (size_t i = 0; i < kPingpoingCnt; ++i)
    {
        dsm->reliable_recv(recv_buf);
        uint64_t get = *(uint64_t *) recv_buf;
        DVLOG(3) << "[bench] server got " << std::hex << get << " for " << i
                 << "-th test";

        *(uint64_t *) buf = get;
        dsm->reliable_send(buf, sizeof(uint64_t), kClientNodeId, mid);
    }
}


void server_multithread(std::shared_ptr<DSM> dsm, size_t thread_nr)
{
    std::vector<std::thread> threads;
    for (size_t i = 0; i < thread_nr; ++i)
    {
        threads.emplace_back(
            [i, dsm]()
            {
                bindCore(1 + i);
                dsm->registerThread();

                char buffer[1024];
                auto *rdma_buf = dsm->get_rdma_buffer();
                for (size_t time = 0; time < kPingpoingCnt; ++time)
                {
                    dsm->reliable_recv(buffer);
                    auto *msg = (BenchMessage *) buffer;

                    check_valid(*msg);

                    memcpy(rdma_buf, buffer, sizeof(BenchMessage));
                    dsm->reliable_send(rdma_buf,
                                       sizeof(BenchMessage),
                                       msg->from_node,
                                       msg->from_mid);
                }
            });
    }
    for (auto &t : threads)
    {
        t.join();
    }
}

void client_multithread(std::shared_ptr<DSM> dsm, size_t thread_nr)
{
    LOG(WARNING) << "[bench] testing multithread for thread = " << thread_nr;
    std::vector<std::thread> threads;
    for (size_t i = 0; i < thread_nr; ++i)
    {
        threads.emplace_back(
            [i, dsm]()
            {
                bindCore(1 + i);
                dsm->registerThread();

                auto tid = dsm->get_thread_id();
                auto from_mid = tid % RMSG_MULTIPLEXING;

                char buffer[1024];

                auto *rdma_buf = dsm->get_rdma_buffer();
                BenchMessage *msg = (BenchMessage *) rdma_buf;
                for (size_t time = 0; time < kPingpoingCnt; ++time)
                {
                    msg->size = rand_int(0, kBenchMessageBufSize);
                    for (size_t s = 0; s < msg->size; ++s)
                    {
                        msg->buf[s] = rand();
                    }
                    msg->from_node = kClientNodeId;
                    CHECK_NE(tid, 0) << "The tid == 0 is reserved. So we (tid "
                                        "- 1) at the below line.";
                    msg->from_mid = from_mid;
                    msg->digest = djb2_digest(msg->buf, msg->size);
                    dsm->reliable_send(rdma_buf,
                                       sizeof(BenchMessage),
                                       kServerNodeId,
                                       from_mid);

                    dsm->reliable_recv(buffer);
                    auto *recv_msg = (BenchMessage *) buffer;
                    check_valid(*recv_msg);
                }
            });
    }
    for (auto &t : threads)
    {
        t.join();
    }
}

void client_wait(std::shared_ptr<DSM> dsm)
{
    // sync
    auto *buf = dsm->get_rdma_buffer();
    dsm->reliable_recv(nullptr);
    dsm->reliable_send(buf, 0, kServerNodeId, 0);
}

constexpr static size_t kMultiThreadNr = 16;
static_assert(kMultiThreadNr < MAX_APP_THREAD);

void client(std::shared_ptr<DSM> dsm)
{
    // client_pingpong_correct(dsm);
    LOG(INFO) << "Begin burn";
    for (size_t i = 0; i < RMSG_MULTIPLEXING; ++i)
    {
        client_pingpong_correct(dsm, i);

        client_varsize_correct(dsm, i);
    }

    client_multithread(dsm, kMultiThreadNr);

    client_wait(dsm);
    LOG(INFO) << "ALL TEST PASSED";
}

void server_wait(std::shared_ptr<DSM> dsm)
{
    auto *buffer = dsm->get_rdma_buffer();
    dsm->reliable_send(buffer, 0, kClientNodeId, 0);
    dsm->reliable_recv(nullptr);
}

void server(std::shared_ptr<DSM> dsm)
{
    // server_pingpong_correct(dsm);

    LOG(INFO) << "Begin burn";
    for (size_t i = 0; i < RMSG_MULTIPLEXING; ++i)
    {
        server_pingpong_correct(dsm, i);

        server_varsize_correct(dsm, i);
    }

    server_multithread(dsm, kMultiThreadNr);

    server_wait(dsm);
    LOG(INFO) << "ALL TEST PASSED";
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