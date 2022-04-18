#include <glog/logging.h>

#include <algorithm>
#include <random>

#include "DSM.h"
#include "Timer.h"
#include "util/monitor.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

// Two nodes
// one node issues cas operations

constexpr uint16_t kClientNodeId = 0;
[[maybe_unused]] constexpr uint16_t kServerNodeId = 1;
constexpr uint32_t kMachineNr = 2;

// constexpr static uint64_t kMagic = 0xaabbccddeeff0011;

// constexpr static size_t kRpcNr = 100;
// constexpr static size_t kMsgSize = 16;

constexpr static size_t kPingpoingCnt = 100 * define::K;
constexpr static size_t kBurnCnt = 1 * define::M;

constexpr static size_t kBenchMessageBufSize = 16;

// constexpr static size_t kMultiThreadNr = 16;
constexpr static size_t kClientThreadNr = RMSG_MULTIPLEXING - 1;
constexpr static size_t kServerThreadNr = RMSG_MULTIPLEXING - 1;
static_assert(kClientThreadNr < kMaxAppThread);
static_assert(kServerThreadNr < kMaxAppThread);
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
inline T &TLS(Args &&... args)
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
    auto *buf = dsm->get_rdma_buffer().buffer;
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

        dsm->reliable_recv(mid, recv_buf);
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
    auto *buf = dsm->get_rdma_buffer().buffer;
    for (size_t i = 0; i < kPingpoingCnt; ++i)
    {
        dsm->reliable_recv(mid, recv_buf);
        memcpy(buf, recv_buf, 32);
        dsm->reliable_send(buf, 32, kClientNodeId, mid);
    }
}

void client_pingpong_correct(std::shared_ptr<DSM> dsm, size_t mid)
{
    LOG(WARNING) << "[bench] testing mid = " << mid;
    auto *buf = dsm->get_rdma_buffer().buffer;
    char recv_buf[1024];
    for (size_t i = 0; i < kPingpoingCnt; ++i)
    {
        uint64_t magic = rand();
        DVLOG(3) << "the " << i << "-th test is " << std::hex << magic;
        *(uint64_t *) buf = magic;
        dsm->reliable_send(buf, sizeof(uint64_t), kServerNodeId, mid);

        dsm->reliable_recv(mid, recv_buf);
        uint64_t get = *(uint64_t *) recv_buf;
        CHECK_EQ(get, magic)
            << "Pingpoing content mismatch for " << i << "-th test. expect "
            << std::hex << magic << ", got " << get;
    }
}
void server_pingpong_correct(std::shared_ptr<DSM> dsm, size_t mid)
{
    char recv_buf[1024];
    auto *buf = dsm->get_rdma_buffer().buffer;
    for (size_t i = 0; i < kPingpoingCnt; ++i)
    {
        dsm->reliable_recv(mid, recv_buf);
        uint64_t get = *(uint64_t *) recv_buf;
        DVLOG(3) << "[bench] server got " << std::hex << get << " for " << i
                 << "-th test";

        *(uint64_t *) buf = get;
        dsm->reliable_send(buf, sizeof(uint64_t), kClientNodeId, mid);
    }
}

void server_multithread(std::shared_ptr<DSM> dsm,
                        size_t total_nr,
                        size_t thread_nr)
{
    std::vector<std::thread> threads;
    std::atomic<size_t> finished_nr{0};

    // std::array<std::atomic<int64_t>, RMSG_MULTIPLEXING> recv_mid_msgs{};

    for (size_t i = 0; i < thread_nr; ++i)
    {
        threads.emplace_back([dsm, &finished_nr, total_nr]() {
            dsm->registerThread();
            auto tid = dsm->get_thread_id() - 1;
            // auto mid = tid % kServerThreadNr;
            auto mid = tid;

            char buffer[102400];
            auto *rdma_buf = dsm->get_rdma_buffer().buffer;

            size_t recv_msg_nr = 0;
            while (finished_nr < total_nr)
            {
                size_t get = dsm->reliable_try_recv(mid, buffer, 64);
                finished_nr.fetch_add(get);
                for (size_t i = 0; i < get; ++i)
                {
                    auto *recv_msg =
                        (BenchMessage *) ((char *) buffer +
                                          ReliableConnection::kMessageSize * i);
                    check_valid(*recv_msg);

                    recv_msg_nr++;
                    BenchMessage *send_msg = (BenchMessage *) rdma_buf;
                    memcpy(send_msg, recv_msg, sizeof(BenchMessage));
                    if (recv_msg_nr % 64 == 0)
                    {
                        recv_msg_nr -= 64;
                        VLOG(3) << "[wait] server tid " << tid << " let go mid "
                                << recv_msg->from_mid;
                        dsm->reliable_send((char *) send_msg,
                                           sizeof(BenchMessage),
                                           kClientNodeId,
                                           mid);
                    }
                }
            }
        });
    }
    for (auto &t : threads)
    {
        t.join();
    }
}

// constexpr static ssize_t kMidOffset = -1;

// we can not reserve mid == 0 in this situation, because we set it to thread
// safe.
void client_multithread(std::shared_ptr<DSM> dsm, size_t thread_nr)
{
    LOG(WARNING) << "[bench] testing multithread for thread = " << thread_nr;
    std::vector<std::thread> threads;

    // std::array<std::atomic<bool>, RMSG_MULTIPLEXING> can_continue_{};
    for (size_t i = 0; i < thread_nr; ++i)
    {
        threads.emplace_back([dsm]() {
            dsm->registerThread();

            auto tid = dsm->get_thread_id() - 1;
            auto from_mid = tid;
            CHECK_EQ(tid, from_mid) << "Currently we ensure that tid == mid";

            size_t sent = 0;

            char buffer[1024];

            auto *rdma_buf = dsm->get_rdma_buffer().buffer;
            BenchMessage *msg = (BenchMessage *) rdma_buf;
            for (size_t time = 0; time < kBurnCnt; ++time)
            {
                msg->size = rand_int(0, kBenchMessageBufSize);
                for (size_t s = 0; s < msg->size; ++s)
                {
                    msg->buf[s] = rand();
                }
                msg->from_node = kClientNodeId;
                msg->from_mid = from_mid;
                msg->digest = djb2_digest(msg->buf, msg->size);
                dsm->reliable_send(
                    rdma_buf, sizeof(BenchMessage), kServerNodeId, from_mid);
                sent++;
                if (sent % 64 == 0)
                {
                    DVLOG(3)
                        << "[wait] tid " << tid << " sent 64. wait for ack. ";
                    dsm->reliable_recv(from_mid, buffer);
                    auto *recv_msg = (BenchMessage *) buffer;
                    DVLOG(3)
                        << "[wait] tid " << tid << " recv continue msg for mid "
                        << recv_msg->from_mid;
                }

                if (time % (100 * define::K) == 0)
                {
                    LOG(WARNING) << "[bench] client tid " << tid
                                 << " finish 100K. time: " << time;
                }
            }
        });
    }
    for (auto &t : threads)
    {
        t.join();
    }
}

void client(std::shared_ptr<DSM> dsm)
{
    // client_pingpong_correct(dsm);
    LOG(INFO) << "Begin burn";
    auto tid = dsm->get_thread_id();

    client_pingpong_correct(dsm, tid);

    client_varsize_correct(dsm, tid);

    client_multithread(dsm, kClientThreadNr);

    dsm->barrier("finished", 10ms);
    LOG(INFO) << "ALL TEST PASSED";
}

void server(std::shared_ptr<DSM> dsm)
{
    // server_pingpong_correct(dsm);

    LOG(INFO) << "Begin burn";
    auto tid = dsm->get_thread_id();

    server_pingpong_correct(dsm, tid);

    server_varsize_correct(dsm, tid);

    size_t expect_work = kClientThreadNr * kBurnCnt;
    server_multithread(dsm, expect_work, kServerThreadNr);

    dsm->barrier("finished", 10ms);
    LOG(INFO) << "ALL TEST PASSED";
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

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