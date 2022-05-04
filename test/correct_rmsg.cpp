#include <glog/logging.h>

#include <algorithm>
#include <random>

#include "DSM.h"
#include "Timer.h"
#include "gflags/gflags.h"
#include "umsg/Config.h"
#include "util/Rand.h"
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
constexpr static size_t kClientThreadNr = kMaxAppThread;
constexpr static size_t kServerThreadNr = NR_DIRECTORY;
static_assert(kClientThreadNr <= kMaxAppThread);
static_assert(kServerThreadNr <= kMaxAppThread);
struct BenchMessage
{
    uint16_t from_node;
    uint16_t from_tid;
    uint8_t size;
    char buf[kBenchMessageBufSize];
    uint64_t digest;
} __attribute__((packed));

// the kMessageSize from ReliableConnection
static_assert(sizeof(BenchMessage) < 32);

void check_valid(const BenchMessage &msg)
{
    CHECK_LT(msg.from_node, kMachineNr);
    CHECK_LE(msg.size, kBenchMessageBufSize);
    auto calculated_dg = djb2_digest(msg.buf, msg.size);
    CHECK_EQ(msg.digest, calculated_dg)
        << "Digest mismatch! Expect " << std::hex << calculated_dg << ", got "
        << msg.digest;
}

void client_varsize_correct(std::shared_ptr<DSM> dsm, size_t dir_id)
{
    LOG(WARNING) << "[bench] testing varsize for dir_id = " << dir_id;
    auto *buf = dsm->get_rdma_buffer().buffer;
    char recv_buf[1024];

    auto &bench_msg = *(BenchMessage *) buf;
    for (size_t i = 0; i < kPingpoingCnt; ++i)
    {
        auto size = fast_pseudo_rand_int(0, kBenchMessageBufSize);
        for (size_t i = 0; i < size; ++i)
        {
            bench_msg.from_tid = dsm->get_thread_id();
            bench_msg.from_node = dsm->get_node_id();
            bench_msg.size = size;
            fast_pseudo_fill_buf(bench_msg.buf, bench_msg.size);
        }
        dsm->unreliable_send(buf, sizeof(BenchMessage), kServerNodeId, dir_id);

        dsm->unreliable_recv(recv_buf);
        auto &recv_msg = *(BenchMessage *) recv_buf;
        if (memcmp(recv_msg.buf, bench_msg.buf, bench_msg.size) != 0)
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
void server_varsize_correct(std::shared_ptr<DSM> dsm)
{
    char recv_buf[1024];
    auto *buf = dsm->get_rdma_buffer().buffer;
    for (size_t i = 0; i < kPingpoingCnt; ++i)
    {
        dsm->unreliable_recv(recv_buf);
        auto &msg = *(BenchMessage *) recv_buf;
        auto from_tid = msg.from_tid;
        memcpy(buf, recv_buf, sizeof(BenchMessage));
        dsm->unreliable_send(
            buf, sizeof(BenchMessage), kClientNodeId, from_tid);
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

            char buffer[102400];
            auto *rdma_buf = dsm->get_rdma_buffer().buffer;

            std::array<ssize_t, kMaxAppThread> recv_msg_nrs{};
            while (finished_nr < total_nr)
            {
                size_t get = dsm->unreliable_try_recv(buffer, 64);
                finished_nr.fetch_add(get);
                for (size_t i = 0; i < get; ++i)
                {
                    auto *recv_msg =
                        (BenchMessage *) ((char *) buffer +
                                          config::umsg::kUserMessageSize * i);
                    check_valid(*recv_msg);
                    auto from_tid = recv_msg->from_tid;
                    recv_msg_nrs[from_tid]++;

                    BenchMessage *send_msg = (BenchMessage *) rdma_buf;
                    memcpy(send_msg, recv_msg, sizeof(BenchMessage));
                    if (recv_msg_nrs[from_tid] % 64 == 0)
                    {
                        recv_msg_nrs[from_tid] -= 64;
                        VLOG(3) << "[wait] server tid " << tid << " let go mid "
                                << from_tid;
                        dsm->unreliable_send((char *) send_msg,
                                             sizeof(BenchMessage),
                                             kClientNodeId,
                                             from_tid);
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
            auto nid = dsm->get_node_id();
            auto dir_id = tid % NR_DIRECTORY;

            size_t sent = 0;

            char buffer[1024];

            auto *rdma_buf = dsm->get_rdma_buffer().buffer;
            BenchMessage *msg = (BenchMessage *) rdma_buf;
            for (size_t time = 0; time < kBurnCnt; ++time)
            {
                msg->size = fast_pseudo_rand_int(0, kBenchMessageBufSize);
                fast_pseudo_fill_buf(msg->buf, msg->size);
                msg->from_node = nid;
                msg->from_tid = tid;
                msg->digest = djb2_digest(msg->buf, msg->size);
                dsm->unreliable_send(
                    rdma_buf, sizeof(BenchMessage), kServerNodeId, dir_id);
                sent++;
                if (sent % 64 == 0)
                {
                    DVLOG(3)
                        << "[wait] tid " << tid << " sent 64. wait for ack. ";
                    dsm->unreliable_recv(buffer);
                    auto *recv_msg = (BenchMessage *) buffer;
                    std::ignore = recv_msg;
                    DVLOG(3) << "[wait] tid " << tid << " recv continue msg";
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

    client_varsize_correct(dsm, tid % NR_DIRECTORY);

    client_multithread(dsm, kClientThreadNr);

    dsm->barrier("finished", 10ms);
    LOG(INFO) << "ALL TEST PASSED";
}

void server(std::shared_ptr<DSM> dsm)
{
    LOG(INFO) << "Begin burn";

    server_varsize_correct(dsm);

    size_t expect_work = kClientThreadNr * kBurnCnt;
    server_multithread(dsm, expect_work, kServerThreadNr);

    dsm->barrier("finished", 10ms);
    LOG(INFO) << "ALL TEST PASSED";
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    CHECK(false) << "TODO: revise codes";
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