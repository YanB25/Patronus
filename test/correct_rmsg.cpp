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

constexpr static size_t kPingpoingCnt = 100_K;
constexpr static size_t kBurnCnt = 1_M;

constexpr static size_t kBenchMessageBufSize = 16;

// constexpr static size_t kMultiThreadNr = 16;
constexpr static size_t kClientThreadNr = 16;
constexpr static size_t kServerThreadNr = NR_DIRECTORY;
static_assert(kClientThreadNr <= kMaxAppThread);
static_assert(kServerThreadNr <= kMaxAppThread);

constexpr static ssize_t kSendToken =
    config::umsg::kExpectInFlightMessageNr - 1;
static_assert(kSendToken < config::umsg::kExpectInFlightMessageNr);
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

inline std::ostream &operator<<(std::ostream &os, const BenchMessage &msg)
{
    os << "{BenchMsg from_node: " << msg.from_node
       << ", from_tid: " << msg.from_tid << ", size: " << (int) msg.size
       << ", digest: " << std::hex << msg.digest << "}";
    return os;
}

void check_valid(const BenchMessage &msg)
{
    CHECK_LT(msg.from_node, ::config::kMachineNr) << msg;
    CHECK_LE(msg.size, kBenchMessageBufSize) << msg;
    auto dg = CityHash64(msg.buf, msg.size);
    CHECK_EQ(dg, msg.digest) << msg;
}

void client_varsize_correct(std::shared_ptr<DSM> dsm, size_t dir_id)
{
    LOG(WARNING) << "[bench] testing varsize for dir_id = " << dir_id;
    auto tid = dsm->get_thread_id();
    auto *buf = dsm->get_rdma_buffer().buffer;
    char recv_buf[1024];
    auto server_nid = ::config::get_server_nids().front();

    auto &bench_msg = *(BenchMessage *) buf;
    for (size_t i = 0; i < kPingpoingCnt; ++i)
    {
        DVLOG(1) << "[bench] client tid: " << tid << " to dir: " << dir_id
                 << " for " << i << "-th tests";
        auto size = fast_pseudo_rand_int(0, kBenchMessageBufSize);
        bench_msg.from_tid = dsm->get_thread_id();
        bench_msg.from_node = dsm->get_node_id();
        bench_msg.size = size;
        fast_pseudo_fill_buf(bench_msg.buf, bench_msg.size);
        bench_msg.digest = CityHash64(bench_msg.buf, bench_msg.size);

        dsm->unreliable_send(buf, sizeof(BenchMessage), server_nid, dir_id);

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
    LOG(INFO) << "[bench] client finished varsize_correct()";
}
void server_varsize_correct(std::shared_ptr<DSM> dsm)
{
    auto tid = dsm->get_thread_id();
    LOG(INFO) << "[bench] started server_varsize_correct(). tid: " << tid;
    char recv_buf[1024];
    auto *buf = dsm->get_rdma_buffer().buffer;
    for (size_t i = 0; i < kPingpoingCnt * ::config::get_client_nids().size();
         ++i)
    {
        dsm->unreliable_recv(recv_buf);
        DVLOG(1) << "[bench] server received " << i << "-th message";
        auto &msg = *(BenchMessage *) recv_buf;
        check_valid(msg);
        auto from_tid = msg.from_tid;
        auto from_nid = msg.from_node;
        memcpy(buf, recv_buf, sizeof(BenchMessage));
        dsm->unreliable_send(buf, sizeof(BenchMessage), from_nid, from_tid);
    }
    LOG(INFO) << "[bench] server finished varsize_correct()";
}

void server_multithread_do(DSM::pointer dsm,
                           std::atomic<size_t> &finished_nr,
                           size_t total_nr)
{
    auto tid = dsm->get_thread_id();

    std::vector<char> __buffer;
    __buffer.resize(config::umsg::kUserMessageSize * config::umsg::kRecvLimit);
    char *buffer = __buffer.data();

    auto *rdma_buf = dsm->get_rdma_buffer().buffer;

    ssize_t recv_msg_nrs[kMaxAppThread][MAX_MACHINE]{};
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
            auto from_nid = recv_msg->from_node;
            recv_msg_nrs[from_tid][from_nid]++;

            BenchMessage *send_msg = (BenchMessage *) rdma_buf;
            memcpy(send_msg, recv_msg, sizeof(BenchMessage));
            if (recv_msg_nrs[from_tid][from_nid] >= kSendToken)
            {
                recv_msg_nrs[from_tid][from_nid] -= kSendToken;
                VLOG(1) << "[wait] server tid " << tid << " let go mid "
                        << from_tid << ". finished nr: " << finished_nr;
                dsm->unreliable_send((char *) send_msg,
                                     sizeof(BenchMessage),
                                     from_nid,
                                     from_tid);
            }
        }
    }
}

void server_multithread(std::shared_ptr<DSM> dsm,
                        size_t total_nr,
                        size_t thread_nr)
{
    std::vector<std::thread> threads;
    std::atomic<size_t> finished_nr{0};

    for (size_t i = 0; i < thread_nr - 1; ++i)
    {
        threads.emplace_back([dsm, &finished_nr, total_nr]() {
            dsm->registerThread();
            server_multithread_do(dsm, finished_nr, total_nr);
        });
    }

    server_multithread_do(dsm, finished_nr, total_nr);

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
        threads.emplace_back([dsm]() {
            dsm->registerThread();
            auto server_nid = ::config::get_server_nids().front();

            auto tid = dsm->get_thread_id();
            auto nid = dsm->get_node_id();
            auto dir_id = tid % NR_DIRECTORY;
            LOG(INFO) << "[bench] client nid " << nid << " tid " << tid
                      << " benching server nid: " << server_nid
                      << ", dir_id: " << dir_id;

            ssize_t sent = 0;

            char buffer[1024];

            auto *rdma_buf = dsm->get_rdma_buffer().buffer;
            BenchMessage *msg = (BenchMessage *) rdma_buf;
            for (size_t time = 0; time < kBurnCnt; ++time)
            {
                msg->size = fast_pseudo_rand_int(0, kBenchMessageBufSize);
                fast_pseudo_fill_buf(msg->buf, msg->size);
                msg->from_node = nid;
                msg->from_tid = tid;
                msg->digest = CityHash64(msg->buf, msg->size);
                dsm->unreliable_send(
                    rdma_buf, sizeof(BenchMessage), server_nid, dir_id);
                sent++;
                if (sent >= kSendToken)
                {
                    DVLOG(1)
                        << "[wait] tid " << tid << " sent use out send token "
                        << kSendToken << ". wait for ack. time: " << time;
                    dsm->unreliable_recv(buffer);
                    auto *recv_msg = (BenchMessage *) buffer;
                    std::ignore = recv_msg;
                    sent -= kSendToken;
                    DVLOG(1) << "[wait] tid " << tid
                             << " recv continue msg. time: " << time;
                }

                if (time % 100_K == 0)
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
    [[maybe_unused]] auto tid = dsm->get_thread_id();
    [[maybe_unused]] auto nid = dsm->get_node_id();

    LOG(INFO) << "Begin burn";

    client_varsize_correct(dsm, tid % NR_DIRECTORY);

    dsm->keeper_barrier("finished varsize_correct()", 10ms);

    client_multithread(dsm, kClientThreadNr);

    dsm->keeper_barrier("finished", 10ms);
    LOG(INFO) << "ALL TEST PASSED";
}

void server(std::shared_ptr<DSM> dsm)
{
    LOG(INFO) << "Begin burn";

    server_varsize_correct(dsm);

    dsm->keeper_barrier("finished varsize_correct()", 10ms);

    size_t expect_work =
        ::config::get_client_nids().size() * kClientThreadNr * kBurnCnt;
    server_multithread(dsm, expect_work, kServerThreadNr);

    dsm->keeper_barrier("finished", 10ms);
    LOG(INFO) << "ALL TEST PASSED";
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    rdmaQueryDevice();

    DSMConfig config;
    config.machineNR = ::config::kMachineNr;

    auto dsm = DSM::getInstance(config);

    sleep(1);

    dsm->registerThread();

    // let client spining
    auto nid = dsm->getMyNodeID();
    if (::config::is_client(nid))
    {
        client(dsm);
    }
    else
    {
        server(dsm);
    }

    LOG(INFO) << "finished. ctrl+C to quit.";
}