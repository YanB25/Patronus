#include <glog/logging.h>

#include <algorithm>
#include <random>

#include "DSM.h"
#include "PerThread.h"
#include "Timer.h"
#include "gflags/gflags.h"
#include "umsg/Config.h"
#include "util/Rand.h"
#include "util/monitor.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");
constexpr uint16_t kClientNodeId = 0;
[[maybe_unused]] constexpr uint16_t kServerNodeId = 1;
constexpr uint32_t kMachineNr = 2;

// constexpr static uint64_t kMagic = 0xaabbccddeeff0011;

// constexpr static size_t kRpcNr = 100;
// constexpr static size_t kMsgSize = 16;

constexpr static size_t kPingpoingCnt = 100 * define::K;
constexpr static size_t kBurnCnt = 100 * define::M;
constexpr static size_t kThreadNr = kMaxAppThread - 1;
// constexpr static size_t kThreadNr = 1;
// constexpr static size_t kBenchMsgSize = 16;

struct BenchMsg
{
    uint16_t tid;
} __attribute__((packed));

void client_pingpong_correct(std::shared_ptr<DSM> dsm)
{
    auto rdma_buffer = dsm->get_rdma_buffer();
    auto *buf = rdma_buffer.buffer;
    DCHECK_GT(rdma_buffer.size, sizeof(uint64_t));
    char recv_buf[1024];
    for (size_t i = 0; i < kPingpoingCnt; ++i)
    {
        uint64_t magic = fast_pseudo_rand_int();
        *(uint64_t *) buf = magic;
        dsm->unreliable_send(buf, sizeof(uint64_t), kServerNodeId, 0);

        dsm->unreliable_recv(recv_buf);
        uint64_t get = *(uint64_t *) recv_buf;
        CHECK_EQ(get, magic)
            << "Pingpoing content mismatch for " << i << "-th test";
    }
}

constexpr static size_t kTokenNr = 4;

void client_burn(std::shared_ptr<DSM> dsm, size_t thread_nr)
{
    std::vector<std::thread> threads;

    // @kTokenNr token, each of which get ReliableConnection::kRecvBuffer /
    constexpr size_t msg_each_token =
        config::umsg::kPostRecvBufferBatch / kTokenNr;
    CHECK_EQ(config::umsg::kPostRecvBufferBatch % kTokenNr, 0);

    Timer t;
    t.begin();

    Perthread<std::atomic<size_t>> counts;
    for (size_t i = 0; i < thread_nr; ++i)
    {
        threads.emplace_back([dsm, &count = counts[i]]() {
            dsm->registerThread();
            auto tid = dsm->get_thread_id();
            auto to_dir_id = tid % NR_DIRECTORY;

            auto rdma_buffer = dsm->get_rdma_buffer();
            auto *buf = rdma_buffer.buffer;
            DCHECK_LT(sizeof(BenchMsg), rdma_buffer.size);
            auto *send_msg = (BenchMsg *) buf;
            send_msg->tid = tid;

            size_t sent = 0;
            char buffer[config::umsg::kUserMessageSize *
                        config::umsg::kRecvLimit];
            int64_t token = kTokenNr;
            for (size_t t = 0; t < kBurnCnt; ++t)
            {
                dsm->unreliable_send(
                    buf, sizeof(BenchMsg), kServerNodeId, to_dir_id);
                sent++;
                if (sent % msg_each_token == 0)
                {
                    count.fetch_add(msg_each_token, std::memory_order_relaxed);

                    token--;
                    VLOG(3) << "[wait] tid " << tid << " sent " << sent
                            << " at " << msg_each_token << ", wait for ack.";
                    do
                    {
                        size_t recv_nr = dsm->unreliable_try_recv(
                            buffer, config::umsg::kRecvLimit);
                        // handle possbile recv token
                        for (size_t r = 0; r < recv_nr; ++r)
                        {
                            void *msg_addr =
                                buffer + config::umsg::kUserMessageSize * r;
                            auto *recv_msg = (BenchMsg *) msg_addr;
                            std::ignore = recv_msg;
                            VLOG(3) << "[wait] tid " << tid
                                    << " recv continue msg from TODO. add one "
                                       "token";
                            token++;
                        }
                        if (token <= 0)
                        {
                            VLOG(1) << "[wait] tid " << tid << " blocked.";
                        }
                    } while (token <= 0);
                    VLOG(3) << "[wait] tid " << tid
                            << " has enough token. current: " << token;
                }
            }
        });
    }

    size_t expect_nr = thread_nr * kBurnCnt;

    auto start = std::chrono::steady_clock::now();
    size_t last_sum = 0;
    while (true)
    {
        size_t sum = 0;
        for (size_t i = 0; i < thread_nr; ++i)
        {
            sum += counts[i].load(std::memory_order_relaxed);
        }
        auto end = std::chrono::steady_clock::now();
        auto ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
                .count();
        LOG(INFO) << "[bench] OPS: " << 1.0 * 1e9 * (sum - last_sum) / ns
                  << ", for sum: " << (sum - last_sum) << ", ns " << ns;

        start = end;
        last_sum = sum;

        sleep(1);
        if (sum == expect_nr)
        {
            break;
        }
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
    Perthread<std::atomic<size_t>> gots;

    std::atomic<bool> finished{false};

    for (size_t i = 0; i < thread_nr; ++i)
    {
        threads.emplace_back([dsm, &gots, &finished]() {
            dsm->registerThread();
            auto tid = dsm->get_thread_id();

            char buffer[config::umsg::kUserMessageSize *
                        config::umsg::kRecvLimit];
            auto *rdma_buf = dsm->get_rdma_buffer().buffer;
            memset(rdma_buf, 0, sizeof(BenchMsg));
            constexpr size_t credit_for_token =
                config::umsg::kPostRecvBufferBatch / kTokenNr;
            while (!finished.load(std::memory_order_relaxed))
            {
                auto get =
                    dsm->unreliable_try_recv(buffer, config::umsg::kRecvLimit);
                for (size_t m = 0; m < get; ++m)
                {
                    auto &msg =
                        *(BenchMsg *) (buffer +
                                       config::umsg::kUserMessageSize * m);
                    auto from_tid = msg.tid;
                    gots[from_tid].fetch_add(1, std::memory_order_relaxed);
                    if (gots[from_tid] >= credit_for_token)
                    {
                        gots[from_tid] -= credit_for_token;
                        VLOG(3) << "[wait] server tid " << tid << " let go";
                        dsm->unreliable_send((char *) rdma_buf,
                                             sizeof(BenchMsg),
                                             kClientNodeId,
                                             from_tid);
                    }
                }
            }
        });
    }
    while (true)
    {
        sleep(1);
        size_t sum = 0;
        for (size_t i = 0; i < gots.size(); ++i)
        {
            sum += gots[i].load(std::memory_order_relaxed);
        }
        if (sum >= total_msg_nr)
        {
            LOG(WARNING)
                << "[bench] Okay, server receives all the messages. Exit...";
            finished = true;
            break;
        }
    }
    for (auto &t : threads)
    {
        t.join();
    }
}
void server_pingpong_correct(std::shared_ptr<DSM> dsm)
{
    char recv_buf[1024];
    auto *buf = dsm->get_rdma_buffer().buffer;
    for (size_t i = 0; i < kPingpoingCnt; ++i)
    {
        dsm->unreliable_recv(recv_buf, 1);
        uint64_t get = *(uint64_t *) recv_buf;

        *(uint64_t *) buf = get;
        dsm->unreliable_send(buf, sizeof(uint64_t), kClientNodeId, 0);
    }
}

void client_wait(std::shared_ptr<DSM> dsm)
{
    static size_t entered_{0};
    dsm->keeper_barrier(std::to_string(entered_++), 10ms);
}

void client(std::shared_ptr<DSM> dsm)
{
    // client_pingpong_correct(dsm);
    sleep(2);
    LOG(INFO) << "Begin burn";
    client_burn(dsm, kThreadNr);

    client_wait(dsm);
    LOG(INFO) << "Exiting...";
}

void server_wait(std::shared_ptr<DSM> dsm)
{
    static size_t entered_{0};
    dsm->keeper_barrier(std::to_string(entered_++), 10ms);
}

void server(std::shared_ptr<DSM> dsm)
{
    // server_pingpong_correct(dsm);

    size_t expect_msg_nr = kThreadNr * kBurnCnt;
    LOG(INFO) << "Begin burn";
    server_burn(dsm, expect_msg_nr, kThreadNr);

    server_wait(dsm);
    LOG(INFO) << "Exiting...";
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    rdmaQueryDevice();
    CHECK(false) << "TODO: see if its correct";

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