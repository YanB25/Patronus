#include <algorithm>
#include <random>

#include "DSM.h"
#include "Timer.h"
#include "util/monitor.h"
#include <glog/logging.h>

// Two nodes
// one node issues cas operations

constexpr uint16_t kClientNodeId = 0;
[[maybe_unused]] constexpr uint16_t kServerNodeId = 1;
constexpr uint32_t kMachineNr = 2;

// constexpr static uint64_t kMagic = 0xaabbccddeeff0011;

// constexpr static size_t kRpcNr = 100;
// constexpr static size_t kMsgSize = 16;

constexpr static size_t kPingpoingCnt = 100 * define::K;
constexpr static size_t kBurnCnt = 100 * define::M;
constexpr static size_t kThreadNr = RMSG_MULTIPLEXING;
// constexpr static size_t kThreadNr = 1;
void client_pingpong_correct(std::shared_ptr<DSM> dsm)
{
    auto *buf = dsm->get_rdma_buffer();
    char recv_buf[1024];
    for (size_t i = 0; i < kPingpoingCnt; ++i)
    {
        uint64_t magic = rand();
        *(uint64_t *) buf = magic;
        dsm->reliable_send(buf, sizeof(uint64_t), kServerNodeId, 0);

        dsm->reliable_recv(recv_buf);
        uint64_t get = *(uint64_t *) recv_buf;
        CHECK_EQ(get, magic)
            << "Pingpoing content mismatch for " << i << "-th test";
        // LOG_EVERY_N(INFO, 1000) << "round " << i << " magic: " << std::hex << magic;
    }
}
void client_burn(std::shared_ptr<DSM> dsm, size_t thread_nr)
{
    std::vector<std::thread> threads;
    Timer t;
    t.begin();
    for (size_t i = 0; i < thread_nr; ++i)
    {
        threads.emplace_back([dsm, i]() {
            bindCore(i + 1);
            dsm->registerThread();
            auto tid = dsm->get_thread_id();
            auto mid = tid % RMSG_MULTIPLEXING;

            auto* buf = dsm->get_rdma_buffer();
            
            for (size_t t = 0; t < kBurnCnt; ++t)
            {
                dsm->reliable_send(buf, 8, kServerNodeId, mid);
            }
        });
    }
    for (auto& t: threads)
    {
        t.join();
    }

    auto ns = t.end();
    auto op = thread_nr * kBurnCnt;
    LOG(INFO) << "count: " << op << ", ns: " << ns << ", ops: " << 1e9 * op / ns;
}
void server_burn(std::shared_ptr<DSM> dsm, size_t thread_nr)
{
    std::vector<std::thread> threads;
    std::atomic<size_t> got{0};
    for (size_t i = 0; i < thread_nr; ++i)
    {
        threads.emplace_back([dsm, i, &got, thread_nr]() {
            bindCore(i + 1);
            dsm->registerThread();
            auto tid = dsm->get_thread_id();
            // auto mid = tid % RMSG_PUBLIC_MULTIPLEXING;

            size_t expect_nr = kBurnCnt * thread_nr;
            char buffer[64 * 128];
            while (got.load() < expect_nr)
            {
                auto get = dsm->reliable_try_recv(buffer, 64);
                got.fetch_add(get);
                VLOG(3) << "[bench] get " << get << " for tid " << tid;
            }
        });
    }
    for (auto& t: threads)
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
        dsm->reliable_recv(recv_buf);
        uint64_t get = *(uint64_t *) recv_buf;

        *(uint64_t *) buf = get;
        dsm->reliable_send(buf, sizeof(uint64_t), kClientNodeId, 0);
    }
}

void client_wait(std::shared_ptr<DSM> dsm)
{
    // sync
    auto* buf = dsm->get_rdma_buffer();
    dsm->reliable_recv(nullptr);
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
    auto* buffer = dsm->get_rdma_buffer();
    dsm->reliable_send(buffer, 0, kClientNodeId, 0);
    dsm->reliable_recv(nullptr);
}

void server(std::shared_ptr<DSM> dsm)
{
    // server_pingpong_correct(dsm);

    LOG(INFO) << "Begin burn";
    server_burn(dsm, kThreadNr);

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