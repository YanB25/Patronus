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

constexpr static uint64_t kMagic = 0xaabbccddeeff0011;

constexpr static size_t kRpcNr = 100;

constexpr static size_t kPingpoingCnt = 100;
void client_pingpong_correct(std::shared_ptr<DSM> dsm)
{
    auto *buf = dsm->get_rdma_buffer();
    char recv_buf[1024];
    for (size_t i = 0; i < kPingpoingCnt; ++i)
    {
        uint64_t magic = rand();
        *(uint64_t *) buf = magic;
        dsm->reliable_send(buf, sizeof(uint64_t), kServerNodeId);

        dsm->reliable_recv(recv_buf);
        uint64_t get = *(uint64_t *) recv_buf;
        CHECK_EQ(get, magic)
            << "Pingpoing content mismatch for " << i << "-th test";
        LOG_EVERY_N(INFO, 100) << "Finish round " << i << " magic: " << std::hex << magic;
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
        dsm->reliable_send(buf, sizeof(uint64_t), kClientNodeId);
    }
}

void client(std::shared_ptr<DSM> dsm)
{
    client_pingpong_correct(dsm);

    // sync
    dsm->reliable_recv(nullptr);
    dsm->reliable_send(nullptr, 0, kServerNodeId);
}

void server(std::shared_ptr<DSM> dsm)
{
    server_pingpong_correct(dsm);

    dsm->reliable_send(nullptr, 0, kServerNodeId);
    dsm->reliable_recv(nullptr);
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