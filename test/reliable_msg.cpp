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

void client(std::shared_ptr<DSM> dsm)
{
    size_t cnt = 0;

    char *buf = dsm->get_rdma_buffer();
    LOG(INFO) << "Sending " << std::hex << kMagic;
    *(uint64_t *) buf = kMagic;
    dsm->reliable_send(buf, sizeof(uint64_t), kServerNodeId);
    cnt++;

    for (size_t i = 0; i < kRpcNr; ++i)
    {
        dsm->reliable_send(buf, sizeof(uint64_t), kServerNodeId);
        cnt++;
    }
    dsm->reliable_recv(nullptr);
}

void server(std::shared_ptr<DSM> dsm)
{
    [[maybe_unused]] char buf[1024];
    dsm->reliable_recv(buf);
    uint64_t get = *(uint64_t *) buf;
    LOG(INFO) << "Got: " << std::hex << get;
    CHECK_EQ(get, kMagic) << "Failed the test";

    for (size_t i = 0; i < kRpcNr; ++i)
    {
        dsm->reliable_recv(buf);
    }
    dsm->reliable_send(nullptr, 0, kClientNodeId);
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