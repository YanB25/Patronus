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
constexpr static size_t kMsgSize = 16;

constexpr static size_t kPingpoingCnt = 100 * define::K;
// constexpr static size_t kBurnCnt = 100 * define::M;

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

void client_varsize_correct(std::shared_ptr<DSM> dsm)
{
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
        dsm->reliable_send(buf, size, kServerNodeId);

        dsm->reliable_recv(recv_buf);
        if (memcmp(recv_buf, buf, size) != 0)
        {
            LOG(ERROR) << "Mismatch result at " << i << "-th test. size " << size;
            for (size_t b = 0; b < size; ++b)
            {
                LOG_IF(ERROR, recv_buf[b] != buf[b]) << "mismatch at " << b << "-th byte. expect " << std::hex << (uint64_t) buf[b] << ", got " << (uint64_t) recv_buf[b];
            }
        }
        CHECK_EQ(memcmp(recv_buf, buf, size), 0)
            << "Pingpoing content mismatch for " << i << "-th test. size " << size << ", recv_buf " << *(uint64_t *) recv_buf << ", buf " << *(uint64_t *) buf;
        // LOG_EVERY_N(INFO, 1000) << "round " << i << " magic: " << std::hex << magic;
    }
}
void server_varsize_correct(std::shared_ptr<DSM> dsm)
{
    char recv_buf[1024];
    auto *buf = dsm->get_rdma_buffer();
    for (size_t i = 0; i < kPingpoingCnt; ++i)
    {
        dsm->reliable_recv(recv_buf);
        memcpy(buf, recv_buf, 32);
        dsm->reliable_send(buf, 32, kClientNodeId);
    }
}

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
        // LOG_EVERY_N(INFO, 1000) << "round " << i << " magic: " << std::hex << magic;
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

void client_wait(std::shared_ptr<DSM> dsm)
{
    // sync
    auto* buf = dsm->get_rdma_buffer();
    dsm->reliable_recv(nullptr);
    dsm->reliable_send(buf, 0, kServerNodeId);

}

void client(std::shared_ptr<DSM> dsm)
{
    // client_pingpong_correct(dsm);
    LOG(INFO) << "Begin burn";
    client_pingpong_correct(dsm);
    client_varsize_correct(dsm);

    client_wait(dsm);
    LOG(INFO) << "Exiting!!!";
}

void server_wait(std::shared_ptr<DSM> dsm)
{
    auto* buffer = dsm->get_rdma_buffer();
    dsm->reliable_send(buffer, 0, kClientNodeId);
    dsm->reliable_recv(nullptr);
}

void server(std::shared_ptr<DSM> dsm)
{
    // server_pingpong_correct(dsm);

    LOG(INFO) << "Begin burn";
    server_pingpong_correct(dsm);
    server_varsize_correct(dsm);

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