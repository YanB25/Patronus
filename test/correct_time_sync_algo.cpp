#include <algorithm>
#include <chrono>
#include <iostream>
#include <random>
template <typename T, typename U>
std::ostream &operator<<(std::ostream &os,
                         const std::chrono::time_point<T, U> &time)
{
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                  time.time_since_epoch())
                  .count();
    auto hours =
        std::chrono::duration_cast<std::chrono::hours>(time.time_since_epoch())
            .count();
    auto days = hours / 24;
    os << "{Timepoint ns: " << ns << ", hours: " << hours << ", days: " << days
       << "}";
    return os;
}

#include "Common.h"
#include "DSM.h"
#include "Timer.h"
#include "patronus/All.h"
#include "util/PerformanceReporter.h"
#include "util/monitor.h"

using namespace define::literals;
using namespace std::chrono_literals;

constexpr uint16_t kClientNodeId = 0;
constexpr uint16_t kServerNodeId = 1;
constexpr uint32_t kMachineNr = 2;

constexpr static size_t kMid = 0;
constexpr static size_t kTestTime = 10_M;
constexpr static size_t kMagic = 0xaabbccdd11223355;

struct ClockInfo
{
    std::atomic<int64_t> ns;
    std::atomic<int64_t> adjustment;
    std::atomic<uint64_t> magic;
};

void client(DSM::pointer dsm)
{
    bindCore(1);
    GlobalAddress gaddr;
    gaddr.nodeID = kServerNodeId;
    gaddr.offset = 0;
    auto buffer = dsm->get_server_buffer();
    auto &my_clock_info = *(ClockInfo *) buffer.buffer;
    my_clock_info.magic = kMagic;

    auto *rdma_buffer = dsm->get_rdma_buffer();
    auto &server_clock_info = *(ClockInfo *) rdma_buffer;

    size_t cont_converge_nr = 0;
    size_t sync_nr = 0;

    while (true)
    {
        dsm->read_sync(rdma_buffer, gaddr, sizeof(ClockInfo));
        CHECK_EQ(server_clock_info.magic, kMagic);
        auto server_time = server_clock_info.ns + server_clock_info.adjustment;
        auto now = std::chrono::system_clock::now();
        auto client_time = patronus::to_ns(now) + my_clock_info.adjustment;

        int64_t this_adjustment = server_time - client_time;
        my_clock_info.adjustment += this_adjustment;

        auto after_client_time =
            patronus::to_ns(now) + my_clock_info.adjustment;
        LOG(INFO) << "[bench] adjust " << this_adjustment
                  << ", total_adjustment: " << my_clock_info.adjustment
                  << ", server_time: " << server_time
                  << ", before_client_time: " << client_time
                  << ", after client time: " << after_client_time;
        CHECK_EQ(server_clock_info.adjustment, 0);

        int64_t bound = 1_K;  // 1 us
        if (abs(this_adjustment) <= bound)
        {
            cont_converge_nr++;
        }
        else
        {
            cont_converge_nr = 0;
        }
        if (unlikely(cont_converge_nr >= 5))
        {
            LOG(INFO) << "[bench] client finish syncing time";
            break;
        }
        sync_nr++;
        CHECK_LT(sync_nr, 100) << "Failed to sync within 100ms";
        std::this_thread::sleep_for(1ms);
    }
}
void server(DSM::pointer dsm)
{
    std::atomic<bool> finish{false};
    std::thread t([dsm, &finish]() {
        bindCore(1);
        auto buffer = dsm->get_server_buffer();
        auto &clock_info = *(ClockInfo *) buffer.buffer;
        clock_info.adjustment = 0;
        clock_info.magic = kMagic;
        while (!finish.load(std::memory_order_relaxed))
        {
            auto now = std::chrono::system_clock::now();
            uint64_t ns = patronus::to_ns(now);
            clock_info.ns = ns;
        }
    });

    dsm->reliable_recv(kMid, nullptr, 1);

    finish = true;
    t.join();
    LOG(INFO) << "Server adjustment 0. exist";
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    rdmaQueryDevice();

    DSMConfig config;
    config.machineNR = kMachineNr;

    auto dsm = DSM::getInstance(config);

    dsm->registerThread();

    LOG(WARNING) << "Using system_clock";

    // let client spining
    auto nid = dsm->getMyNodeID();
    if (nid == kClientNodeId)
    {
        std::this_thread::sleep_for(1s);
        client(dsm);
        for (size_t i = 0; i < 3; ++i)
        {
            LOG(INFO) << "finish one round sync. sleep 10s";
            std::this_thread::sleep_for(10s);
            client(dsm);
        }
        dsm->reliable_send(nullptr, 0, kServerNodeId, kMid);
    }
    else
    {
        server(dsm);
    }

    LOG(INFO) << "finished. ctrl+C to quit.";
}