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

constexpr uint16_t kClientNodeId = 0;
constexpr uint16_t kServerNodeId = 1;
constexpr uint32_t kMachineNr = 2;

constexpr static size_t kMid = 0;
constexpr static size_t kTestTime = 10_M;

struct BenchMessage
{
    uint64_t time;
};

void client(DSM::pointer dsm)
{
    OnePassMonitor m;
    auto *buf = dsm->get_rdma_buffer();
    char recv_buffer[1024];
    for (size_t i = 0; i < kTestTime; ++i)
    {
        auto &msg = *(BenchMessage *) buf;
        auto now = std::chrono::system_clock::now();
        msg.time = patronus::to_ns(now);
        dsm->reliable_send(buf, sizeof(msg), kServerNodeId, kMid);
        dsm->reliable_recv(kMid, recv_buffer, 1);

        auto &recv_msg = *(BenchMessage *) recv_buffer;
        auto that_time = patronus::ns_to_system_clock(recv_msg.time);
        now = std::chrono::system_clock::now();
        // CHECK_LT(that_time, now) << "recv BenchMessage from future";

        auto diff_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           now - that_time)
                           .count();
        m.collect(diff_ns);
    }

    LOG(INFO) << "Network time different: " << m;
    LOG_IF(ERROR, m.min() <= 0)
        << "Time reverse detected. possible time epsilon >= " << m.min() / 1000
        << " us";
}
void server(DSM::pointer dsm)
{
    OnePassMonitor m;
    auto *buf = dsm->get_rdma_buffer();
    char recv_buf[1024];
    for (size_t i = 0; i < kTestTime; ++i)
    {
        dsm->reliable_recv(kMid, recv_buf);
        auto &msg = *(BenchMessage *) recv_buf;
        auto that_time = patronus::ns_to_system_clock(msg.time);
        auto now = std::chrono::system_clock::now();
        // CHECK_LT(that_time, now) << "Receive a msg from future";
        auto diff_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           now - that_time)
                           .count();
        m.collect(diff_ns);

        auto &send_msg = *(BenchMessage *) buf;
        now = std::chrono::system_clock::now();
        send_msg.time = patronus::to_ns(now);
        dsm->reliable_send(buf, sizeof(BenchMessage), kClientNodeId, kMid);
    }

    LOG(INFO) << "Network time different: " << m;
    LOG_IF(ERROR, m.min() <= 0)
        << "** Time reverse detected. possible time epsilon >= "
        << m.min() / 1000 << " us";
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