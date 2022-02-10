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
using namespace patronus;

constexpr uint16_t kClientNodeId = 0;
constexpr uint16_t kServerNodeId = 1;
constexpr uint32_t kMachineNr = 2;

constexpr static size_t kMid = 0;
constexpr static size_t kTestTime = 1_M;
constexpr static size_t kTimeDriftLimit = 50_K;  // 50us

struct BenchMessage
{
    uint64_t time;
};

void client(Patronus::pointer p)
{
    LOG(INFO) << "Client begin test";
    OnePassMonitor m;
    OnePassMonitor send_recv_m;
    auto dsm = p->get_dsm();
    auto &syncer = p->time_syncer();
    auto *buf = dsm->get_rdma_buffer().buffer;
    char recv_buffer[1024];
    for (size_t i = 0; i < kTestTime; ++i)
    {
        auto &msg = *(BenchMessage *) buf;
        auto patronus_now = syncer.patronus_now();
        msg.time = patronus_now.term();

        auto before_send_recv = std::chrono::steady_clock::now();
        dsm->reliable_send(buf, sizeof(msg), kServerNodeId, kMid);
        dsm->reliable_recv(kMid, recv_buffer, 1);
        auto after_send_recv = std::chrono::steady_clock::now();
        auto send_recv_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                after_send_recv - before_send_recv)
                .count();

        auto &recv_msg = *(BenchMessage *) recv_buffer;
        auto that_patronus_time = time::PatronusTime(recv_msg.time);
        patronus_now = syncer.patronus_now();

        auto diff_ns = patronus_now - that_patronus_time;
        // TODO(patronus): time drift too high, can not satisfiy this.
        // CHECK(!syncer.definitely_gt(that_patronus_time, patronus_now))
        //     << "** Receive a msg from future. that: " << that_patronus_time
        //     << ", now: " << patronus_now << ", epsilon: " << syncer.epsilon()
        //     << ", definitely_gt. diff_ns: " << diff_ns;

        m.collect(diff_ns);
        send_recv_m.collect(send_recv_ns);
    }

    LOG(INFO) << "Time difference of me and server: " << m
              << ", send_recv_latency: " << send_recv_m;
    LOG_IF(ERROR, m.min() <= 0)
        << "Time reverse detected. possible time epsilon >= " << m.min() / 1000
        << " us";
    CHECK(m.abs_average() <= kTimeDriftLimit)
        << "** Time drift larger than allowed";
}
void server(Patronus::pointer p)
{
    LOG(INFO) << "Server begin test";
    OnePassMonitor m;
    auto dsm = p->get_dsm();
    auto &syncer = p->time_syncer();
    auto *buf = dsm->get_rdma_buffer().buffer;
    char recv_buf[1024];
    for (size_t i = 0; i < kTestTime; ++i)
    {
        dsm->reliable_recv(kMid, recv_buf);
        auto &msg = *(BenchMessage *) recv_buf;
        auto that_patronus_time = time::PatronusTime(msg.time);
        auto patronus_now = syncer.patronus_now();
        auto diff_ns = patronus_now - that_patronus_time;
        // TODO(patronus): time drift too high, can not satisfiy this.
        // CHECK(!syncer.definitely_gt(that_patronus_time, patronus_now))
        //     << "** Receive a msg from future. that: " << that_patronus_time
        //     << ", now: " << patronus_now << ", epsilon: " << syncer.epsilon()
        //     << ", definitely_gt. diff_ns: " << diff_ns;
        m.collect(diff_ns);

        auto &send_msg = *(BenchMessage *) buf;
        patronus_now = syncer.patronus_now();
        send_msg.time = patronus_now.term();
        dsm->reliable_send(buf, sizeof(BenchMessage), kClientNodeId, kMid);
    }

    LOG(INFO) << "Network time different: " << m;
    LOG_IF(ERROR, m.min() <= 0)
        << "** Time reverse detected. possible time epsilon >= "
        << m.min() / 1000 << " us";
    CHECK(m.abs_average() <= kTimeDriftLimit)
        << "** Time drift larget than allowed";
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    LOG(WARNING) << "Current allow " << kTimeDriftLimit
                 << " ns drift, which is too large. should try to optimize "
                    "till several us.";

    rdmaQueryDevice();

    PatronusConfig config;
    config.machine_nr = kMachineNr;

    auto patronus = Patronus::ins(config);

    // let client spining
    auto nid = patronus->get_node_id();
    if (nid == kClientNodeId)
    {
        client(patronus);
    }
    else
    {
        server(patronus);
    }

    LOG(INFO) << "finished. ctrl+C to quit.";
}