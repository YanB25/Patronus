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
using namespace std::chrono_literals;

constexpr uint16_t kClientNodeId = 0;
constexpr uint16_t kServerNodeId = 1;
constexpr uint32_t kMachineNr = 2;

constexpr static size_t kMid = 0;
constexpr static ssize_t kTestSecond = 60;

struct BenchMessage
{
    uint64_t time;
};

uint64_t get_current_ns()
{
    auto now = std::chrono::system_clock::now();
    // auto now = std::chrono::steady_clock::now();
    // auto now = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               now.time_since_epoch())
        .count();
    // return rdtsc();
}

void client(Patronus::pointer p)
{
    std::array<OnePassIntegerMonitor, kTestSecond> diff_ns_ms;
    std::array<bool, kTestSecond> entered;
    entered.fill(false);

    OnePassIntegerMonitor send_recv_m;

    auto dsm = p->get_dsm();
    auto *buf = dsm->get_rdma_buffer().buffer;
    char recv_buffer[1024];

    auto begin_now = std::chrono::steady_clock::now();
    while (true)
    {
        auto now = std::chrono::steady_clock::now();
        size_t ith_second =
            std::chrono::duration_cast<std::chrono::seconds>(now - begin_now)
                .count();
        if (unlikely(ith_second >= kTestSecond))
        {
            break;
        }
        if (unlikely(!entered[ith_second]))
        {
            LOG(INFO) << "[bench] " << ith_second << " s.";
            entered[ith_second] = true;
        }

        auto &msg = *(BenchMessage *) buf;
        msg.time = get_current_ns();

        auto before_send_recv = std::chrono::steady_clock::now();
        dsm->reliable_send(buf, sizeof(msg), kServerNodeId, kMid);
        dsm->reliable_recv(kMid, recv_buffer, 1);
        auto after_send_recv = std::chrono::steady_clock::now();
        auto send_recv_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                after_send_recv - before_send_recv)
                .count();

        auto &recv_msg = *(BenchMessage *) recv_buffer;
        auto recv_ns = recv_msg.time;
        auto recv_now_ns = get_current_ns();
        if (unlikely(recv_ns == 0))
        {
            // a signal to exit
            break;
        }

        int64_t diff_ns = recv_now_ns - recv_ns;

        diff_ns_ms[ith_second].collect(diff_ns);
        send_recv_m.collect(send_recv_ns);
    }
    auto &msg = *(BenchMessage *) buf;
    msg.time = 0;
    dsm->reliable_send(buf, sizeof(msg), kServerNodeId, kMid);

    for (size_t i = 0; i < diff_ns_ms.size(); ++i)
    {
        LOG(INFO) << "[bench] " << i << " s: " << diff_ns_ms[i];
    }

    auto &first_m = diff_ns_ms[0];
    auto &last_m = diff_ns_ms[diff_ns_ms.size() - 1];
    auto time_drift = abs(last_m.average() - first_m.average());
    double drift_ns_per_second = 1.0 * time_drift / kTestSecond;
    double drift_ns_per_day = drift_ns_per_second * 60 * 60 * 24;
    LOG(INFO) << "[bench] witness time drift of " << drift_ns_per_second
              << " unit/s, or " << 1.0 * drift_ns_per_day << " unit/day";
}
void server(Patronus::pointer p)
{
    auto dsm = p->get_dsm();
    auto *buf = dsm->get_rdma_buffer().buffer;
    char recv_buf[1024];

    std::array<OnePassIntegerMonitor, kTestSecond> diff_ns_ms;
    auto begin_now = std::chrono::steady_clock::now();

    while (true)
    {
        auto now = std::chrono::steady_clock::now();
        auto ith_second =
            std::chrono::duration_cast<std::chrono::seconds>(now - begin_now)
                .count();

        dsm->reliable_recv(kMid, recv_buf);
        auto &msg = *(BenchMessage *) recv_buf;
        auto that_ns = msg.time;
        auto now_ns = get_current_ns();
        int64_t diff_ns = now_ns - that_ns;
        if (likely(ith_second < kTestSecond))
        {
            diff_ns_ms[ith_second].collect(diff_ns);
        }

        if (unlikely(that_ns == 0))
        {
            break;
        }

        auto &send_msg = *(BenchMessage *) buf;
        now_ns = get_current_ns();
        send_msg.time = now_ns;
        dsm->reliable_send(buf, sizeof(BenchMessage), kClientNodeId, kMid);
    }

    for (size_t i = 0; i < diff_ns_ms.size(); ++i)
    {
        LOG(INFO) << "[bench] " << i << " s: " << diff_ns_ms[i];
    }
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
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

    auto now = std::chrono::steady_clock::now();
    auto before_rdtsc = rdtsc();
    std::this_thread::sleep_for(1s);
    auto then = std::chrono::steady_clock::now();
    auto then_rdtsc = rdtsc();
    auto diff_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(then - now)
            .count();
    auto diff_rdtsc = then_rdtsc - before_rdtsc;

    LOG(INFO) << "[bench] diff_ns: " << diff_ns
              << ", diff_rdtsc: " << diff_rdtsc
              << ", rdtsc/ns: " << 1.0 * diff_rdtsc / diff_ns
              << ", ns/rdtsc: " << 1.0 * diff_ns / diff_rdtsc;

    LOG(INFO) << "finished. ctrl+C to quit.";
}