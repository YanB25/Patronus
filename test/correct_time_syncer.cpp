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
#include "patronus/TimeSyncer.h"
#include "util/PerformanceReporter.h"
#include "util/monitor.h"

using namespace define::literals;
using namespace std::chrono_literals;

constexpr uint16_t kClientNodeId = 0;
constexpr uint16_t kServerNodeId = 1;
constexpr uint32_t kMachineNr = 2;

void client(DSM::pointer dsm)
{
    GlobalAddress gaddr;
    gaddr.nodeID = kServerNodeId;
    gaddr.offset = 0;
    auto server_buf = dsm->get_server_buffer();
    patronus::time::TimeSyncer syncer(dsm, gaddr, server_buf.buffer, 4096);
    syncer.sync();

    // okay, continue unit test
    auto epsilon = syncer.epsilon();
    auto now = std::chrono::system_clock::now();
    CHECK(syncer.may_eq(now, now));
    CHECK(syncer.may_eq(now, now + std::chrono::nanoseconds(epsilon - 1)));
    CHECK(syncer.may_eq(now, now - std::chrono::nanoseconds(epsilon - 1)));
    CHECK(
        syncer.definitely_gt(now + std::chrono::nanoseconds(epsilon + 1), now));
    CHECK(
        syncer.definitely_lt(now - std::chrono::nanoseconds(epsilon + 1), now));

    auto now2 = std::chrono::steady_clock::now();
    CHECK(syncer.may_eq(now2, now2));
    CHECK(syncer.may_eq(now2, now2 + std::chrono::nanoseconds(epsilon - 1)));
    CHECK(syncer.may_eq(now2, now2 - std::chrono::nanoseconds(epsilon - 1)));
    CHECK(syncer.definitely_gt(now2 + std::chrono::nanoseconds(epsilon + 1),
                               now2));
    CHECK(syncer.definitely_lt(now2 - std::chrono::nanoseconds(epsilon + 1),
                               now2));

    // do a typical wait
    auto begin_now = syncer.chrono_now();
    auto cur_now = syncer.chrono_now();
    size_t loop_nr = 0;
    while (!syncer.definitely_gt(cur_now, begin_now))
    {
        loop_nr++;
        cur_now = syncer.chrono_now();
    }
    LOG(INFO) << "ignore me: loop_nr: " << loop_nr;
}
void server(DSM::pointer dsm)
{
    GlobalAddress gaddr;
    gaddr.nodeID = kServerNodeId;  // set to server deliberately
    gaddr.offset = 0;
    auto server_buf = dsm->get_server_buffer();
    patronus::time::TimeSyncer syncer(dsm, gaddr, server_buf.buffer, 4096);
    syncer.sync();
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