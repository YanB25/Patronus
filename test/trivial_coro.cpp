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

constexpr static size_t kCoroCnt = 8;
thread_local CoroCall workers[kCoroCnt];
thread_local CoroCall master;

void do_worker(std::shared_ptr<DSM> dsm, CoroYield &yield, coro_t coro_id)
{
    auto tid = dsm->get_thread_id();
    LOG(INFO) << "Enter do_worker. My work finished. I am tid " << tid
              << ", coro_id " << (int) coro_id;
    yield(master);
}

void do_master(std::shared_ptr<DSM> dsm, CoroYield &yield)
{
    auto tid = dsm->get_thread_id();
    LOG(INFO) << "Enter master. tid " << tid << " starting to yield...";
    for (size_t i = 0; i < kCoroCnt; ++i)
    {
        LOG(INFO) << "Yielding to worker " << i;
        yield(workers[i]);
    }

    LOG(INFO) << "master exit...";
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    VLOG(1) << "It is 1 vlog";
    VLOG(2) << "It is 2 vlog";
    VLOG(3) << "It is 3 vlog";
    LOG(INFO) << "Support color ? " << getenv("TERM");

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
        for (size_t i = 0; i < kCoroCnt; ++i)
        {
            workers[i] = CoroCall([dsm, i](CoroYield &yield)
                                  { do_worker(dsm, yield, i); });
        }
        master = CoroCall([dsm](CoroYield &yield) { do_master(dsm, yield); });

        master();
    }
    else
    {
    }

    LOG(INFO) << "finished. ctrl+C to quit.";
}