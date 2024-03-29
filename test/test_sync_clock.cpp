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

void client([[maybe_unused]] std::shared_ptr<DSM> dsm)
{
    // auto& cm = dsm->clock_manager();
    // cm.init_sync_clock();
    // while (true)
    // {
    //     sleep(1);
    //     cm.sync_clock();
    // }
}

void server([[maybe_unused]] std::shared_ptr<DSM> dsm)
{
    // auto& cm = dsm->clock_manager();
    // cm.init_sync_clock();
    // while (true)
    // {
    //     sleep(1);
    //     cm.sync_clock();
    // }
}
int main()
{
    // if (argc < 3)
    // {
    //     fprintf(stderr, "%s [window_nr] [window_size]\n", argv[0]);
    //     return -1;
    // }
    // size_t window_nr = 0;
    // size_t window_size = 0;
    // sscanf(argv[1], "%lu", &window_nr);
    // sscanf(argv[2], "%lu", &window_size);

    // printf("window_nr: %lu, window_size: %lu\n", window_nr, window_size);

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
    while (1)
    {
        sleep(1);
    }
}