#include <algorithm>
#include <random>

#include "patronus/Patronus.h"
#include "Timer.h"
#include "util/monitor.h"
#include "Timer.h"

// Two nodes
// one node issues cas operations

constexpr uint16_t kClientNodeId = 0;
[[maybe_unused]] constexpr uint16_t kServerNodeId = 1;
constexpr uint32_t kMachineNr = 2;

using namespace patronus;

void client(std::unique_ptr<Patronus> p)
{
    auto tid = p->thread_id();
    LOG(INFO) << "I am client. tid " << tid;
}
void server(std::unique_ptr<Patronus> p)
{
    auto tid = p->thread_id();
    LOG(INFO) << "I am server. tid " << tid;
}

int main(int argc, char* argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    bindCore(0);

    rdmaQueryDevice();

    DSMConfig config;
    config.machineNR = kMachineNr;

    // auto dsm = DSM::getInstance(config);
    auto patronus = Patronus::ins(config);

    sleep(1);

    patronus->registerThread();

    // let client spining
    auto nid = patronus->node_id();
    if (nid == kClientNodeId)
    {
        client(std::move(patronus));
    }
    else
    {
        server(std::move(patronus));
    }

    LOG(INFO) << "finished. ctrl+C to quit.";
}