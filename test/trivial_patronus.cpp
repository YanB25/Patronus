#include <algorithm>
#include <random>

#include "Timer.h"
#include "patronus/Patronus.h"
#include "util/monitor.h"

// Two nodes
// one node issues cas operations

constexpr uint16_t kClientNodeId = 0;
[[maybe_unused]] constexpr uint16_t kServerNodeId = 1;
constexpr uint32_t kMachineNr = 2;

using namespace patronus;

void client(Patronus::pointer p)
{
    auto tid = p->get_thread_id();
    LOG(INFO) << "I am client. tid " << tid;
}
void server(Patronus::pointer p)
{
    auto tid = p->get_thread_id();
    LOG(INFO) << "I am server. tid " << tid;
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    rdmaQueryDevice();

    PatronusConfig config;
    config.machine_nr = kMachineNr;

    auto patronus = Patronus::ins(config);

    sleep(1);

    // let client spining
    auto nid = patronus->get_node_id();
    if (nid == kClientNodeId)
    {
        patronus->registerClientThread();
        client(patronus);
    }
    else
    {
        patronus->registerServerThread();
        server(patronus);
    }

    LOG(INFO) << "finished. ctrl+C to quit.";
}