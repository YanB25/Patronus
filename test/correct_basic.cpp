#include <algorithm>
#include <random>

#include "DSM.h"
#include "Timer.h"
#include "gflags/gflags.h"
#include "util/monitor.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

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
    config.machineNR = ::config::kMachineNr;

    auto dsm = DSM::getInstance(config);

    dsm->registerThread();
    auto server_nid = ::config::get_server_nids().front();

    // let client spining
    auto nid = dsm->getMyNodeID();
    if (::config::is_client(nid))
    {
        dsm->keeper_barrier("sync", 100ms);
        dsm->reconnectThreadToDir(server_nid, 0);
    }
    else
    {
        dsm->reinitializeDir(0);
        dsm->keeper_barrier("sync", 100ms);
    }

    LOG(INFO) << "finished. ctrl+C to quit.";
}