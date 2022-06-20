#include <iostream>
#include <thread>

#include "DSM.h"
#include "gflags/gflags.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    DSMConfig config;
    config.machineNR = ::config::kMachineNr;

    // Do all the things here
    auto dsm = DSM::getInstance(config);
    // When you reach here, all the things have done

    // You must call registerThread() before using DSM.
    dsm->registerThread();

    LOG(INFO) << "Connection build. node_id: " << dsm->get_node_id()
              << ", thread_id: " << dsm->get_thread_id();

    // See
    // DSM::read(...)
    // DSM::write(...)
    // DSM::cas(...)
    // For more information

    // See `test/atomic_latency` for the use of DSM.

    return 0;
}