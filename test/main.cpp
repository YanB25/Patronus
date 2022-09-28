#include <iostream>
#include <thread>
#include <unordered_map>

#include "DSM.h"
#include "gflags/gflags.h"
#include "patronus/Patronus.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    DSMConfig config;
    config.machineNR = ::config::kMachineNr;
    auto dsm = DSM::getInstance(config);

    LOG(INFO) << "IP is " << getIP();
    std::filesystem::path pwd = std::filesystem::current_path();
    LOG(INFO) << "pwd: " << pwd;

    return 0;
}