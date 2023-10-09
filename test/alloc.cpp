#include "DSM.h"
#include "gflags/gflags.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    DSMConfig config;
    config.machineNR = 2;
    auto dsm = DSM::getInstance(config);

    sleep(4);

    dsm->registerThread();

    for (int i = 0; i < 102; i++)
    {
        std::cout << dsm->alloc(1024 * 1024) << std::endl;
    }

    while (true)
        ;
}