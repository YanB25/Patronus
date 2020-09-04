#include "DSM.h"

int main()
{
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