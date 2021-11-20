#include "DSM.h"
#include "Timer.h"

// Two nodes
// one node issues cas operations

constexpr uint16_t kClientNodeId = 1;
constexpr uint16_t kServerNodeId = 0;
constexpr uint32_t kMachineNr = 2;

void client()
{
    info("client: TODO");
    while (1)
    {
        sleep(1);
    }
}
int main()
{
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
        client();
    }

    auto& cache = dsm->get_internal_buffer();
    char *buffer = (char*) cache.data;
    size_t max_size = cache.size;
    constexpr static size_t kMemoryWindowSize = 4 * 1024;

    {
        Timer timer;

        printf("\n-------- alloc mw ----------\n");
        timer.begin();
        size_t mw_nr = 10;
        std::vector<ibv_mw *> mws;
        mws.resize(mw_nr);
        for (size_t i = 0; i < mw_nr; ++i)
        {
            mws[i] = dsm->alloc_mw();
        }
        timer.end_print(mw_nr);

        printf("\n-------- bind mw ----------\n");
        check(kMemoryWindowSize * mw_nr < max_size,
              "mw_nr %lu too large, overflow an rdma buffer.", mw_nr);
        timer.begin();
        for (size_t i = 0; i < mw_nr; ++i)
        {
            const char* buffer_start = buffer + i * kMemoryWindowSize;
            dsm->bind_memory_region(mws[i], buffer_start, kMemoryWindowSize, kClientNodeId);
        }
        dsm->poll_rdma_cq(mw_nr);
        timer.end(mw_nr);

        printf("\n-------- free mw ----------\n");
        timer.begin();
        for (size_t i = 0; i < mw_nr; ++i)
        {
            dsm->free_mw(mws[i]);
        }
        timer.end_print(mw_nr);
    }

    printf("OK\n");

    while (true)
        ;
}