#include "DSM.h"
#include "Timer.h"

// Two nodes
// one node issues cas operations

constexpr uint16_t kClientNodeId = 0;
constexpr uint16_t kServerNodeId = 1;
constexpr uint32_t kMachineNr = 2;

void client(std::shared_ptr<DSM> dsm)
{
    info("client: TODO");
}

void server(std::shared_ptr<DSM> dsm, size_t mw_nr, size_t window_size)
{
    auto &cache = dsm->get_internal_buffer();
    char *buffer = (char *) cache.data;
    size_t max_size = cache.size;

    {
        Timer timer;

        printf("\n-------- alloc mw ----------\n");
        timer.begin();
        std::vector<ibv_mw *> mws;
        mws.resize(mw_nr);
        for (size_t i = 0; i < mw_nr; ++i)
        {
            mws[i] = dsm->alloc_mw();
        }
        timer.end_print(mw_nr);

        printf("\n-------- bind mw ----------\n");
        check(window_size < max_size,
              "mw_nr %lu too large, overflow an rdma buffer.",
              mw_nr);
        timer.begin();
        size_t window_nr = max_size / window_size;
        for (size_t i = 0; i < mw_nr; ++i)
        {
            // if i too large, we roll back i to 0.
            const char *buffer_start =
                buffer + (i % window_nr) * window_size;
            dsm->bind_memory_region(
                mws[i], buffer_start, window_size, kClientNodeId);
        }
        dsm->poll_rdma_cq(mw_nr);
        timer.end_print(mw_nr);

        printf("\n-------- free mw ----------\n");
        timer.begin();
        for (size_t i = 0; i < mw_nr; ++i)
        {
            dsm->free_mw(mws[i]);
        }
        timer.end_print(mw_nr);
    }
}
int main(int argc, char** argv)
{
    if (argc < 3)
    {
        fprintf(stderr, "%s [window_nr] [window_size]\n", argv[0]);
        return -1;
    }
    size_t window_nr = 0;
    size_t window_size = 0;
    sscanf(argv[1], "%lu", &window_nr);
    sscanf(argv[2], "%lu", &window_size);

    printf("window_nr: %lu, window_size: %lu\n", window_nr, window_size);

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
        server(dsm, window_nr, window_size);
    }
    while (1)
    {
        sleep(1);
    }
}