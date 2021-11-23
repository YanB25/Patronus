#include <algorithm>

#include "DSM.h"
#include "Timer.h"
#include "util/monitor.h"

// Two nodes
// one node issues cas operations

constexpr uint16_t kClientNodeId = 0;
constexpr uint16_t kServerNodeId = 1;
constexpr uint32_t kMachineNr = 2;

void client(std::shared_ptr<DSM> dsm)
{
    info("client: TODO");
}

std::atomic<double> alloc_mw_ns;
std::atomic<double> free_mw_ns;
std::atomic<double> bind_mw_ns;

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
        alloc_mw_ns = timer.end(mw_nr);
        timer.print();

        printf("\n-------- bind mw ----------\n");
        check(window_size < max_size,
              "mw_nr %lu too large, overflow an rdma buffer.",
              mw_nr);
        timer.begin();
        size_t nr_per_poll = 100;
        size_t remain_nr = mw_nr;
        size_t window_nr = max_size / window_size;
        while (remain_nr > 0)
        {
            size_t work_nr = std::min(remain_nr, nr_per_poll);
            for (size_t i = 0; i < work_nr; ++i)
            {
                // if i too large, we roll back i to 0.
                const char *buffer_start =
                    buffer + (i % window_nr) * window_size;
                dsm->bind_memory_region(
                    mws[i], buffer_start, window_size, kClientNodeId);
            }
            dsm->poll_rdma_cq(work_nr);
            remain_nr -= work_nr;
        }
        bind_mw_ns = timer.end(mw_nr);
        timer.print();

        printf("\n-------- free mw ----------\n");
        timer.begin();
        for (size_t i = 0; i < mw_nr; ++i)
        {
            dsm->free_mw(mws[i]);
        }
        free_mw_ns = timer.end(mw_nr);
        timer.print();
    }
}
int main(int argc, char **argv)
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

    auto &m = bench::BenchManager::ins();
    auto &bench = m.reg("memory-window");
    bench.add_column("alloc-mw", &alloc_mw_ns)
        .add_column("bind-mw", &bind_mw_ns)
        .add_column("free-mw", &free_mw_ns);

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
        std::vector<size_t> window_nr_arr{
            100, 1000, 10000, 100000, 1000000ull, 10000000ull};
        std::vector<size_t> window_size_arr{4096};
        for (auto window_nr : window_nr_arr)
        {
            for (auto window_size : window_size_arr)
            {
                server(dsm, window_nr, window_size);
                bench.snapshot();
            }
        }
    }
    m.report("memory-window");

    info("finished. ctrl+C to quit.");
    while (1)
    {
        sleep(1);
    }
}