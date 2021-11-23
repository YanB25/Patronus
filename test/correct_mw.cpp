#include <algorithm>
#include <random>

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
    GlobalAddress gaddr;
    gaddr.nodeID = kServerNodeId;
    gaddr.offset = 1024;

    auto *buffer = dsm->get_rdma_buffer();

    uint64_t magic = 0xabcdef0123456789;
    dsm->write_sync(buffer, gaddr, sizeof(magic));
    info("write finished.");
}
// Notice: TLS object is created only once for each combination of type and
// thread. Only use this when you prefer multiple callers share the same
// instance.
template <class T, class... Args>
inline T &TLS(Args &&...args)
{
    thread_local T _tls_item(std::forward<Args>(args)...);
    return _tls_item;
}
inline std::mt19937 &rand_generator()
{
    return TLS<std::mt19937>();
}

// [min, max]
uint64_t rand_int(uint64_t min, uint64_t max)
{
    std::uniform_int_distribution<uint64_t> dist(min, max);
    return dist(rand_generator());
}

void server(std::shared_ptr<DSM> dsm)
{
    auto &cache = dsm->get_internal_buffer();
    char *buffer = (char *) cache.data;
    size_t max_size = cache.size;

    while (true)
    {
        uint64_t read;
        read = buffer[1024];
        printf("Read: %x\n", read);
        sleep(1);
    }

    
    constexpr static size_t kMWSize = 1024;

    // std::array<ibv_mw *, kMWSize> mws;
    // for (size_t i = 0; i < kMWSize; ++i)
    // {
    //     mws[i] = dsm->alloc_mw();
    // }
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
        server(dsm);
    }

    info("finished. ctrl+C to quit.");
    while (1)
    {
        sleep(1);
    }
}