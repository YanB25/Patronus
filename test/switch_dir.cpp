#include <algorithm>
#include <random>

#include "DSM.h"
#include "Timer.h"
#include "util/monitor.h"
#include "Timer.h"

// Two nodes
// one node issues cas operations

constexpr uint16_t kClientNodeId = 0;
[[maybe_unused]] constexpr uint16_t kServerNodeId = 1;
constexpr uint32_t kMachineNr = 2;

constexpr static uint64_t kMagic = 0xffaaffaa;


void loop_expect(const char *lhs_buf, const char *rhs_buf, size_t size)
{
    while (memcmp(lhs_buf, rhs_buf, size) != 0)
    {
        printf("buf %p != expect\n", lhs_buf);
        sleep(1);
    }
    printf("buf %p == expect!\n", lhs_buf);
}

void client(std::shared_ptr<DSM> dsm)
{
    GlobalAddress gaddr;
    gaddr.nodeID = kServerNodeId;
    gaddr.offset = 0;

    auto* buffer = dsm->get_rdma_buffer();
    uint64_t magic = kMagic;
    *(uint64_t *) buffer = magic;

    dsm->write_sync(buffer, gaddr, sizeof(uint64_t));
}

void server(std::shared_ptr<DSM> dsm)
{
    info("Server sleep forever.");
    const auto& buf_conf = dsm->get_server_internal_buffer();
    char* buffer = buf_conf.buffer;

    loop_expect(buffer, (char*) &kMagic, sizeof(kMagic));

    while (true)
    {
        sleep(1);
    }
}

int main()
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
}