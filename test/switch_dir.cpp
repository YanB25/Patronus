#include <algorithm>
#include <random>

#include "DSM.h"
#include "Timer.h"
#include "util/monitor.h"

// Two nodes
// one node issues cas operations

constexpr uint16_t kClientNodeId = 0;
[[maybe_unused]] constexpr uint16_t kServerNodeId = 1;
constexpr uint32_t kMachineNr = 2;

constexpr static uint64_t kMagic = 0xaaaaaaaaaaaaaaaa;
constexpr static uint64_t kMagic2 = 0xbbbbbbbbbbbbbbbb;
constexpr static uint64_t kMagic3 = 0xcccccccccccccccc;

void loop_expect(const char *lhs_buf, const char *rhs_buf, size_t size)
{
    while (memcmp(lhs_buf, rhs_buf, size) != 0)
    {
        printf("buf %p != expect. read %lx. expect %lx\n",
               lhs_buf,
               *(uint64_t *) lhs_buf,
               *(uint64_t *) rhs_buf);
        sleep(1);
    }
    printf("buf %p == expect!\n", lhs_buf);
}

void client(std::shared_ptr<DSM> dsm)
{
    GlobalAddress gaddr;
    gaddr.nodeID = kServerNodeId;
    gaddr.offset = 0;

    auto *buffer = dsm->get_rdma_buffer();
    *(uint64_t *) buffer = kMagic;

    dsm->write_sync(buffer, gaddr, sizeof(uint64_t));
    LOG(INFO) << "finished writing magic 1";
    // sync
    dsm->recv();
    dsm->roll_dir();

    *(uint64_t *) buffer = kMagic2;
    dsm->write_sync(buffer, gaddr, sizeof(uint64_t));
    LOG(INFO) << "finished writing magic 2";

    dsm->recv();
    // expect to roll back to 0 when NR_DIRECTORY == 2
    dsm->roll_dir();

    *(uint64_t *) buffer = kMagic3;
    dsm->write_sync(buffer, gaddr, sizeof(uint64_t));
    LOG(INFO) << "finished writing magic 3";
}

void server(std::shared_ptr<DSM> dsm)
{
    const auto &buf_conf = dsm->get_server_internal_buffer();
    char *buffer = buf_conf.buffer;

    loop_expect(buffer, (char *) &kMagic, sizeof(kMagic));

    dsm->send(nullptr, 0, kClientNodeId);

    loop_expect(buffer, (char *) &kMagic2, sizeof(kMagic2));

    // info("start to send 2nd msg");
    dsm->send(nullptr, 0, kClientNodeId);
    // info("send 2nd msg finished");

    loop_expect(buffer, (char *) &kMagic3, sizeof(kMagic3));
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

    LOG(INFO) << "finished. ctrl+C to quit.";
}