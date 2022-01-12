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

struct SwitchNotifyMsg
{
} __attribute__((packed));

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
    LOG(WARNING) << "I am client.";
    dsm->debug_show_exchanges();

    GlobalAddress gaddr;
    gaddr.nodeID = kServerNodeId;
    gaddr.offset = 0;

    auto *buffer = dsm->get_rdma_buffer();
    *(uint64_t *) buffer = kMagic;

    CHECK(dsm->write_sync(buffer, gaddr, sizeof(uint64_t), nullptr, 1));
    LOG(INFO) << "finished writing magic 1";
    // sync
    dsm->recv();
    dsm->roll_dir();

    *(uint64_t *) buffer = kMagic2;
    CHECK(dsm->write_sync(buffer, gaddr, sizeof(uint64_t), nullptr, 2));
    LOG(INFO) << "finished writing magic 2";

    dsm->recv();
    dsm->reconnectThreadToDir(kServerNodeId, 0 /* dirID */);
    // // test if the 0 QP ready
    dsm->force_set_dir(0);

    *(uint64_t *) buffer = kMagic3;
    CHECK(dsm->write_sync(buffer, gaddr, sizeof(uint64_t), nullptr, 3));
    LOG(INFO) << "finished writing magic 3";

    dsm->debug_show_exchanges();
}

void server(std::shared_ptr<DSM> dsm)
{
    LOG(WARNING) << "I am server.";
    dsm->debug_show_exchanges();

    const auto &buf_conf = dsm->get_server_buffer();
    char *buffer = buf_conf.buffer;

    loop_expect(buffer, (char *) &kMagic, sizeof(kMagic));

    dsm->send(nullptr, 0, kClientNodeId);

    LOG(INFO) << "Server starts to reinit dir 0";
    dsm->reinitializeDir(0);

    // loop_expect(buffer, (char *) &kMagic2, sizeof(kMagic2));

    // info("start to send 2nd msg");
    dsm->send(nullptr, 0, kClientNodeId);
    dsm->debug_show_exchanges();
    // info("send 2nd msg finished");

    loop_expect(buffer, (char *) &kMagic3, sizeof(kMagic3));
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

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