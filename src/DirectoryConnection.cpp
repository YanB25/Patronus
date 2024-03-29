#include "DirectoryConnection.h"

#include <glog/logging.h>

#include <chrono>
#include <thread>

#include "Connection.h"
#include "Timer.h"
using namespace std::chrono_literals;

/**
 * every DirectoryConnection has its own protection domain
 */
DirectoryConnection::DirectoryConnection(
    uint16_t dirID,
    void *dsmPool,
    uint64_t dsmSize,
    uint32_t machineNR,
    std::vector<RemoteConnection> &remoteInfo)
    : dirID(dirID), remoteInfo(&remoteInfo), machine_nr_(machineNR)
{
    DefOnceContTimer(timer,
                     config::kMonitorControlPath,
                     "DirectoryConnection::DirectoryConnection()");

    CHECK(createContext(&ctx));
    timer.pin("createContext");

    cq =
        CHECK_NOTNULL(ibv_create_cq(ctx.ctx, RAW_RECV_CQ_COUNT, NULL, NULL, 0));
    rpc_cq =
        CHECK_NOTNULL(ibv_create_cq(ctx.ctx, RAW_RECV_CQ_COUNT, NULL, NULL, 0));

    timer.pin("2x ibv_create_cq");
    message = RawMessageConnection::newInstance(ctx, rpc_cq, DIR_MESSAGE_NR);

    message->initRecv();
    message->initSend();
    timer.pin("message");

    // dsm memory
    this->dsmPool = dsmPool;
    this->dsmSize = dsmSize;
    this->dsmMR =
        CHECK_NOTNULL(createMemoryRegion((uint64_t) dsmPool, dsmSize, &ctx));
    timer.pin("createMR");
    this->dsmLKey = dsmMR->lkey;

    // on-chip lock memory
    if (dirID == 0)
    {
        this->dmPool = (void *) define::kLockStartAddr;
        this->lockSize = define::kLockChipMemSize;
        this->lockMR = nullptr;
        size_t retry_nr = 0;
        do
        {
            this->lockMR = createMemoryRegionOnChip(
                (uint64_t) this->dmPool, this->lockSize, &ctx);
            if (unlikely(this->lockMR == nullptr))
            {
                LOG(ERROR) << "Failed to create memory region on device memory "
                              "for size "
                           << this->lockSize << ". Sleep for a while.";
                std::this_thread::sleep_for(1s);
                retry_nr++;
                CHECK_LE(retry_nr, 60)
                    << "** Failed to create memory region on device memory. "
                       "Retry limit exceeded.";
            }
        } while (this->lockMR == nullptr);
        this->lockLKey = lockMR->lkey;
        timer.pin("createMR OnChip");
    }

    // app, RC
    for (int i = 0; i < kMaxAppThread; ++i)
    {
        QPs.emplace_back(machineNR);
        for (size_t k = 0; k < machineNR; ++k)
        {
            CHECK(createQueuePair(
                &QPs.back()[k], IBV_QPT_RC, cq, &ctx, 128, 0, nullptr));
        }
    }
    timer.pin("create QPs");

    timer.pin("reliable recv");
    timer.report();
}

void DirectoryConnection::sendMessage2App(RawMessage *m,
                                          uint16_t node_id,
                                          uint16_t th_id)
{
    message->sendRawMessage(m,
                            (*remoteInfo)[node_id].appMessageQPN[th_id],
                            (*remoteInfo)[node_id].dirToAppAh[dirID][th_id]);
}
DirectoryConnection::~DirectoryConnection()
{
    DefOnceContTimer(
        timer, config::kMonitorControlPath, "~DirectoryConnection()");
    for (const auto &qps : QPs)
    {
        for (ibv_qp *qp : qps)
        {
            CHECK(destroyQueuePair(qp));
        }
    }

    timer.pin("destroy QPs");
    if (dirID == 0)
    {
        CHECK(destroyMemoryRegionOnChip(this->lockMR, ctx.dm));
    }
    timer.pin("destroy on-chip MRs");
    CHECK(destroyMemoryRegion(this->dsmMR));
    timer.pin("destroy off-chip MRs");

    if (message)
    {
        message->destroy();
    }
    timer.pin("destroy message");

    CHECK(destroyCompleteQueue(cq));
    CHECK(destroyCompleteQueue(rpc_cq));
    timer.pin("destroy cqs");
    // must free AH before freeing PD, otherwise it crashes when trying to free
    // AH.
    for (size_t node_id = 0; node_id < machine_nr_; ++node_id)
    {
        for (size_t i = 0; i < kMaxAppThread; ++i)
        {
            ibv_ah *pah = (*remoteInfo)[node_id].dirToAppAh[dirID][i];
            if (unlikely(pah == nullptr))
            {
                continue;
            }

            PLOG_IF(ERROR, ibv_destroy_ah(pah)) << "failed to destroy ah";
            (*remoteInfo)[node_id].dirToAppAh[dirID][i] = nullptr;
        }
    }

    CHECK(destroyContext(&ctx));
    timer.pin("destroy context (dealloc PD, close device)");
    timer.report();
}