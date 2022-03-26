#include "DirectoryConnection.h"

#include <glog/logging.h>

#include "Connection.h"
#include "Timer.h"

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
    // dinfo("[dirCon] dirID: %d, ctx->pd: %p", dirID, ctx.pd);
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
        this->lockMR = createMemoryRegionOnChip(
            (uint64_t) this->dmPool, this->lockSize, &ctx);
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
            // LOG(INFO) << "[debug] trying to free dirToAppAh[" << dirID <<
            // "]["
            //           << i << "] val "
            //           << (void *) (*remoteInfo)[node_id].dirToAppAh[dirID][i]
            //           << " to node_id " << node_id;

            PLOG_IF(ERROR, ibv_destroy_ah(pah)) << "failed to destroy ah";
            (*remoteInfo)[node_id].dirToAppAh[dirID][i] = nullptr;
        }
    }
    // LOG(INFO) << "!!! [debug] freeing context " << (void *) &ctx << " with pd
    // "
    //           << (void *) ctx.pd;
    CHECK(destroyContext(&ctx));
    timer.pin("destroy context (dealloc PD, close device)");
    timer.report();
}