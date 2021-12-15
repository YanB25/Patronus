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
    const std::vector<RemoteConnection> &remoteInfo)
    : dirID(dirID), remoteInfo(remoteInfo)
{
    DefOnceContTimer(timer,
                     config::kMonitorControlPath,
                     "DirectoryConnection::DirectoryConnection()");

    CHECK(createContext(&ctx));
    // dinfo("[dirCon] dirID: %d, ctx->pd: %p", dirID, ctx.pd);
    timer.pin("createContext");

    cq = ibv_create_cq(ctx.ctx, RAW_RECV_CQ_COUNT, NULL, NULL, 0);
    rpc_cq = ibv_create_cq(ctx.ctx, RAW_RECV_CQ_COUNT, NULL, NULL, 0);
    timer.pin("2x ibv_create_cq");
    message = RawMessageConnection::newInstance(ctx, rpc_cq, DIR_MESSAGE_NR);

    message->initRecv();
    message->initSend();
    timer.pin("message");

    // dsm memory
    this->dsmPool = dsmPool;
    this->dsmSize = dsmSize;
    this->dsmMR = createMemoryRegion((uint64_t) dsmPool, dsmSize, &ctx);
    timer.pin("createMR");
    // dinfo(
    //     "[DSM] CreateMemoryRegion at %p, size %ld. mr: %p, lkey: %u, rkey:
    //     %u. pd: %p", dsmPool, dsmSize, dsmMR, dsmMR->lkey, dsmMR->rkey,
    //     ctx.pd);
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
    for (int i = 0; i < MAX_APP_THREAD; ++i)
    {
        QPs.emplace_back(machineNR);
        for (size_t k = 0; k < machineNR; ++k)
        {
            createQueuePair(&QPs.back()[k], IBV_QPT_RC, cq, &ctx);
            // dinfo(
            //     "Directory: bingding QP: QPs[%lu][%lu]: qp: %p, cq: %p, lkey:
            //     "
            //     "%u. mr: %p. pd: %p",
            //     QPs.size() - 1,
            //     k,
            //     QPs.back()[k],
            //     (char *) cq,
            //     dsmLKey,
            //     dsmMR,
            //     ctx.pd);
        }
    }
    timer.pin("create QPs");
    timer.report();
}

void DirectoryConnection::sendMessage2App(RawMessage *m,
                                          uint16_t node_id,
                                          uint16_t th_id)
{
    message->sendRawMessage(m,
                            remoteInfo[node_id].appMessageQPN[th_id],
                            remoteInfo[node_id].dirToAppAh[dirID][th_id]);
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
    CHECK(destroyContext(&ctx));
    timer.pin("destroy context (dealloc PD, close device)");
    timer.report();
}