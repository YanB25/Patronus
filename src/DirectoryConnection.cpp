#include "DirectoryConnection.h"

#include "Connection.h"

DirectoryConnection::DirectoryConnection(
    uint16_t dirID,
    void *dsmPool,
    uint64_t dsmSize,
    uint32_t machineNR,
    const std::vector<RemoteConnection> &remoteInfo)
    : dirID(dirID), remoteInfo(remoteInfo)
{
    createContext(&ctx);
    cq = ibv_create_cq(ctx.ctx, RAW_RECV_CQ_COUNT, NULL, NULL, 0);
    rpc_cq = ibv_create_cq(ctx.ctx, RAW_RECV_CQ_COUNT, NULL, NULL, 0);
    message = RawMessageConnection::newInstance(ctx, rpc_cq, DIR_MESSAGE_NR);

    message->initRecv();
    message->initSend();

    // dsm memory
    this->dsmPool = dsmPool;
    this->dsmSize = dsmSize;
    this->dsmMR = createMemoryRegion((uint64_t) dsmPool, dsmSize, &ctx);
    dinfo("DSM CreateMemoryRegion at %p, size %ld", dsmPool, dsmSize);
    this->dsmLKey = dsmMR->lkey;

    // on-chip lock memory
    if (dirID == 0)
    {
        this->dmPool = (void *) define::kLockStartAddr;
        this->lockSize = define::kLockChipMemSize;
        this->lockMR = createMemoryRegionOnChip(
            (uint64_t) this->dmPool, this->lockSize, &ctx);
        this->lockLKey = lockMR->lkey;
    }

    // app, RC
    for (int i = 0; i < MAX_APP_THREAD; ++i)
    {
        QPs.emplace_back(machineNR);
        for (size_t k = 0; k < machineNR; ++k)
        {
            createQueuePair(&QPs.back()[k], IBV_QPT_RC, cq, &ctx);
        }
    }
}

void DirectoryConnection::sendMessage2App(RawMessage *m,
                                          uint16_t node_id,
                                          uint16_t th_id)
{
    message->sendRawMessage(m,
                            remoteInfo[node_id].appMessageQPN[th_id],
                            remoteInfo[node_id].dirToAppAh[dirID][th_id]);
}
