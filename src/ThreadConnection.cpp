#include "ThreadConnection.h"

#include "Connection.h"

ThreadConnection::ThreadConnection(
    uint16_t threadID,
    void *cachePool,
    uint64_t cacheSize,
    uint32_t machineNR,
    const std::vector<RemoteConnection> &remoteInfo)
    : threadID(threadID), remoteInfo(&remoteInfo)
{
    createContext(&ctx);

    cq = ibv_create_cq(ctx.ctx, RAW_RECV_CQ_COUNT, NULL, NULL, 0);
    // rpc_cq = cq;
    rpc_cq = ibv_create_cq(ctx.ctx, RAW_RECV_CQ_COUNT, NULL, NULL, 0);

    message = new RawMessageConnection(ctx, rpc_cq, APP_MESSAGE_NR);

    this->cachePool = cachePool;
    cacheMR = createMemoryRegion((uint64_t) cachePool, cacheSize, &ctx);
    // dinfo("Create memory region at %p, size %lu", (char*) cachePool,
    // cacheSize);

    cacheLKey = cacheMR->lkey;

    // dir, RC
    for (int i = 0; i < NR_DIRECTORY; ++i)
    {
        QPs.emplace_back(machineNR);
        for (size_t k = 0; k < machineNR; ++k)
        {
            createQueuePair(&QPs.back()[k], IBV_QPT_RC, cq, &ctx);
            // dinfo("QPs[%lu][%lu]: qp: %p, cq: %p, lkey: %u. mr: %p",
            // QPs.size() - 1, k, QPs.back()[k], (char*)cq, cacheLKey, cacheMR);
        }
    }
}

ThreadConnection::~ThreadConnection()
{
    for (const auto& qps: QPs)
    {
        for (ibv_qp* qp: qps)
        {
            CHECK(destroyQueuePair(qp));
        }
    }
    CHECK(destroyMemoryRegion(cacheMR));
    if (message)
    {
        message->destroy();
        delete message;
    }
    CHECK(destroyCompleteQueue(rpc_cq));
    CHECK(destroyCompleteQueue(cq));
    CHECK(destroyContext(&ctx));

}

void ThreadConnection::sendMessage2Dir(RawMessage *m,
                                       uint16_t node_id,
                                       uint16_t dir_id,
                                       bool sync)
{
    const auto& remoteInfoObj = *remoteInfo;
    if (!sync)
    {
        message->sendRawMessage(
            m,
            remoteInfoObj[node_id].dirMessageQPN[dir_id],
            remoteInfoObj[node_id].appToDirAh[threadID][dir_id]);
    }
    else
    {
        message->syncSendRawMessage(
            m,
            remoteInfoObj[node_id].dirMessageQPN[dir_id],
            remoteInfoObj[node_id].appToDirAh[threadID][dir_id]);
    }
}