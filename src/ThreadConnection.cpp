#include "ThreadConnection.h"

#include <glog/logging.h>

#include "Connection.h"
#include "ReliableMessageConnection.h"
#include "Timer.h"

ThreadConnection::ThreadConnection(
    uint16_t threadID,
    void *cachePool,
    uint64_t cacheSize,
    uint32_t machineNR,
    const std::vector<RemoteConnection> &remoteInfo)
    : threadID(threadID), remoteInfo(&remoteInfo)
{
    DefOnceContTimer(timer,
                     config::kMonitorControlPath,
                     "ThreadConnection::ThreadConnection()");

    CHECK(createContext(&ctx));
    timer.pin("createContext");

    cq = ibv_create_cq(ctx.ctx, RAW_RECV_CQ_COUNT, NULL, NULL, 0);
    // rpc_cq = cq;
    rpc_cq = ibv_create_cq(ctx.ctx, RAW_RECV_CQ_COUNT, NULL, NULL, 0);

    timer.pin("3x ibv_create_cq");

    message = new RawMessageConnection(ctx, rpc_cq, APP_MESSAGE_NR);
    timer.pin("RawMessageConnection");

    this->cachePool = cachePool;
    cacheMR = createMemoryRegion((uint64_t) cachePool, cacheSize, &ctx);
    // dinfo("Create memory region at %p, size %lu", (char*) cachePool,
    // cacheSize);
    timer.pin("CreateMemoryRegion");

    cacheLKey = cacheMR->lkey;

    // dir, RC
    for (int i = 0; i < NR_DIRECTORY; ++i)
    {
        QPs.emplace_back(machineNR);
        for (size_t k = 0; k < machineNR; ++k)
        {
            CHECK(createQueuePair(&QPs.back()[k], IBV_QPT_RC, cq, &ctx, 128, 0, nullptr));
        }
    }
    timer.pin("CreateQPs");
    timer.pin("InitReliableSend");

    timer.report();
}

bool ThreadConnection::resetQP(size_t node_id, size_t dir_id)
{
    auto *qp = QPs[dir_id][node_id];
    if (!modifyQPtoReset(qp))
    {
        return false;
    }
    return true;
}

ThreadConnection::~ThreadConnection()
{
    DefOnceContTimer(timer, config::kMonitorControlPath, "~ThreadConnection");

    for (const auto &qps : QPs)
    {
        for (ibv_qp *qp : qps)
        {
            CHECK(destroyQueuePair(qp));
        }
    }
    timer.pin("destroy QPs");
    CHECK(destroyMemoryRegion(cacheMR));
    timer.pin("destroy MRs");
    if (message)
    {
        message->destroy();
        delete message;
        message = nullptr;
    }
    timer.pin("destroy messages");
    CHECK(destroyCompleteQueue(rpc_cq));
    CHECK(destroyCompleteQueue(cq));
    timer.pin("destroy CQs");
    CHECK(destroyContext(&ctx));
    timer.pin("destroy ctx");
    timer.report();
}

void ThreadConnection::sendMessage2Dir(RawMessage *m,
                                       uint16_t node_id,
                                       uint16_t dir_id,
                                       bool sync)
{
    const auto &remoteInfoObj = *remoteInfo;
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