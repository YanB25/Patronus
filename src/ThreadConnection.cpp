#include "ThreadConnection.h"

#include "Connection.h"

ThreadConnection::ThreadConnection(uint16_t threadID, void *cachePool,
                                   uint64_t cacheSize, uint32_t machineNR,
                                   RemoteConnection *remoteInfo,
                                   const uint8_t mac[6])
    : threadID(threadID), remoteInfo(remoteInfo) {
    createContext(&ctx);
    cq = ibv_create_cq(ctx.ctx, RAW_RECV_CQ_COUNT, NULL, NULL, 0);

    message = new RawMessageConnection(ctx, cq, APP_MESSAGE_NR);

    this->cachePool = cachePool;
    cacheMR = createMemoryRegion((uint64_t)cachePool, cacheSize, &ctx);
    cacheLKey = cacheMR->lkey;

    // dir, RC
    for (int i = 0; i < NR_DIRECTORY; ++i) {
        data[i] = new ibv_qp *[machineNR];
        for (size_t k = 0; k < machineNR; ++k) {
            createQueuePair(&data[i][k], IBV_QPT_RC, cq, &ctx);
        }
    }

}
