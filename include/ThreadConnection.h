#ifndef __THREADCONNECTION_H__
#define __THREADCONNECTION_H__

#include "Common.h"
#include "RawMessageConnection.h"

struct RemoteConnection;

// app thread
struct ThreadConnection {

    uint16_t threadID;

    RdmaContext ctx;
    ibv_cq *cq;

    RawMessageConnection *message;

    ibv_qp **data[NR_DIRECTORY];

    ibv_mr *cacheMR;
    void *cachePool;
    uint32_t cacheLKey;
    RemoteConnection *remoteInfo;

    ThreadConnection(uint16_t threadID, void *cachePool, uint64_t cacheSize,
                     uint32_t machineNR, RemoteConnection *remoteInfo,
                     const uint8_t mac[6]);

    void sendMessage(RawMessage *m) { message->sendRawMessage(m); }
};

#endif /* __THREADCONNECTION_H__ */
