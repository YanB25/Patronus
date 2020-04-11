#ifndef __DIRECTORYCONNECTION_H__
#define __DIRECTORYCONNECTION_H__ 

#include "Common.h"
#include "RawMessageConnection.h"

struct RemoteConnection;

// directory thread
struct DirectoryConnection {
    uint16_t dirID;

    RdmaContext ctx;
    ibv_cq *cq;

    RawMessageConnection *message;

    ibv_qp **data2app[MAX_APP_THREAD];

    ibv_mr *dsmMR;

    void *dsmPool;
    uint64_t dsmSize; // Bytes

    uint32_t dsmLKey;
    RemoteConnection *remoteInfo;

    DirectoryConnection(uint16_t dirID, void *dsmPool, uint64_t dsmSize, uint32_t machineNR,
                        RemoteConnection *remoteInfo, const uint8_t mac[8]);

    void sendMessage(RawMessage *m) { message->sendRawMessage(m); }
};

#endif /* __DIRECTORYCONNECTION_H__ */
