#ifndef __DIRECTORYCONNECTION_H__
#define __DIRECTORYCONNECTION_H__

#include "Common.h"
#include "RawMessageConnection.h"
#include <vector>

struct RemoteConnection;

// directory thread
struct DirectoryConnection
{
    uint16_t dirID;

    RdmaContext ctx;
    /**
     * cq is a shared completion queue used in Reliable Connection QPs
     */
    ibv_cq *cq;
    /**
     * cq is a completion queue used in Unreliable Datagram QP
     */
    ibv_cq *rpc_cq;

    std::shared_ptr<RawMessageConnection> message;

    /**
     * @brief maintain QPs for every MAX_APP_THREAD of every machineNR
     */
    std::vector<std::vector<ibv_qp *>> QPs;

    ibv_mr *dsmMR;
    void *dsmPool;
    uint64_t dsmSize;
    uint32_t dsmLKey;

    ibv_mr *lockMR;
    void *dmPool;  // address on-chip
    uint64_t lockSize;
    uint32_t lockLKey;

    const std::vector<RemoteConnection> remoteInfo;

    DirectoryConnection(uint16_t dirID,
                        void *dsmPool,
                        uint64_t dsmSize,
                        uint32_t machineNR,
                        const std::vector<RemoteConnection> &remoteInfo);

    void sendMessage2App(RawMessage *m, uint16_t node_id, uint16_t th_id);
};

#endif /* __DIRECTORYCONNECTION_H__ */
