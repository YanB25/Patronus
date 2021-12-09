#ifndef __THREADCONNECTION_H__
#define __THREADCONNECTION_H__

#include "Common.h"
#include "RawMessageConnection.h"
#include <vector>

struct RemoteConnection;

/**
 * @brief ThreadConnection is an abstraction of application thread
 */
struct ThreadConnection
{
    uint16_t threadID;

    RdmaContext ctx;
    /**
     * cq is a shared completion queue used in Reliable Connection QPs
     */
    ibv_cq *cq;
    /**
     * cq is a completion queue used in Unreliable Datagram QP
     */
    ibv_cq *rpc_cq;

    RawMessageConnection *message;

    /**
     * @brief maintain QPs[NR_DIRECTORY][machineNR]
     */
    std::vector<std::vector<ibv_qp *>> QPs;

    ibv_mr *cacheMR;
    void *cachePool;
    uint32_t cacheLKey;
    const std::vector<RemoteConnection> &remoteInfo;

    ThreadConnection(uint16_t threadID,
                     void *cachePool,
                     uint64_t cacheSize,
                     uint32_t machineNR,
                     const std::vector<RemoteConnection> &remoteInfo);

    void sendMessage2Dir(RawMessage *m, uint16_t node_id, uint16_t dir_id = 0, bool eager_signal = false);
};

#endif /* __THREADCONNECTION_H__ */
