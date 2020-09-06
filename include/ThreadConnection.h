#ifndef __THREADCONNECTION_H__
#define __THREADCONNECTION_H__

#include "Common.h"
#include "RawMessageConnection.h"

struct RemoteConnection;

/**
 * @brief ThreadConnection is an abstraction of application thread
 */
struct ThreadConnection
{
    uint16_t threadID;

    RdmaContext ctx;
    ibv_cq *cq;  // for one-side verbs
    ibv_cq *rpc_cq;

    RawMessageConnection *message;

    /**
     * data contains NR_DIRECTORY * machineNR QP
     */
    ibv_qp **data[NR_DIRECTORY];

    ibv_mr *cacheMR;
    void *cachePool;
    uint32_t cacheLKey;
    const std::vector<RemoteConnection> &remoteInfo;

    ThreadConnection(uint16_t threadID,
                     void *cachePool,
                     uint64_t cacheSize,
                     uint32_t machineNR,
                     const std::vector<RemoteConnection> &remoteInfo);

    void sendMessage2Dir(RawMessage *m, uint16_t node_id, uint16_t dir_id = 0);
};

#endif /* __THREADCONNECTION_H__ */
