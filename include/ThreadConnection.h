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
    uint16_t threadID{0};

    RdmaContext ctx;
    /**
     * cq is a shared completion queue used in Reliable Connection QPs
     */
    ibv_cq *cq{nullptr};
    /**
     * cq is a completion queue used in Unreliable Datagram QP
     */
    ibv_cq *rpc_cq{nullptr};

    RawMessageConnection *message{nullptr};

    /**
     * @brief maintain QPs[NR_DIRECTORY][machineNR]
     */
    std::vector<std::vector<ibv_qp *>> QPs;

    ibv_mr *cacheMR{nullptr};
    void *cachePool{nullptr};
    uint32_t cacheLKey{0};
    const std::vector<RemoteConnection>* remoteInfo{nullptr};

    ThreadConnection(uint16_t threadID,
                     void *cachePool,
                     uint64_t cacheSize,
                     uint32_t machineNR,
                     const std::vector<RemoteConnection> &remoteInfo);
    ThreadConnection(const ThreadConnection&) = delete;
    ThreadConnection& operator=(const ThreadConnection&) = delete;
    ThreadConnection& operator=(ThreadConnection&& rhs)
    {
        threadID = std::move(rhs.threadID);
        ctx = std::move(rhs.ctx);
        cq = std::move(rhs.cq);
        rhs.cq = nullptr;
        rpc_cq = std::move(rhs.rpc_cq);
        rhs.rpc_cq = nullptr;
        message = std::move(rhs.message);
        rhs.message = nullptr;
        QPs = std::move(rhs.QPs);
        cacheMR = std::move(rhs.cacheMR);
        rhs.cacheMR = nullptr;
        cachePool = std::move(rhs.cachePool);
        rhs.cachePool = nullptr;
        cacheLKey = std::move(rhs.cacheLKey);
        remoteInfo = std::move(rhs.remoteInfo);
        rhs.remoteInfo = nullptr;
        return *this;
    }
    ThreadConnection(ThreadConnection&& rhs)
    {
        (*this) = std::move(rhs);
    }
    void sendMessage2Dir(RawMessage *m, uint16_t node_id, uint16_t dir_id = 0, bool eager_signal = false);
    ~ThreadConnection();
};

#endif /* __THREADCONNECTION_H__ */
