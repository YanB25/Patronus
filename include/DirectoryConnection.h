#pragma once
#ifndef __DIRECTORYCONNECTION_H__
#define __DIRECTORYCONNECTION_H__

#include "Common.h"
#include "RawMessageConnection.h"
#include "ReliableMessageConnection.h"
#include <vector>

struct RemoteConnection;

// directory thread
struct DirectoryConnection
{
    uint16_t dirID{0};

    RdmaContext ctx;
    /**
     * cq is a shared completion queue used in Reliable Connection QPs
     */
    ibv_cq *cq{nullptr};
    /**
     * cq is a completion queue used in Unreliable Datagram QP
     */
    ibv_cq *rpc_cq{nullptr};

    std::shared_ptr<RawMessageConnection> message;

    /**
     * @brief maintain QPs[MAX_APP_THREAD][machineNR]
     */
    std::vector<std::vector<ibv_qp *>> QPs;

    ibv_mr *dsmMR{nullptr};
    void *dsmPool{nullptr};
    uint64_t dsmSize{0};
    uint32_t dsmLKey{0};

    ibv_mr *lockMR{nullptr};
    void *dmPool{nullptr};  // address on-chip
    uint64_t lockSize{0};
    uint32_t lockLKey{0};

    std::vector<RemoteConnection> remoteInfo;

    DirectoryConnection(uint16_t dirID,
                        void *dsmPool,
                        uint64_t dsmSize,
                        uint32_t machineNR,
                        const std::vector<RemoteConnection> &remoteInfo);
    DirectoryConnection(DirectoryConnection&) = delete;
    DirectoryConnection& operator=(DirectoryConnection&) = delete;
    DirectoryConnection& operator=(DirectoryConnection&& rhs)
    {
        dirID = rhs.dirID;
        ctx = std::move(rhs.ctx);
        cq = rhs.cq;
        rhs.cq = nullptr;
        rpc_cq = rhs.rpc_cq;
        rhs.rpc_cq = nullptr;
        message = rhs.message;
        rhs.message = nullptr;
        QPs = std::move(rhs.QPs);
        dsmMR = rhs.dsmMR;
        rhs.dsmMR = nullptr;
        dsmPool = rhs.dsmPool;
        rhs.dsmPool = nullptr;
        dsmSize = rhs.dsmSize;
        dsmLKey = rhs.dsmLKey;
        lockMR = rhs.lockMR;
        rhs.lockMR = nullptr;
        dmPool = rhs.dmPool;
        rhs.dmPool = nullptr;
        lockSize = rhs.lockSize;
        lockLKey = rhs.lockLKey;
        remoteInfo = std::move(rhs.remoteInfo);
        return *this;
    }
    DirectoryConnection(DirectoryConnection&& rhs)
    {
        *this = std::move(rhs);
    }

    void sendMessage2App(RawMessage *m, uint16_t node_id, uint16_t th_id);
    ~DirectoryConnection();
};

#endif /* __DIRECTORYCONNECTION_H__ */
