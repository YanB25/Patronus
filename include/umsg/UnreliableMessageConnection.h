#pragma once
#ifndef UNRELIABLE_MESSAGE_CONNECTION_H_
#define UNRELIABLE_MESSAGE_CONNECTION_H_

#include <glog/logging.h>
#include <infiniband/verbs.h>

#include <memory>
#include <mutex>
#include <vector>

#include "Common.h"
#include "Config.h"
#include "Rdma.h"
#include "ValidityMutex.h"

template <size_t kReceiverNr>
class UnreliableRecvMessageConnection;
template <size_t kSenderNr>
class UnreliableSendMessageConnection;

template <size_t kSenderNr, size_t kReceiverNr>
class UnreliableConnection
{
public:
    const static size_t kRecvBuffer = config::umsg::kRecvBuffer;
    // post 2 batch in advance. must be 2.
    constexpr static size_t kPostRecvBufferAdvanceBatch =
        config::umsg::kPostRecvBufferAdvanceBatch;
    constexpr static size_t kPostRecvBufferBatch =
        config::umsg::kPostRecvBufferBatch;
    // better be cahceline alinged. e.g. multiple of 64
    constexpr static size_t kMessageSize = config::umsg::kMessageSize;
    /**
     * @brief how much send # before a signal
     */
    constexpr static size_t kSenderBatchSize = config::umsg::kSenderBatchSize;
    // so that, server can get maximum messages by ONE poll.
    constexpr static size_t kRecvLimit = config::umsg::kRecvLimit;
    constexpr static size_t kMaxRecvBuffer = config::umsg::kMaxRecvBuffer;

    using DebugMutex = ValidityMutex<config::kEnableValidityMutex>;

    /**
     * @brief Construct a new Reliable Connection object
     *
     * @param mm the rdma buffer
     * @param mmSize
     */
    ReliableConnection(uint64_t mm, size_t mmSize, size_t machine_nr);
    ~ReliableConnection();
    void send(size_t threadID,
              const char *buf,
              size_t size,
              uint16_t node_id,
              size_t mid);
    void recv(size_t mid, char *ibuf, size_t limit = 1);
    size_t try_recv(size_t mid, char *ibuf, size_t limit = 1);

private:
    RdmaContext &context()
    {
        return ctx_;
    }
    uint32_t rkey() const
    {
        return recv_mr_->rkey;
    }
    std::vector<std::vector<ibv_qp *>> &QPs()
    {
        return QPs_;
    }
    friend class DSMKeeper;
    // for both
    RdmaContext ctx_;
    std::array<ibv_qp *, kSenderNr> send_QPs_;
    std::array<ibv_qp *, kReceiverNr> recv_QPs_;
    // // for receiver
    // void *recv_msg_pool_{nullptr};
    // ibv_mr *recv_mr_{nullptr};
    // uint32_t recv_lkey_{0};
    std::array<ibv_cq *, kSenderNr> send_cqs_;
    std::array<ibv_cq *, kReceiverNr> recv_cqs_;
    // for sender
    ibv_mr *send_mr_{nullptr};
    uint32_t send_lkey_{0};
    std::array<ibv_cq *, RMSG_MULTIPLEXING> send_cqs_;

    std::unique_ptr<ReliableRecvMessageConnection> recv_;
    std::unique_ptr<ReliableSendMessageConnection> send_;

    std::array<DebugMutex, RMSG_MULTIPLEXING> debug_locks_;
};

#endif