#pragma once
#ifndef RELIABLE_MESSAGE_CONNECTION_H_
#define RELIABLE_MESSAGE_CONNECTION_H_

#include <glog/logging.h>
#include <infiniband/verbs.h>

#include <memory>
#include <vector>

#include "Common.h"

class ReliableRecvMessageConnection;
class ReliableSendMessageConnection;

class ReliableConnection
{
public:
    const static size_t kRecvBuffer = 128;
    constexpr static size_t kMessageSize = 64;

    /**
     * @brief Construct a new Reliable Connection object
     *
     * @param mm the rdma buffer
     * @param mmSize
     */
    ReliableConnection(uint64_t mm, size_t mmSize, size_t machine_nr);
    ~ReliableConnection();
    void send(const char *buf, size_t size, uint16_t node_id, size_t mid);
    void recv(char *ibuf);
    bool try_recv(char *ibuf);

private:
    RdmaContext& context()
    {
        return ctx_;
    }
    uint32_t rkey() const
    {
        return recv_mr_->rkey;
    }
    std::vector<std::vector<ibv_qp*>>& QPs()
    {
        return QPs_;
    }
    friend class DSMKeeper;
    // for both
    RdmaContext ctx_;
    /**
     * @brief QPs[RMSG_MULTIPLEXING][machineNR]
     */
    std::vector<std::vector<ibv_qp *>> QPs_;
    // for receiver
    void *recv_msg_pool_{nullptr};
    ibv_mr *recv_mr_{nullptr};
    uint32_t recv_lkey_{0};
    ibv_cq *recv_cq_{nullptr};
    // for sender
    ibv_mr *send_mr_{nullptr};
    uint32_t send_lkey_{0};
    ibv_cq *send_cq_{nullptr};

    std::unique_ptr<ReliableRecvMessageConnection> recv_;
    std::unique_ptr<ReliableSendMessageConnection> send_;
};

#endif