#pragma once
#ifndef RELIABLE_RECEIVER_H_
#define RELIABLE_RECEIVER_H_

#include "ReliableMessageConnection.h"

class ReliableRecvMessageConnection
{
    constexpr static int kMessageSize = ReliableConnection::kMessageSize;
    constexpr static size_t kRecvBuffer = ReliableConnection::kRecvBuffer;

    const static size_t kPostRecvBufferAdvanceBatch =
        ReliableConnection::kPostRecvBufferAdvanceBatch;
    const static size_t kPostRecvBufferBatch =
        ReliableConnection::kPostRecvBufferBatch;

public:
    ReliableRecvMessageConnection(std::vector<std::vector<ibv_qp *>> &QPs,
                                  ibv_cq *cq,
                                  void *msg_pool,
                                  uint32_t lkey);
    ~ReliableRecvMessageConnection();
    void recv(char *ibuf, size_t msg_limit = 1);
    /**
     * @brief try to recv any buffered messages
     *
     * @param ibuf The buffered to store received messages
     * @param msg_limit The number of messages at most the ibuf can store
     * @return size_t The number of messages actually received.
     */
    size_t try_recv(char *ibuf, size_t msg_limit = 1);
    void init();

private:
    void handle_wc(char *ibuf, const ibv_wc &wc);
    void fills(ibv_sge &sge,
               ibv_recv_wr &wr,
               size_t threadID,
               size_t node_id,
               size_t batch_id);

    bool inited_{false};
    void poll_cq();

    /**
     * @brief maintain QPs[RMSG_MULTIPLEXING][machineNR]
     */
    std::vector<std::vector<ibv_qp *>> &QPs_;
    ibv_cq *recv_cq_{nullptr};
    void *msg_pool_{nullptr};
    uint32_t lkey_{0};

    size_t get_msg_pool_idx(size_t dirID, size_t node_id, size_t batch_id)
    {
        return dirID * kRecvBuffer * MAX_MACHINE + node_id * kRecvBuffer +
               batch_id;
    }

    ibv_recv_wr recvs[RMSG_MULTIPLEXING][MAX_MACHINE][kRecvBuffer];
    ibv_sge recv_sgl[RMSG_MULTIPLEXING][MAX_MACHINE][kRecvBuffer];

    using Aligned =
        std::aligned_storage<sizeof(std::atomic<size_t>), 128>::type;
    Aligned msg_recv_index_layout_[RMSG_MULTIPLEXING][MAX_MACHINE] = {};
};

#endif