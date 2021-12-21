#pragma once
#ifndef RELIABLE_RECEIVER_H_
#define RELIABLE_RECEIVER_H_

#include "ReliableMessageConnection.h"

class ReliableRecvMessageConnection
{
    // post 2 batch in advance.
    const static int kPostRecvBufferAdvanceBatch = 2;
    const static int kPostRecvBufferBatch = 64;

    constexpr static int kMessageSize = ReliableConnection::kMessageSize;
    constexpr static size_t kRecvBuffer = ReliableConnection::kRecvBuffer;

public:
    ReliableRecvMessageConnection(std::vector<std::vector<ibv_qp *>> &QPs,
                                  ibv_cq *cq,
                                  void *msg_pool,
                                  uint32_t lkey);
    ~ReliableRecvMessageConnection();
    void recv(char *ibuf);
    bool try_recv(char *ibuf);
    void init();

private:
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

    using Aligned = std::aligned_storage<sizeof(std::atomic<size_t>), 128>::type;
    Aligned msg_recv_index_layout_[RMSG_MULTIPLEXING][MAX_MACHINE] = {};
    /**
     * @brief Array[RMSG_MULTIPLEXING][MAX_MACHINE]
     */
    std::atomic<size_t> *msg_recv_index_;
    // std::array<std::atomic<size_t>, MAX_MACHINE> msg_recv_index_{};
};

#endif