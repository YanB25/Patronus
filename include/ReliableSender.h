#pragma once
#ifndef RELIABLE_SENDER_H_
#define RELIABLE_SENDER_H_
#include "ReliableMessageConnection.h"

class ReliableSendMessageConnection
{
    constexpr static size_t kMessageSize = 32;
    const static int kSendBatch = 16;

public:
    ReliableSendMessageConnection(std::vector<std::vector<ibv_qp *>> &QPs,
                                  ibv_cq *cq, uint32_t lkey);
    ~ReliableSendMessageConnection();
    void send(size_t node_id, const char *buf, size_t size, size_t targetID);

private:
    void poll_cq();

    /**
     * @brief QPs[RMSG_MULTIPLEXING][machineNR]
     */
    std::vector<std::vector<ibv_qp *>> &QPs_;
    ibv_cq *send_cq_{nullptr};

    using Aligned = std::aligned_storage<sizeof(std::atomic<size_t>), 128>::type;
    Aligned msg_send_indexes_inner_[RMSG_MULTIPLEXING];
    std::atomic<size_t>* msg_send_indexes_{nullptr};

    std::atomic<bool> second_{false};

    uint32_t lkey_{0};
};

#endif