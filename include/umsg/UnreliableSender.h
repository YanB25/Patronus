#pragma once
#ifndef UNRELIABLE_SENDER_H_
#define UNRELIABLE_SENDER_H_
#include "ReliableMessageConnection.h"

class ReliableSendMessageConnection
{
    constexpr static size_t kMessageSize = ReliableConnection::kMessageSize;
    const static int kSendBatch = ReliableConnection::kSenderBatchSize;

public:
    ReliableSendMessageConnection(std::vector<std::vector<ibv_qp *>> &QPs,
                                  std::array<ibv_cq *, RMSG_MULTIPLEXING> &cqs,
                                  uint32_t lkey);
    ~ReliableSendMessageConnection();
    void send(size_t threadID,
              size_t node_id,
              const char *buf,
              size_t size,
              size_t targetID);

private:
    ssize_t poll_cq(size_t mid);
    ssize_t poll_cq_at_least(size_t mid, ssize_t until);

    std::vector<std::vector<ibv_qp *>> &QPs_;
    std::array<ibv_cq *, RMSG_MULTIPLEXING> send_cqs_;

    uint32_t lkey_{0};
};

#endif