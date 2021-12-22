#include "ReliableMessageConnection.h"

#include <glog/logging.h>

#include "ReliableReceiver.h"
#include "ReliableSender.h"

ReliableConnection::ReliableConnection(uint64_t mm,
                                       size_t mm_size,
                                       size_t machine_nr)
{
    if constexpr (!config::kEnableReliableMessage)
    {
        return;
    }
    CHECK(createContext(&ctx_));
    constexpr static size_t kMsgNr =
        MAX_MACHINE * kRecvBuffer * RMSG_MULTIPLEXING;
    recv_msg_pool_ = CHECK_NOTNULL(hugePageAlloc(kMsgNr * kMessageSize));

    recv_mr_ = CHECK_NOTNULL(createMemoryRegion(
        (uint64_t) recv_msg_pool_, kMsgNr * kMessageSize, &ctx_));
    recv_lkey_ = recv_mr_->lkey;

    send_mr_ = CHECK_NOTNULL(createMemoryRegion((uint64_t) mm, mm_size, &ctx_));
    send_lkey_ = send_mr_->lkey;

    recv_cq_ = CHECK_NOTNULL(
        ibv_create_cq(ctx_.ctx, RAW_RECV_CQ_COUNT, nullptr, nullptr, 0));
    // at maximum, the sender will have (RMSG_MULTIPLEXING) * kSenderBatchSize
    // pending cqes
    size_t max_cqe_for_sender = RMSG_MULTIPLEXING * kSenderBatchSize * 2;
    send_cq_ = CHECK_NOTNULL(
        ibv_create_cq(ctx_.ctx, max_cqe_for_sender, nullptr, nullptr, 0));

    for (size_t i = 0; i < RMSG_MULTIPLEXING; ++i)
    {
        QPs_.emplace_back(machine_nr);
        for (size_t m = 0; m < machine_nr; ++m)
        {
            CHECK(createQueuePair(&QPs_.back()[m],
                                  IBV_QPT_RC,
                                  send_cq_,
                                  recv_cq_,
                                  &ctx_,
                                  128,
                                  32));
        }
    }

    send_ = std::make_unique<ReliableSendMessageConnection>(
        QPs_, send_cq_, send_lkey_);
    recv_ = std::make_unique<ReliableRecvMessageConnection>(
        QPs_, recv_cq_, recv_msg_pool_, recv_lkey_);
}
ReliableConnection::~ReliableConnection()
{
    if constexpr (!config::kEnableReliableMessage)
    {
        return;
    }

    for (size_t i = 0; i < QPs_.size(); ++i)
    {
        for (size_t m = 0; m < QPs_[0].size(); ++m)
        {
            CHECK(destroyQueuePair(QPs_[i][m]));
        }
    }
    CHECK(destroyCompleteQueue(send_cq_));
    CHECK(destroyCompleteQueue(recv_cq_));
    CHECK(destroyMemoryRegion(recv_mr_));
    CHECK(destroyMemoryRegion(send_mr_));
    CHECK(hugePageFree(
        recv_msg_pool_,
        RMSG_MULTIPLEXING * MAX_MACHINE * kRecvBuffer * kMessageSize));
    CHECK(destroyContext(&ctx_));
}

void ReliableConnection::send(
    size_t threadID, const char *buf, size_t size, uint16_t node_id, size_t mid)
{
    DCHECK(config::kEnableReliableMessage);
    send_->send(threadID, node_id, buf, size, mid);
}
void ReliableConnection::recv(char *ibuf, size_t limit)
{
    DCHECK(config::kEnableReliableMessage);
    return recv_->recv(ibuf, limit);
}
size_t ReliableConnection::try_recv(char *ibuf, size_t limit)
{
    DCHECK(config::kEnableReliableMessage);
    return recv_->try_recv(ibuf, limit);
}