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
    // NOTE: has to be THREAD_UNSAFE instead of THREAD_SINGLE
    // - THEAD_UNSAFE: Access to the associated objects are not thread safe.
    // - THREAD_SINGLE: different objects associated with the same resource
    // domain must be called by the same thread.
    // TODO: don't know why:
    // If resource domain is attached, it assert false, because of multi-thread problem
    // if I set thread model to THREAD_SINGLE or THREAD_UNSAFE, it will not throw.
    // However, the performance drops significantly.
    // Soo strange.
    CHECK(createContext(
        &ctx_, 1, 1, 0, std::nullopt, std::nullopt));
    constexpr static size_t kMsgNr =
        MAX_MACHINE * kRecvBuffer * RMSG_MULTIPLEXING;
    recv_msg_pool_ = CHECK_NOTNULL(hugePageAlloc(kMsgNr * kMessageSize));
    CHECK_EQ((uint64_t) recv_msg_pool_ % 64, 0) << "must be cacheline aligned.";

    recv_mr_ = CHECK_NOTNULL(createMemoryRegion(
        (uint64_t) recv_msg_pool_, kMsgNr * kMessageSize, &ctx_));
    recv_lkey_ = recv_mr_->lkey;

    send_mr_ = CHECK_NOTNULL(createMemoryRegion((uint64_t) mm, mm_size, &ctx_));
    send_lkey_ = send_mr_->lkey;

    size_t max_cqe_for_receiver = machine_nr * RMSG_MULTIPLEXING *
                                  kPostRecvBufferBatch *
                                  kPostRecvBufferAdvanceBatch;
    for (size_t i = 0; i < RMSG_MULTIPLEXING; ++i)
    {
        recv_cqs_[i] =
            CHECK_NOTNULL(createCompleteQueue(&ctx_, max_cqe_for_receiver));
    }
    // at maximum, the sender will have (RMSG_MULTIPLEXING) * kSenderBatchSize
    // pending cqes
    size_t max_cqe_for_sender = machine_nr * MAX_APP_THREAD;
    for (size_t i = 0; i < RMSG_MULTIPLEXING; ++i)
    {
        send_cqs_[i] =
            CHECK_NOTNULL(createCompleteQueue(&ctx_, max_cqe_for_sender));
    }

    // sender size requirement
    size_t qp_max_depth_send = kSenderBatchSize * MAX_APP_THREAD;
    // receiver size requirement
    size_t qp_max_depth_recv =
        kPostRecvBufferAdvanceBatch * kPostRecvBufferBatch;
    for (size_t i = 0; i < RMSG_MULTIPLEXING; ++i)
    {
        QPs_.emplace_back(machine_nr);
        for (size_t m = 0; m < machine_nr; ++m)
        {
            CHECK(createQueuePair(&QPs_.back()[m],
                                  IBV_QPT_RC,
                                  send_cqs_[i],
                                  recv_cqs_[i],
                                  &ctx_,
                                  qp_max_depth_send,
                                  qp_max_depth_recv,
                                  32));
        }
    }

    send_ = std::make_unique<ReliableSendMessageConnection>(
        QPs_, send_cqs_, send_lkey_);
    recv_ = std::make_unique<ReliableRecvMessageConnection>(
        QPs_, recv_cqs_, recv_msg_pool_, recv_lkey_);
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
    for (size_t i = 0; i < send_cqs_.size(); ++i)
    {
        CHECK(destroyCompleteQueue(send_cqs_[i]));
    }
    for (size_t i = 0; i < recv_cqs_.size(); ++i)
    {
        CHECK(destroyCompleteQueue(recv_cqs_[i]));
    }
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
void ReliableConnection::recv(size_t mid, char *ibuf, size_t limit)
{
    DCHECK(config::kEnableReliableMessage);
    return recv_->recv(mid, ibuf, limit);
}
size_t ReliableConnection::try_recv(size_t mid, char *ibuf, size_t limit)
{
    DCHECK(config::kEnableReliableMessage);
    return recv_->try_recv(mid, ibuf, limit);
}