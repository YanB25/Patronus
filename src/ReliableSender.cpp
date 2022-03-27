#include "ReliableSender.h"

ReliableSendMessageConnection::ReliableSendMessageConnection(
    std::vector<std::vector<ibv_qp *>> &QPs,
    std::array<ibv_cq *, RMSG_MULTIPLEXING> &cqs,
    uint32_t lkey)
    : QPs_(QPs), send_cqs_(cqs), lkey_(lkey)
{
}

ReliableSendMessageConnection::~ReliableSendMessageConnection()
{
}

void ReliableSendMessageConnection::send(size_t threadID,
                                         size_t node_id,
                                         const char *buf,
                                         size_t size,
                                         size_t targetID)
{
    DCHECK_LT(targetID, RMSG_MULTIPLEXING);
    DCHECK_LT(threadID, kMaxAppThread);
    DCHECK_LE(size, kMessageSize) << "[rmsg] message size exceed limits";

    DCHECK_EQ(threadID, targetID)
        << "TODO: This assertion is added very later, until I realize that we "
           "don't actual need the star-like reliable channel. What we need is "
           "actual a one-to-one channel for client thread and server directory "
           "(or many-to-one to be more specifically). Currently, I assume that "
           "threadID always equal to targetID";

    static thread_local bool thread_second_{false};
    static thread_local size_t msg_send_index_{0};
    static thread_local size_t polled_{0};
    static thread_local size_t send_signaled_{0};

    // TODO: should be calculated carefully.
    constexpr static size_t kCqeAllowedSize = 32;

    msg_send_index_++;
    bool signal = false;

    if (unlikely(msg_send_index_ % kSendBatch == 0))
    {
        if (likely(thread_second_))
        {
            DVLOG(3) << "[rmsg] Thread " << threadID << " triggers one poll"
                     << ", current targetID " << targetID;
            if (unlikely(send_signaled_ - polled_ >= kCqeAllowedSize))
            {
                // two more
                auto expect_poll =
                    send_signaled_ - polled_ - kCqeAllowedSize + 2;
                polled_ += poll_cq_at_least(targetID, expect_poll);
            }
            else
            {
                polled_ += poll_cq(targetID);
            }
        }
        signal = true;
        thread_second_ = true;
    }
    bool inlined = (size <= 32);

    DVLOG(3) << "[rmsg] Thread " << threadID << " sending to QP[" << targetID
             << "][" << node_id << "] with size " << size
             << ", inlined: " << inlined << ", signal: " << signal;

    // TODO: the fillSeg by rdmaSend is overkilled. try to optimize.
    auto wrid = WRID(WRID_PREFIX_RELIABLE_SEND, threadID, targetID, size).val;
    bool succ = rdmaSend(QPs_[targetID][node_id],
                         (uint64_t) buf,
                         size,
                         lkey_,
                         signal,
                         inlined,
                         wrid,
                         size);
    if (signal)
    {
        send_signaled_++;
    }

    PLOG_IF(FATAL, !succ) << "** Send with RDMA_SEND failed. wrid: " << wrid
                          << ", issued_signaled: " << send_signaled_
                          << " (effectively " << send_signaled_ * kSendBatch
                          << ") "
                          << ", polled: " << polled_
                          << ", ongoing: " << (send_signaled_ - polled_);
}

ssize_t ReliableSendMessageConnection::poll_cq(size_t mid)
{
    thread_local static ibv_wc wc[2 * kSendBatch];

    size_t polled = ibv_poll_cq(send_cqs_[mid], 2 * kSendBatch, wc);
    if (unlikely(polled < 0))
    {
        PLOG(ERROR) << "Failed to poll cq.";
    }
    for (size_t i = 0; i < polled; ++i)
    {
        if (unlikely(wc[i].status != IBV_WC_SUCCESS))
        {
            LOG(ERROR) << "[rmsg] sender wc failed. " << WRID(wc[i].wr_id);
        }
    }
    return polled;
}

ssize_t ReliableSendMessageConnection::poll_cq_at_least(size_t mid,
                                                        ssize_t until)
{
    DCHECK_GT(until, 0);
    ssize_t polled_nr = 0;
    while (polled_nr < until)
    {
        polled_nr += poll_cq(mid);
    }
    return polled_nr;
}