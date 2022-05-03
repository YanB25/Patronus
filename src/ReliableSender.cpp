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

    static thread_local bool thread_second_{false};
    static thread_local size_t msg_send_index_{0};
    static thread_local size_t polleds_[RMSG_MULTIPLEXING]{};
    static thread_local size_t send_signaleds_[RMSG_MULTIPLEXING]{};

    // TODO: should be calculated carefully.
    constexpr static size_t kCqeAllowedSize = 64;

    msg_send_index_++;
    bool signal = false;

    // Not exactly correct, because each threads does NOT sharing how many
    // signal they sent and how many polled they finished.
    // I have an idea: track the *last* targetID. If targetID changed (detected)
    // poll to let the ongoing signals under a threshold before going on. That
    // would be a *perfect* solution.
    // But just don't want to fix until I really need.
    if (unlikely(msg_send_index_ % kSendBatch == 0))
    {
        if (likely(thread_second_))
        {
            DVLOG(3) << "[rmsg] Thread " << threadID << " triggers one poll"
                     << ", current targetID " << targetID;
            auto sent = send_signaleds_[targetID];
            auto polled = polleds_[targetID];
            if (unlikely(sent - polled >= kCqeAllowedSize))
            {
                // two more
                auto expect_poll = sent - polled - kCqeAllowedSize + 2;
                polleds_[targetID] += poll_cq_at_least(targetID, expect_poll);
            }
            else
            {
                polleds_[targetID] += poll_cq(targetID);
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
        send_signaleds_[targetID]++;
    }

    PLOG_IF(FATAL, !succ) << "** Send with RDMA_SEND failed. wrid: "
                          << WRID(wrid) << ". At targetID: " << targetID
                          << ", issued_signaled: " << send_signaleds_[targetID]
                          << " (effectively "
                          << send_signaleds_[targetID] * kSendBatch << ") "
                          << ", polled: " << polleds_[targetID] << ", ongoing: "
                          << (send_signaleds_[targetID] - polleds_[targetID]);
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