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
    DCHECK_LT(threadID, MAX_APP_THREAD);
    DCHECK_LE(size, kMessageSize) << "[rmsg] message size exceed limits";

    static thread_local bool thread_second_{false};
    static thread_local size_t msg_send_index_{0};

    msg_send_index_++;
    bool signal = false;

    if (msg_send_index_ % kSendBatch == 0)
    {
        if (thread_second_)
        {
            DVLOG(3) << "[rmsg] Thread " << threadID << " triggers one poll"
                     << ", current targetID " << targetID;
            poll_cq(targetID);
        }
        signal = true;
        thread_second_ = true;
    }
    bool inlined = (size <= 32);

    DVLOG(3) << "[rmsg] Thread " << threadID << " sending to QP[" << targetID
             << "][" << node_id << "] with size " << size
             << ", inlined: " << inlined << ", signal: " << signal;

    // TODO: the fillSeg by rdmaSend is overkilled. try to optimize.
    CHECK(
        rdmaSend(QPs_[targetID][node_id],
                 (uint64_t) buf,
                 size,
                 lkey_,
                 signal,
                 inlined,
                 WRID(WRID_PREFIX_RELIABLE_SEND, threadID, targetID, size).val,
                 size));
}

void ReliableSendMessageConnection::poll_cq(size_t mid)
{
    thread_local static ibv_wc wc[64];

    size_t polled = ibv_poll_cq(send_cqs_[mid], 64, wc);
    if (polled < 0)
    {
        PLOG(ERROR) << "Failed to poll cq.";
    }
    for (size_t i = 0; i < polled; ++i)
    {
        if (wc[i].status != IBV_WC_SUCCESS)
        {
            LOG(ERROR) << "[rmsg] sender wc failed. " << WRID(wc[i].wr_id);
        }
    }
}