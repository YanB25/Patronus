#include "ReliableSender.h"

ReliableSendMessageConnection::ReliableSendMessageConnection(
    std::vector<std::vector<ibv_qp *>> &QPs, ibv_cq *cq, uint32_t lkey)
    : QPs_(QPs), send_cq_(cq), lkey_(lkey)
{
    msg_send_indexes_ = (std::atomic<size_t> *) &msg_send_indexes_inner_;
}

ReliableSendMessageConnection::~ReliableSendMessageConnection()
{
}

void ReliableSendMessageConnection::send(size_t node_id,
                                         const char *buf,
                                         size_t size,
                                         size_t targetID)
{
    DCHECK_LT(targetID, RMSG_MULTIPLEXING);
    CHECK_LE(size, kMessageSize) << "[rmsg] message size exceed limits";

    auto nr =
        msg_send_indexes_[targetID].fetch_add(1, std::memory_order_relaxed) + 1;
    bool signal = false;

    if (nr % kSendBatch == 0)
    {
        if (second_)
        {
            DVLOG(3) << "[rmsg] One poll";
            poll_cq();
        }
        signal = true;
        second_ = true;
    }
    bool inlined = (size <= 32);

    DVLOG(3) << "[rmsg] sending to QP[" << targetID << "][" << node_id
            << "] with size " << size << ", inlined: " << inlined << ", signal: " << signal;

    CHECK(rdmaSend(QPs_[targetID][node_id],
                   (uint64_t) buf,
                   size,
                   lkey_,
                   signal,
                   inlined,
                   WRID(WRID_PREFIX_RELIABLE_SEND, 0).val,
                   size));
}

void ReliableSendMessageConnection::poll_cq()
{
    ibv_wc wc;
    static auto err_h = [](ibv_wc *wc)
    {
        LOG(ERROR) << "Failed to process send. ";
        DCHECK_EQ(WRID(wc->wr_id).prefix, WRID_PREFIX_RELIABLE_SEND);
    };

    auto ret = pollWithCQ(send_cq_, 1, &wc, err_h);
    PLOG_IF(ERROR, ret < 0) << "failed to pollWithCQ";
}