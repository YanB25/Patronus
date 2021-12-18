#include "ReliableMessageConnection.h"

#include <glog/logging.h>

ReliableRecvMessageConnection::ReliableRecvMessageConnection(size_t machine_nr)
{
    CHECK(createContext(&ctx_));

    constexpr static size_t kMsgNr = MAX_MACHINE * kRecvBuffer;
    msg_pool_ = CHECK_NOTNULL(hugePageAlloc(kMsgNr * kMessageSize));

    mr_ = CHECK_NOTNULL(
        createMemoryRegion((uint64_t) msg_pool_, kMsgNr * kMessageSize, &ctx_));
    lkey_ = mr_->lkey;

    memset(recvs, 0, sizeof(recvs));
    memset(recv_sgl, 0, sizeof(recv_sgl));

    recv_cq_ = CHECK_NOTNULL(
        ibv_create_cq(ctx_.ctx, RAW_RECV_CQ_COUNT, nullptr, nullptr, 0));

    QPs_.resize(machine_nr);
    for (size_t i = 0; i < machine_nr; ++i)
    {
        CHECK(createQueuePair(&QPs_[i], IBV_QPT_RC, recv_cq_, &ctx_));
    }
}
ReliableRecvMessageConnection::~ReliableRecvMessageConnection()
{
    for (size_t i = 0; i < QPs_.size(); ++i)
    {
        CHECK(destroyQueuePair(QPs_[i]));
    }
    CHECK(destroyCompleteQueue(recv_cq_));
    CHECK(destroyMemoryRegion(mr_));
    hugePageFree(msg_pool_, MAX_MACHINE * kRecvBuffer * kMessageSize);
    destroyContext(&ctx_);
}

/**
 * @brief should be inited after the qp is valid.
 *
 */
void ReliableRecvMessageConnection::init()
{
    inited_ = true;
    for (size_t m = 0; m < MAX_MACHINE; ++m)
    {
        for (int k = 0; k < kRecvBuffer; ++k)
        {
            auto &s = recv_sgl[m][k];
            memset(&s, 0, sizeof(s));

            s.addr =
                (uint64_t) msg_pool_ + get_msg_pool_idx(m, k) * kMessageSize;
            s.length = kMessageSize;
            s.lkey = lkey_;

            auto &r = recvs[m][k];
            memset(&r, 0, sizeof(r));

            r.sg_list = &s;
            r.num_sge = 1;
            if ((k + 1) % kPostRecvBufferBatch == 0)
            {
                // LOG(INFO) << "recvs[" << m << "][" << k << "] set next to "
                //           << "nullptr. Current k " << k;
                r.next = nullptr;
            }
            else
            {
                // LOG(INFO) << "recvs[" << m << "][" << k << "] set next to "
                //           << "recvs[" << m << "][" << k + 1 << "]. Current k
                //           "
                //           << k;
                r.next = &recvs[m][k + 1];
                CHECK_LT(m, MAX_MACHINE);
                CHECK_LT(k + 1, kRecvBuffer)
                    << "Current m " << m << ", k " << k;
            }

            r.wr_id = WRID(WRID_PREFIX_RELIABLE_RECV, m * 100).val;
        }
    }

    struct ibv_recv_wr *bad;
    for (size_t i = 0; i < kPostRecvBufferAdvanceBatch; ++i)
    {
        for (size_t remoteID = 0; remoteID < QPs_.size(); ++remoteID)
        {
            if (ibv_post_recv(QPs_[remoteID],
                              &recvs[remoteID][i * kPostRecvBufferBatch],
                              &bad))
            {
                PLOG(ERROR) << "Receive failed.";
            }
        }
    }
}

size_t ReliableRecvMessageConnection::try_recv(char *ibuf)
{
    DCHECK(inited_);
    ibv_wc wc;
    int ret = ibv_poll_cq(recv_cq_, 1, &wc);
    if (ret < 0)
    {
        PLOG(ERROR) << "failed to pollWithCQ";
        return 0;
    }
    if (ret == 0)
    {
        return 0;
    }
    if (wc.status != IBV_WC_SUCCESS)
    {
        PLOG(ERROR) << "Failed to process recv. wc " << WRID(wc.wr_id);
        return 0;
    }
    else
    {
        [[maybe_unused]] uint32_t type = WRID(wc.wr_id).prefix;
        DCHECK_EQ(type, WRID_PREFIX_RELIABLE_RECV)
            << "mess up QP. may cause unexpected blocking.";
    }

    uint32_t node_id = WRID(wc.wr_id).id;
    size_t cur_idx =
        msg_recv_index_[node_id].fetch_add(1, std::memory_order_relaxed);

    VLOG(3) << "[Rmsg] Recved msg from node " << node_id << ", cur_idx "
            << cur_idx;

    if ((cur_idx + 1) % kPostRecvBufferBatch == 0)
    {
        size_t post_buf_idx =
            (cur_idx + 1 % kRecvBuffer) +
            kPostRecvBufferAdvanceBatch * kPostRecvBufferBatch;
        struct ibv_recv_wr *bad;
        VLOG(3) << "[rmsg] Posting another " << kPostRecvBufferBatch
                  << " recvs to node " << node_id << " for cur_idx " << cur_idx
                  << " i.e. recvs[" << node_id << "]["
                  << (post_buf_idx % kRecvBuffer) << "]";
        PLOG_IF(ERROR,
                ibv_post_recv(QPs_[node_id],
                              &recvs[node_id][post_buf_idx % kRecvBuffer],
                              &bad))
            << "failed to post recv";
    }

    if (ibuf)
    {
        char *buf = (char *) msg_pool_ +
                    get_msg_pool_idx(node_id, cur_idx) * kMessageSize;
        memcpy(ibuf, buf, kMessageSize);
    }
    return kMessageSize;
}

size_t ReliableRecvMessageConnection::recv(char *ibuf)
{
    DCHECK(inited_);
    size_t ret;
    do
    {
        ret = try_recv(ibuf);
    } while (ret == 0);
    return ret;
}

ReliableSendMessageConnection::ReliableSendMessageConnection(uint64_t mm,
                                                             size_t mm_size,
                                                             size_t machine_nr)
{
    CHECK(createContext(&ctx_));

    mr_ = CHECK_NOTNULL(createMemoryRegion((uint64_t) mm, mm_size, &ctx_));
    lkey_ = mr_->lkey;

    send_cq_ = CHECK_NOTNULL(
        ibv_create_cq(ctx_.ctx, RAW_RECV_CQ_COUNT, nullptr, nullptr, 0));

    QPs_.resize(machine_nr);
    for (size_t i = 0; i < machine_nr; ++i)
    {
        CHECK(createQueuePair(&QPs_[i], IBV_QPT_RC, send_cq_, &ctx_));
    }
}

ReliableSendMessageConnection::~ReliableSendMessageConnection()
{
    for (size_t i = 0; i < QPs_.size(); ++i)
    {
        CHECK(destroyQueuePair(QPs_[i]));
    }
    CHECK(destroyCompleteQueue(send_cq_));
    CHECK(destroyMemoryRegion(mr_));
    destroyContext(&ctx_);
}

void ReliableSendMessageConnection::send(size_t node_id,
                                         const char *buf,
                                         size_t size)
{
    CHECK_LT(size, kMessageSize) << "[rmsg] message size exceed limits";

    auto nr = msg_sent_nr_.fetch_add(1, std::memory_order_relaxed) + 1;
    bool signal = false;

    if (nr % kSendBatch == 0)
    {
        if (second_)
        {
            VLOG(3) << "[rmsg] One poll";
            poll_cq();
        }
        signal = true;
        second_ = true;
    }

    if (signal)
    {
        VLOG(3) << "[rmsg] One signal";
    }
    CHECK(rdmaSend(QPs_[node_id],
                   (uint64_t) buf,
                   size,
                   lkey_,
                   signal,
                   WRID(WRID_PREFIX_RELIABLE_SEND, 0).val,
                   0));
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