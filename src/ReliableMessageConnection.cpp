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

void ReliableRecvMessageConnection::fills(ibv_sge &sge,
                                          ibv_recv_wr &wr,
                                          size_t node_id,
                                          size_t batch_id)
{
    memset(&sge, 0, sizeof(ibv_sge));

    sge.addr = (uint64_t) msg_pool_ + get_msg_pool_idx(node_id, batch_id) * kMessageSize;
    sge.length = kMessageSize;
    sge.lkey = lkey_;

    memset(&wr, 0, sizeof(ibv_recv_wr));

    wr.sg_list = &sge;
    wr.num_sge = 1;
    if ((batch_id + 1) % kPostRecvBufferBatch == 0)
    {
        VLOG(3) << "[rmsg-recv] recvs[" << node_id << "][" << batch_id << "] set next to "
                << "nullptr. Current k " << batch_id << " @" << (void *) sge.addr;
        wr.next = nullptr;
    }
    else
    {
        VLOG(3) << "[rmesg-recv] recvs[" << node_id << "][" << batch_id << "] set next to "
                << "recvs[" << node_id << "][" << batch_id + 1 << "]. Current k " << batch_id
                << " @" << (void *) sge.addr;
        wr.next = &recvs[node_id][batch_id + 1];
        CHECK_LT(node_id, MAX_MACHINE);
        CHECK_LT(batch_id + 1, kRecvBuffer) << "Current m " << node_id << ", k " << batch_id;
    }

    wr.wr_id = WRID(WRID_PREFIX_RELIABLE_RECV, node_id).val;
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
            auto& sge = recv_sgl[m][k];
            auto& wr = recvs[m][k];
            fills(sge, wr, m, k);
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
            VLOG(3) << "[rmsg] posting recvs[" << remoteID << "]["
                         << i * kPostRecvBufferBatch << "]" << "to remoteID " << remoteID;
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

    if ((cur_idx + 1) % kPostRecvBufferBatch == 0)
    {
        size_t post_buf_idx =
            (cur_idx + 1 % kRecvBuffer) +
            (kPostRecvBufferAdvanceBatch - 1) * kPostRecvBufferBatch;
        struct ibv_recv_wr *bad;
        for (size_t i = 0; i < kPostRecvBufferBatch; ++i)
        {
            size_t second_id = (post_buf_idx + i) % kRecvBuffer;
            auto& sgl = recv_sgl[node_id][second_id];
            auto& wr = recvs[node_id][second_id];
            fills(sgl, wr, node_id, second_id);
        }
        VLOG(3) << "[rmsg] Posting another " << kPostRecvBufferBatch
                     << " recvs to node " << node_id << " for cur_idx "
                     << cur_idx << " i.e. recvs[" << node_id << "]["
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
                    get_msg_pool_idx(node_id, cur_idx % kRecvBuffer) * kMessageSize;
        memcpy(ibuf, buf, kMessageSize);

        auto get = *(uint64_t *) buf;
        VLOG(3) << "[Rmsg] Recved msg from node " << node_id << ", cur_idx "
                << cur_idx << ", it is " << std::hex << get << " @"
                << (void *) buf;
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