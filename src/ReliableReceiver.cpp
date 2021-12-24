#include "ReliableReceiver.h"

ReliableRecvMessageConnection::ReliableRecvMessageConnection(
    std::vector<std::vector<ibv_qp *>> &QPs,
    std::array<ibv_cq *, RMSG_MULTIPLEXING>& cqs,
    void *msg_pool,
    uint32_t lkey)
    : QPs_(QPs), recv_cqs_(cqs), msg_pool_(msg_pool), lkey_(lkey)
{
    memset(recvs, 0, sizeof(recvs));
    memset(recv_sgl, 0, sizeof(recv_sgl));
}
ReliableRecvMessageConnection::~ReliableRecvMessageConnection()
{
}

void ReliableRecvMessageConnection::fills(ibv_sge &sge,
                                          ibv_recv_wr &wr,
                                          size_t multiplexing_id,
                                          size_t node_id,
                                          size_t batch_id)
{
    memset(&sge, 0, sizeof(ibv_sge));

    sge.addr =
        (uint64_t) msg_pool_ +
        get_msg_pool_idx(multiplexing_id, node_id, batch_id) * kMessageSize;
    DCHECK_EQ(sge.addr % 64, 0) << "Should be cacheline aligned. otherwise the "
                                   "performance is not optimized.";

    sge.length = kMessageSize;
    sge.lkey = lkey_;

    memset(&wr, 0, sizeof(ibv_recv_wr));

    wr.sg_list = &sge;
    wr.num_sge = 1;
    if ((batch_id + 1) % kPostRecvBufferBatch == 0)
    {
        DVLOG(3) << "[rmsg-recv] recvs[" << multiplexing_id << "][" << node_id
                 << "][" << batch_id << "] set next to "
                 << "nullptr. Current k " << batch_id << " @"
                 << (void *) sge.addr;
        wr.next = nullptr;
    }
    else
    {
        DVLOG(3) << "[rmesg-recv] recvs[" << multiplexing_id << "][" << node_id
                 << "][" << batch_id << "] set next to "
                 << "recvs[" << multiplexing_id << "][" << node_id << "]["
                 << batch_id + 1 << "]. Current k " << batch_id << " @"
                 << (void *) sge.addr;
        wr.next = &recvs[multiplexing_id][node_id][batch_id + 1];
        DCHECK_LT(node_id, MAX_MACHINE);
        DCHECK_LT(batch_id + 1, kRecvBuffer)
            << "Current m " << node_id << ", k " << batch_id;
        DCHECK_LT(multiplexing_id, RMSG_MULTIPLEXING);
    }

    DCHECK_LT(WRID_PREFIX_RELIABLE_RECV,
              std::numeric_limits<decltype(WRID::prefix)>::max());
    DCHECK_LT(MAX_MACHINE, std::numeric_limits<decltype(WRID::u16_a)>::max());
    DCHECK_LT(RMSG_MULTIPLEXING,
              std::numeric_limits<decltype(WRID::u16_b)>::max());
    wr.wr_id =
        WRID(WRID_PREFIX_RELIABLE_RECV, node_id, multiplexing_id, batch_id).val;
}

/**
 * @brief should be inited after the qp is valid.
 *
 */
void ReliableRecvMessageConnection::init()
{
    for (size_t t = 0; t < RMSG_MULTIPLEXING; ++t)
    {
        for (size_t m = 0; m < MAX_MACHINE; ++m)
        {
            for (size_t k = 0; k < kRecvBuffer; ++k)
            {
                auto &sge = recv_sgl[t][m][k];
                auto &wr = recvs[t][m][k];
                fills(sge, wr, t, m, k);
            }
        }
    }

    struct ibv_recv_wr *bad;
    for (size_t multiplexing_id = 0; multiplexing_id < RMSG_MULTIPLEXING;
         ++multiplexing_id)
    {
        for (size_t i = 0; i < kPostRecvBufferAdvanceBatch; ++i)
        {
            for (size_t remoteID = 0; remoteID < QPs_[0].size(); ++remoteID)
            {
                if (ibv_post_recv(QPs_[multiplexing_id][remoteID],
                                  &recvs[multiplexing_id][remoteID]
                                        [i * kPostRecvBufferBatch],
                                  &bad))
                {
                    PLOG(ERROR) << "Receive failed.";
                }
                DVLOG(3) << "[rmsg] posting recvs[" << multiplexing_id << "]["
                         << remoteID << "][" << i * kPostRecvBufferBatch << "]"
                         << "to remoteID " << remoteID;
            }
        }
    }

    inited_ = true;
}

size_t ReliableRecvMessageConnection::try_recv(size_t mid, char *ibuf, size_t msg_limit)
{
    DCHECK(inited_);
    constexpr static size_t kMaxPollNr = 64;
    msg_limit = std::min(msg_limit, kMaxPollNr);

    static thread_local ibv_wc wc[kMaxPollNr];

    size_t actually_polled = ibv_poll_cq(recv_cqs_[mid], msg_limit, wc);
    if (actually_polled < 0)
    {
        PLOG(ERROR) << "failed to ibv_poll_cq";
        return 0;
    }
    // TODO: for now, we always + kMessageSize. Could we just + message actual
    // size?
    for (size_t i = 0; i < actually_polled; ++i)
    {
        handle_wc(ibuf + i * kMessageSize, wc[i]);
    }
    return actually_polled;
}

void ReliableRecvMessageConnection::handle_wc(char *ibuf, const ibv_wc &wc)
{
    if (wc.status != IBV_WC_SUCCESS)
    {
        LOG(ERROR) << "[rmsg] Failed to handle wc: " << WRID(wc.wr_id)
                   << ", status: " << wc.status;
        return;
    }
    uint32_t node_id = WRID(wc.wr_id).u16_a;
    uint32_t mid = WRID(wc.wr_id).u16_b;

    DCHECK_LT(node_id, MAX_MACHINE);
    DCHECK_LT(mid, RMSG_MULTIPLEXING);

    void *msg_recv_index_layout = &msg_recv_index_layout_[mid][node_id];
    std::atomic<size_t> &msg_recv_index =
        *(std::atomic<size_t> *) msg_recv_index_layout;
    size_t cur_idx = msg_recv_index.fetch_add(1, std::memory_order_relaxed);
    auto actual_size = wc.imm_data;

    if (ibuf)
    {
        char *buf = (char *) msg_pool_ +
                    get_msg_pool_idx(mid, node_id, cur_idx % kRecvBuffer) *
                        kMessageSize;
        memcpy(ibuf, buf, actual_size);

        auto get = *(uint64_t *) buf;
        DVLOG(3) << "[Rmsg] Recved msg from node " << node_id << ", mid " << mid
                 << ", cur_idx " << cur_idx << ", it is " << std::hex << get
                 << " @" << (void *) buf;
    }
    else
    {
        DVLOG(3) << "[Rmsg] Recved msg from node " << node_id << ", cur_idx "
                 << cur_idx;
    }

    if ((cur_idx + 1) % kPostRecvBufferBatch == 0)
    {
        size_t post_buf_idx =
            (cur_idx + 1 % kRecvBuffer) +
            (kPostRecvBufferAdvanceBatch - 1) * kPostRecvBufferBatch;
        ibv_recv_wr *bad;
        DVLOG(3) << "[rmsg] Posting another " << kPostRecvBufferBatch
                 << " recvs to node " << node_id << ", mid " << mid
                 << ". cur_idx " << cur_idx << " i.e. recvs[" << mid << "]["
                 << node_id << "][" << (post_buf_idx % kRecvBuffer) << "]";

        PLOG_IF(ERROR,
                ibv_post_recv(QPs_[mid][node_id],
                              &recvs[mid][node_id][post_buf_idx % kRecvBuffer],
                              &bad))
            << "failed to post recv";
    }
}

void ReliableRecvMessageConnection::recv(size_t mid, char *ibuf, size_t msg_limit)
{
    size_t remain = msg_limit;
    while (remain > 0)
    {
        auto actual = try_recv(mid, ibuf, remain);
        DCHECK_LE(actual, remain);
        remain -= actual;
    }
}