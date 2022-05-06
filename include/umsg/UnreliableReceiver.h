#pragma once
#ifndef RELIABLE_RECEIVER_H_
#define RELIABLE_RECEIVER_H_

#include <infiniband/verbs.h>

#include "Common.h"
#include "PerThread.h"
#include "Rdma.h"
#include "city.h"
#include "umsg/Config.h"
#include "umsg/UnreliableConnection.h"

template <size_t kEndpointNr>
class UnreliableRecvMessageConnection
{
    constexpr static int kUserMessageSize = config::umsg::kUserMessageSize;
    constexpr static int kPostMessageSize = config::umsg::kPostMessageSize;
    constexpr static size_t kRecvBuffer = config::umsg::kRecvBuffer;

    constexpr static size_t kPostRecvBufferAdvanceBatch =
        config::umsg::kPostRecvBufferAdvanceBatch;
    constexpr static size_t kPostRecvBufferBatch =
        config::umsg::kPostRecvBufferBatch;
    constexpr static size_t kRecvLimit = config::umsg::kRecvLimit;

    // see recvs[...][...]
    constexpr static size_t kMessageNr = kEndpointNr * kRecvBuffer;
    constexpr static size_t kMessageBufferSize = kMessageNr * kPostMessageSize;

public:
    UnreliableRecvMessageConnection(std::array<ibv_qp *, kEndpointNr> &QPs,
                                    std::array<ibv_cq *, kEndpointNr> recv_cqs,
                                    RdmaContext &ctx)
        : QPs_(QPs), recv_cqs_(recv_cqs), ctx_(ctx)
    {
        memset(recvs, 0, sizeof(recvs));
        memset(recv_sgl, 0, sizeof(recv_sgl));
        msg_pool_ = CHECK_NOTNULL(hugePageAlloc(kMessageBufferSize));
        CHECK_EQ((uint64_t) msg_pool_ % 64, 0) << "should be aligned";
        recv_mr_ = CHECK_NOTNULL(createMemoryRegion(
            (uint64_t) msg_pool_, kMessageNr * kPostMessageSize, &ctx));
        lkey_ = recv_mr_->lkey;
    }

    ~UnreliableRecvMessageConnection()
    {
        CHECK(destroyMemoryRegion(recv_mr_));
        CHECK(hugePageFree(msg_pool_, kMessageBufferSize));
    }
    /**
     * @brief try to recv any buffered messages
     *
     * @param ibuf The buffered to store received messages
     * @param msg_limit The number of messages at most the ibuf can store
     * @return size_t The number of messages actually received.
     */
    size_t try_recv(size_t i_ep_id, char *ibuf, size_t msg_limit = 1);
    void recv(size_t i_ep_id, char *ibuf, size_t msg_limit = 1);
    void init();

private:
    void handle_wc(char *ibuf, const ibv_wc &wc);
    void fills(ibv_sge &sge, ibv_recv_wr &wr, size_t ep_id, size_t batch_id);

    bool inited_{false};
    void poll_cq();
    void *msg_pool_{nullptr};
    ibv_mr *recv_mr_{nullptr};
    uint32_t lkey_{0};

    /**
     * @brief maintain QPs[kEndpointNr]
     */
    std::array<ibv_qp *, kEndpointNr> &QPs_{};
    /**
     * @brief maintain receive CQs[kEndpointNr]
     */
    std::array<ibv_cq *, kEndpointNr> recv_cqs_{};
    RdmaContext &ctx_;

    size_t get_msg_pool_idx(size_t epID, size_t batch_id)
    {
        return epID * kRecvBuffer + batch_id;
    }

    ibv_recv_wr recvs[kEndpointNr][kRecvBuffer];
    ibv_sge recv_sgl[kEndpointNr][kRecvBuffer];

    size_t msg_recv_index_[kEndpointNr]{};
};
template <size_t kEndpointNr>
void UnreliableRecvMessageConnection<kEndpointNr>::fills(ibv_sge &sge,
                                                         ibv_recv_wr &wr,
                                                         size_t th_id,
                                                         size_t batch_id)
{
    memset(&sge, 0, sizeof(ibv_sge));

    sge.addr = (uint64_t) msg_pool_ +
               get_msg_pool_idx(th_id, batch_id) * kPostMessageSize;

    sge.length = kPostMessageSize;
    sge.lkey = lkey_;

    memset(&wr, 0, sizeof(ibv_recv_wr));

    wr.sg_list = &sge;
    wr.num_sge = 1;
    if ((batch_id + 1) % kPostRecvBufferBatch == 0)
    {
        DVLOG(10) << "[umsg-recv] recvs[" << th_id << "][" << batch_id
                  << "] set next to "
                  << "nullptr. Current k " << batch_id << " @"
                  << (void *) sge.addr;
        wr.next = nullptr;
    }
    else
    {
        DVLOG(10) << "[umesg-recv] recvs[" << th_id << "][" << batch_id
                  << "] set next to "
                  << "recvs[" << th_id << "][" << batch_id + 1
                  << "]. Current k " << batch_id << " @" << (void *) sge.addr;
        wr.next = &recvs[th_id][batch_id + 1];
        DCHECK_LT(batch_id + 1, kRecvBuffer) << "Current  k " << batch_id;
        DCHECK_LT(th_id, kEndpointNr);
    }

    DCHECK_LT(WRID_PREFIX_RELIABLE_RECV,
              std::numeric_limits<decltype(WRID::prefix)>::max());
    DCHECK_LT(MAX_MACHINE, std::numeric_limits<decltype(WRID::u16_a)>::max());
    DCHECK_LT(kEndpointNr, std::numeric_limits<decltype(WRID::u16_b)>::max());
    wr.wr_id =
        WRID(WRID_PREFIX_RELIABLE_RECV, th_id, 0 /* not used */, batch_id).val;
}
template <size_t kEndpointNr>
void UnreliableRecvMessageConnection<kEndpointNr>::init()
{
    for (size_t t = 0; t < kEndpointNr; ++t)
    {
        for (size_t k = 0; k < kRecvBuffer; ++k)
        {
            auto &sge = recv_sgl[t][k];
            auto &wr = recvs[t][k];
            fills(sge, wr, t, k);
        }
    }

    struct ibv_recv_wr *bad;
    for (size_t ep_id = 0; ep_id < kEndpointNr; ++ep_id)
    {
        for (size_t i = 0; i < kPostRecvBufferAdvanceBatch; ++i)
        {
            ibv_recv_wr *post_wr = &recvs[ep_id][i * kPostRecvBufferBatch];
            if (ibv_post_recv(QPs_[ep_id], post_wr, &bad))
            {
                PLOG(ERROR) << "Receive failed.";
            }
            DVLOG(3) << "[umsg] posting recvs[" << ep_id << "]["
                     << i * kPostRecvBufferBatch << "] with WRID "
                     << WRID(post_wr->wr_id);
        }
    }

    inited_ = true;
}
template <size_t kEndpointNr>
size_t UnreliableRecvMessageConnection<kEndpointNr>::try_recv(size_t ep_id,
                                                              char *ibuf,
                                                              size_t msg_limit)
{
    DCHECK(inited_);
    msg_limit = std::min(msg_limit, kRecvLimit);

    static thread_local ibv_wc wc[kRecvLimit];

    size_t actually_polled = ibv_poll_cq(recv_cqs_[ep_id], msg_limit, wc);
    if (unlikely(actually_polled < 0))
    {
        PLOG(ERROR) << "failed to ibv_poll_cq";
        return 0;
    }

    for (size_t i = 0; i < actually_polled; ++i)
    {
        handle_wc(ibuf + i * kUserMessageSize, wc[i]);
    }
    return actually_polled;
}

template <size_t kEndpointNr>
void UnreliableRecvMessageConnection<kEndpointNr>::recv(size_t ep_id,
                                                        char *ibuf,
                                                        size_t msg_limit)
{
    DCHECK(inited_);
    msg_limit = std::min(msg_limit, kRecvLimit);

    static thread_local ibv_wc wc[kRecvLimit];

    size_t actual_polled = 0;
    while (actual_polled < msg_limit)
    {
        auto expect = msg_limit - actual_polled;
        DCHECK_GT(expect, 0);
        auto got = ibv_poll_cq(recv_cqs_[ep_id], expect, wc + actual_polled);
        if (unlikely(got < 0))
        {
            PLOG(FATAL) << "Failed to ibv_poll_cq. ";
            return;
        }
        actual_polled += got;
    }
    DCHECK_EQ(actual_polled, msg_limit);

    for (size_t i = 0; i < actual_polled; ++i)
    {
        handle_wc(ibuf + i * kUserMessageSize, wc[i]);
    }
}

template <size_t kEndpointNr>
void UnreliableRecvMessageConnection<kEndpointNr>::handle_wc(char *ibuf,
                                                             const ibv_wc &wc)
{
    if (unlikely(wc.status != IBV_WC_SUCCESS))
    {
        PLOG(FATAL) << "[umsg] Failed to handle wc: " << WRID(wc.wr_id)
                    << ", status: " << wc.status;
        return;
    }
    auto wrid = WRID(wc.wr_id);
    auto prefix = wrid.prefix;
    DCHECK_EQ(prefix, WRID_PREFIX_RELIABLE_RECV);
    auto th_id = wrid.u16_a;
    std::ignore = wrid.u16_b;
    auto batch_id = wrid.u16_c;

    DCHECK_LT(th_id, kEndpointNr);
    DCHECK_LT(batch_id, kRecvBuffer);

    size_t cur_idx = msg_recv_index_[th_id]++;
    auto actual_size = wc.imm_data;

    if (likely(ibuf != nullptr))
    {
        DCHECK_EQ(cur_idx % kRecvBuffer, batch_id)
            << "I thought they are equal. in fact, I think the WRID "
               "information is ground truth";

        char *buf =
            (char *) msg_pool_ +
            get_msg_pool_idx(th_id, cur_idx % kRecvBuffer) * kPostMessageSize;
        // add the header
        buf += 40;
        memcpy(ibuf, buf, actual_size);

        DVLOG(3) << "[umsg] Recved msg size " << actual_size << " from ep_id "
                 << th_id << ", batch_id: " << batch_id << ", cur_idx "
                 << cur_idx << ", hash is " << std::hex
                 << CityHash64(buf, actual_size) << " @" << (void *) buf
                 << ". WRID: " << wrid;
    }
    else
    {
        DVLOG(3) << "[umsg] Recved msg, wr: " << wrid << ", cur_idx "
                 << cur_idx;
    }

    if (unlikely((cur_idx + 1) % kPostRecvBufferBatch == 0))
    {
        size_t post_buf_idx =
            ((cur_idx + 1) % kRecvBuffer) +
            (kPostRecvBufferAdvanceBatch - 1) * kPostRecvBufferBatch;
        ibv_recv_wr *bad;
        DVLOG(3) << "[umsg] Posting another " << kPostRecvBufferBatch
                 << " recvs, th_id " << th_id << ". cur_idx " << cur_idx
                 << " i.e. recvs[" << th_id << "]["
                 << (post_buf_idx % kRecvBuffer) << "]";

        PLOG_IF(
            ERROR,
            ibv_post_recv(
                QPs_[th_id], &recvs[th_id][post_buf_idx % kRecvBuffer], &bad))
            << "failed to post recv";
    }
}

#endif