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
#include "util/Pre.h"

template <size_t kEndpointNr>
class UnreliableRecvMessageConnection
{
    constexpr static int kUserMessageSize = config::umsg::kUserMessageSize;
    constexpr static int kPostMessageSize = config::umsg::kPostMessageSize;
    constexpr static size_t kRecvBuffer = config::umsg::kRecvBuffer;

    constexpr static size_t kPostRecvBufferAdvanceBatch =
        config::umsg::kPostRecvBufferAdvanceBatch;
    constexpr static size_t kPostRecvBufferBatchNr =
        config::umsg::kPostRecvBufferBatchNr;
    constexpr static size_t kPostRecvBufferBatch =
        config::umsg::kPostRecvBufferBatch;
    constexpr static size_t kRecvLimit = config::umsg::kRecvLimit;

    static_assert(kPostRecvBufferAdvanceBatch < kPostRecvBufferBatchNr);

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
        recv_mr_ = CHECK_NOTNULL(
            createMemoryRegion((uint64_t) msg_pool_, kMessageBufferSize, &ctx));
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
    struct msg_desc_t
    {
        const char *msg_addr;
        size_t msg_size;
        size_t batch_id;
    };
    size_t try_recv_no_cpy(size_t i_ep_id,
                           msg_desc_t *ptr_buf,
                           size_t msg_limit = 1);
    void return_buf_no_cpy(size_t th_id, msg_desc_t *msg_descs, size_t size)
    {
        if constexpr (::config::umsg::kEnableNoCpySafeCheck)
        {
            for (size_t i = 0; i < size; ++i)
            {
                mark_buffer_not_used(th_id, msg_descs[i].batch_id);
            }
        }
    }
    void recv(size_t i_ep_id, char *ibuf, size_t msg_limit = 1);
    void init();

private:
    void handle_wc(char *ibuf, const ibv_wc &wc);
    void handle_wc_no_cpy(msg_desc_t *, const ibv_wc &wc);
    void fills(ibv_sge &sge, ibv_recv_wr &wr, size_t ep_id, size_t batch_id);
    size_t buffer_addr_to_buf_id(void *buf_addr)
    {
        DCHECK_GE((uint64_t) buf_addr, (uint64_t) msg_pool_ + 40);
        uint64_t offset_buf = (char *) buf_addr - (char *) msg_pool_ - 40;
        DCHECK_EQ((uint64_t) offset_buf % kPostMessageSize, 0);
        auto internal = offset_buf / kPostMessageSize;
        auto buf_id = internal % kRecvBuffer;
        return buf_id;
    }

    size_t buf_id_to_batch_id(size_t buf_id) const
    {
        auto ret = buf_id / kPostRecvBufferBatch;
        DCHECK_LT(ret, kPostRecvBufferBatchNr);
        return ret;
    }
    size_t buf_id_to_slot_id(size_t buf_id) const
    {
        auto ret = buf_id % kPostRecvBufferBatch;
        return ret;
    }
    void mark_buffer_in_used(size_t th_id, size_t batch_id)
    {
        DCHECK_LT(th_id, batch_buffer_in_used_.size());
        DCHECK_LT(batch_id, batch_buffer_in_used_[th_id].size());
        batch_buffer_in_used_[th_id][batch_id]++;
        DCHECK_GE(batch_buffer_in_used_[th_id][batch_id], 0);
        DCHECK_LE(batch_buffer_in_used_[th_id][batch_id], kPostRecvBufferBatch);
    }
    void mark_buffer_not_used(size_t th_id, size_t batch_id)
    {
        DCHECK_LT(th_id, batch_buffer_in_used_.size());
        DCHECK_LT(batch_id, batch_buffer_in_used_[th_id].size());
        batch_buffer_in_used_[th_id][batch_id]--;
        DCHECK_GE(batch_buffer_in_used_[th_id][batch_id], 0);
        DCHECK_LE(batch_buffer_in_used_[th_id][batch_id], kPostRecvBufferBatch);
    }
    void validate_buf_batch_is_safe(size_t th_id, size_t buf_id)
    {
        auto batch_id = buf_id_to_batch_id(buf_id);
        DCHECK_LT(th_id, batch_buffer_in_used_.size());
        DCHECK_LT(batch_id, batch_buffer_in_used_[th_id].size());
        auto cnt = batch_buffer_in_used_[th_id][batch_id];
        CHECK_EQ(cnt, 0) << "Detected recv buffer batch in-use. th_id: "
                         << th_id << ", buf_id: " << buf_id
                         << ", batch_id: " << batch_id << ". Detail: "
                         << util::pre_iter(batch_buffer_in_used_[th_id]);
    }

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

    AlignedArray<size_t, kEndpointNr> msg_recv_indexes_{};
    AlignedArray<std::array<ssize_t, kPostRecvBufferBatchNr>, kEndpointNr>
        batch_buffer_in_used_{};
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
size_t UnreliableRecvMessageConnection<kEndpointNr>::try_recv_no_cpy(
    size_t ep_id, msg_desc_t *msg_desc, size_t msg_limit)
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
        handle_wc_no_cpy(&msg_desc[i], wc[i]);
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

    size_t cur_idx = msg_recv_indexes_[th_id]++;
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
        size_t next_buf_idx = (cur_idx + 1) % kRecvBuffer;
        size_t next_batch_id = buf_id_to_batch_id(next_buf_idx);
        // minus one, because we already reach "next".
        // that's why it's called next_batch_id
        size_t post_batch_id = next_batch_id + kPostRecvBufferAdvanceBatch - 1;
        post_batch_id %= kPostRecvBufferBatchNr;
        size_t post_idx = post_batch_id * kPostRecvBufferBatch;
        ibv_recv_wr *bad;
        DVLOG(3) << "[umsg] Posting another " << kPostRecvBufferBatch
                 << " recvs, th_id " << th_id << ". cur_idx " << cur_idx
                 << " i.e. recvs[" << th_id << "][" << post_idx << "]";

        PLOG_IF(ERROR,
                ibv_post_recv(QPs_[th_id], &recvs[th_id][post_idx], &bad))
            << "failed to post recv";
    }
}

template <size_t kEndpointNr>
void UnreliableRecvMessageConnection<kEndpointNr>::handle_wc_no_cpy(
    msg_desc_t *msg_desc, const ibv_wc &wc)
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
    auto buf_id = wrid.u16_c;

    DCHECK_LT(th_id, kEndpointNr);
    DCHECK_LT(buf_id, kRecvBuffer);

    size_t cur_idx = msg_recv_indexes_[th_id]++;
    auto actual_size = wc.imm_data;

    if (likely(msg_desc != nullptr))
    {
        DCHECK_EQ(cur_idx % kRecvBuffer, buf_id)
            << "I thought they are equal. In fact, I think the WRID "
               "information is ground truth";

        char *buf = (char *) msg_pool_ +
                    get_msg_pool_idx(th_id, buf_id) * kPostMessageSize;
        // add the header
        buf += 40;

        msg_desc->msg_addr = buf;
        msg_desc->msg_size = actual_size;

        if constexpr (::config::umsg::kEnableNoCpySafeCheck)
        {
            auto batch_id = buf_id_to_batch_id(buf_id);
            msg_desc->batch_id = batch_id;
            mark_buffer_in_used(th_id, batch_id);
        }

        DVLOG(3) << "[umsg] Recved msg size " << actual_size << " from ep_id "
                 << th_id << ", buf_id: " << buf_id << ", cur_idx " << cur_idx
                 << ", hash is " << std::hex << CityHash64(buf, actual_size)
                 << " @" << (void *) buf << ". WRID: " << wrid;
    }
    else
    {
        DVLOG(3) << "[umsg] Recved msg, wr: " << wrid << ", cur_idx "
                 << cur_idx;
    }

    if (unlikely((cur_idx + 1) % kPostRecvBufferBatch == 0))
    {
        size_t next_buf_idx = (cur_idx + 1) % kRecvBuffer;
        size_t next_batch_id = buf_id_to_batch_id(next_buf_idx);
        // minus one, because we already reach "next".
        // that's why it's called next_batch_id
        size_t post_batch_id = next_batch_id + kPostRecvBufferAdvanceBatch - 1;
        post_batch_id %= kPostRecvBufferBatchNr;
        size_t post_idx = post_batch_id * kPostRecvBufferBatch;

        ibv_recv_wr *bad;
        DVLOG(3) << "[umsg] Posting another " << kPostRecvBufferBatch
                 << " recvs, th_id " << th_id << ". cur_idx " << cur_idx
                 << " i.e. recvs[" << th_id << "][" << post_idx << "]";

        DCHECK_EQ(post_idx % kPostRecvBufferBatch, 0);

        if constexpr (::config::umsg::kEnableNoCpySafeCheck)
        {
            validate_buf_batch_is_safe(th_id, post_idx);
        }

        PLOG_IF(ERROR,
                ibv_post_recv(QPs_[th_id], &recvs[th_id][post_idx], &bad))
            << "failed to post recv";
    }
}

#endif