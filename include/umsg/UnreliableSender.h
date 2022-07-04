#pragma once
#ifndef RELIABLE_SENDER_H_
#define RELIABLE_SENDER_H_
#include <infiniband/verbs.h>

#include "Common.h"
#include "Connection.h"
#include "Rdma.h"
#include "umsg/Config.h"
#include "umsg/UnreliableConnection.h"

struct SenderBatchContext
{
    ibv_sge sges[config::umsg::kSenderBatchSize];
    ibv_send_wr wrs[config::umsg::kSenderBatchSize];
    ibv_send_wr *bad_wr;
    size_t idx{0};
    size_t send_signal{0};
};

template <size_t kEndpointNr>
class UnreliableSendMessageConnection
{
public:
    constexpr static size_t V = ::config::verbose::kUmsg;
    constexpr static size_t kUserMessageSize = config::umsg::kUserMessageSize;
    constexpr static int kSendBatch = config::umsg::kSenderBatchSize;
    UnreliableSendMessageConnection(
        std::array<ibv_qp *, kEndpointNr> &QPs,
        std::array<ibv_cq *, kEndpointNr> &send_cqs,
        const std::vector<RemoteConnection> &remote_info,
        uint64_t mm,
        size_t mm_size,
        RdmaContext &ctx,
        size_t send_depth)
        : QPs_(QPs),
          send_cqs_(send_cqs),
          remote_infos_(remote_info),
          ctx_(ctx),
          send_depth_(send_depth),
          max_allowed_ongoing(send_depth / kSendBatch - 1)
    {
        send_mr_ =
            CHECK_NOTNULL(createMemoryRegion((uint64_t) mm, mm_size, &ctx_));
        lkey_ = send_mr_->lkey;

        for (size_t i = 0; i < kEndpointNr; ++i)
        {
            auto &wr = wrs_[i];
            auto &sge = sges_[i];
            memset(&wr, 0, sizeof(ibv_send_wr));
            memset(&sge, 0, sizeof(ibv_sge));
            sge.lkey = lkey_;
            wr.sg_list = &sge;
            wr.num_sge = 1;
            wr.opcode = IBV_WR_SEND_WITH_IMM;
            wr.next = nullptr;
            wr.wr.ud.remote_qkey = UD_PKEY;
        }

        for (size_t i = 0; i < kEndpointNr; ++i)
        {
            auto &batch_ctx = batch_contexts_[i];
            for (size_t b = 0; b < kSendBatch; ++b)
            {
                batch_ctx.idx = 0;
                batch_ctx.send_signal = 0;
                batch_ctx.bad_wr = nullptr;
                auto &sge = batch_ctx.sges[b];
                auto &wr = batch_ctx.wrs[b];
                memset(&wr, 0, sizeof(wr));
                memset(&sge, 0, sizeof(sge));
                sge.lkey = lkey_;
                wr.sg_list = &sge;
                wr.num_sge = 1;
                wr.opcode = IBV_WR_SEND_WITH_IMM;
                wr.next = nullptr;
                wr.wr.ud.remote_qkey = UD_PKEY;
            }
        }
    }
    ~UnreliableSendMessageConnection()
    {
        CHECK(destroyMemoryRegion(send_mr_));
        send_mr_ = nullptr;
    }
    void send(size_t i_ep_id,
              const char *buf,
              size_t size,
              size_t node_id,
              size_t target_ep_id);
    [[nodiscard]] bool prepare_send(size_t i_ep_id,
                                    const char *buf,
                                    size_t size,
                                    size_t node_id,
                                    size_t target_ep_id);
    void commit_send(size_t i_ep_id);

private:
    ssize_t poll_cq(size_t ep_id);

    std::array<ibv_qp *, kEndpointNr> &QPs_{};
    std::array<ibv_cq *, kEndpointNr> send_cqs_{};
    const std::vector<RemoteConnection> &remote_infos_;
    RdmaContext &ctx_;
    size_t send_depth_{0};

    ibv_mr *send_mr_{nullptr};
    uint32_t lkey_{0};

    AlignedArray<size_t, kEndpointNr> msg_send_index_{};
    // rate limiter for each QP
    const ssize_t max_allowed_ongoing{0};
    AlignedArray<ssize_t, kEndpointNr> ongoing_signals_{};

    AlignedArray<ibv_sge, kEndpointNr> sges_;
    AlignedArray<ibv_send_wr, kEndpointNr> wrs_;
    AlignedArray<ibv_send_wr *, kEndpointNr> bad_wrs_;

    std::array<SenderBatchContext, kEndpointNr> batch_contexts_;
};

template <size_t kEndpointNr>
void UnreliableSendMessageConnection<kEndpointNr>::commit_send(size_t i_ep_id)
{
    auto &batch_ctx = batch_contexts_[i_ep_id];
    auto &msg_idx = msg_send_index_[i_ep_id];
    auto &ongoing_signal = ongoing_signals_[i_ep_id];
    if (batch_ctx.idx == 0)
    {
        return;
    }
    size_t signal_nr = 0;
    for (size_t i = 0; i < batch_ctx.idx; ++i)
    {
        msg_idx++;
        if (msg_idx % kSendBatch == 0)
        {
            signal_nr++;
            auto &wr = batch_ctx.wrs[i];
            wr.send_flags |= IBV_SEND_SIGNALED;
        }
    }
    // signal_nr should be 0 or 1.
    DCHECK_GE(signal_nr, 0);
    DCHECK_LT(signal_nr, max_allowed_ongoing)
        << "to make sure the poll_cq loop will not be dead loop";

    ongoing_signal += signal_nr;
    DVLOG(V) << "[umsg] commit_send: ongoing_signal(after): " << ongoing_signal
             << ", batch_signal: " << signal_nr
             << ", max_allowd: " << max_allowed_ongoing
             << ", will wait: " << (ongoing_signal >= max_allowed_ongoing)
             << ". wr_size: " << batch_ctx.idx;
    if (unlikely(ongoing_signal >= max_allowed_ongoing))
    {
        do
        {
            poll_cq(i_ep_id);
        } while (ongoing_signal >= max_allowed_ongoing);
    }
    auto &wr = batch_ctx.wrs[0];
    auto &bad_wr = batch_ctx.bad_wr;
    for (size_t i = 0; i < batch_ctx.idx; ++i)
    {
        bool last = (i + 1 == batch_ctx.idx);
        if (last)
        {
            batch_ctx.wrs[i].next = nullptr;
        }
        else
        {
            batch_ctx.wrs[i].next = &batch_ctx.wrs[i + 1];
        }
    }
    if (ibv_post_send(QPs_[i_ep_id], &wr, &bad_wr))
    {
        PLOG(FATAL) << "** Send with RDMA_SEND failed. "
                    << ", ongoing_signal: " << ongoing_signal;
    }
    batch_ctx.idx = 0;
    batch_ctx.send_signal = 0;
}

template <size_t kEndpointNr>
bool UnreliableSendMessageConnection<kEndpointNr>::prepare_send(
    size_t i_ep_id,
    const char *buf,
    size_t size,
    size_t node_id,
    size_t target_ep_id)
{
    auto &batch_ctx = batch_contexts_[i_ep_id];
    if (unlikely(batch_ctx.idx >= kSendBatch))
    {
        return false;
    }
    DCHECK_LT(target_ep_id, kEndpointNr);
    DCHECK_LT(i_ep_id, kEndpointNr);
    DCHECK_LE(size, kUserMessageSize) << "[umsg] message size exceed limits";

    bool inlined = (size <= 32);

    auto wrid =
        WRID(WRID_PREFIX_RELIABLE_SEND, target_ep_id, i_ep_id, size).val;

    auto *ah = remote_infos_[node_id].ud_conn.AH[i_ep_id][target_ep_id];
    auto qpn = remote_infos_[node_id].ud_conn.QPN[target_ep_id];

    auto idx = batch_ctx.idx++;
    DCHECK_LE(idx, kSendBatch);
    auto &sge = batch_ctx.sges[idx];
    auto &wr = batch_ctx.wrs[idx];
    sge.addr = (uint64_t) buf;
    sge.length = size;
    wr.wr_id = wrid;
    wr.send_flags = 0;
    wr.wr.ud.ah = ah;
    wr.wr.ud.remote_qpn = qpn;
    wr.imm_data = size;
    wr.next = nullptr;
    if (inlined)
    {
        wr.send_flags |= IBV_SEND_INLINE;
    }

    DVLOG(V) << "[umsg] EP: " << i_ep_id << " PREPARED sending to node "
             << node_id << " QP[" << target_ep_id << "] with size " << size
             << ", inlined: " << inlined << ". Hash: " << std::hex
             << CityHash64(buf, size);
    return true;
}

template <size_t kEndpointNr>
void UnreliableSendMessageConnection<kEndpointNr>::send(size_t i_ep_id,
                                                        const char *buf,
                                                        size_t size,
                                                        size_t node_id,
                                                        size_t target_ep_id)
{
    DCHECK_LT(target_ep_id, kEndpointNr);
    DCHECK_LT(i_ep_id, kEndpointNr);
    DCHECK_LE(size, kUserMessageSize) << "[umsg] message size exceed limits";

    auto &msg_idx = msg_send_index_[i_ep_id];
    msg_idx++;
    bool signal = false;

    auto &ongoing_signal = ongoing_signals_[i_ep_id];
    if (unlikely(msg_idx % kSendBatch == 0))
    {
        DVLOG(V) << "[umsg] triggers one poll for i_ep_id: " << i_ep_id;
        signal = true;
        ongoing_signal++;
        if (unlikely(ongoing_signal >= max_allowed_ongoing))
        {
            do
            {
                poll_cq(i_ep_id);
            } while (ongoing_signal >= max_allowed_ongoing);
        }
        // else
        // {
        //     poll_cq(i_ep_id);
        // }
    }
    bool inlined = (size <= 32);

    DVLOG(V) << "[umsg] EP: " << i_ep_id << " sending to node " << node_id
             << " QP[" << target_ep_id << "] with size " << size
             << ", inlined: " << inlined << ", signal: " << signal
             << ". Hash: " << std::hex << CityHash64(buf, size);

    auto wrid =
        WRID(WRID_PREFIX_RELIABLE_SEND, target_ep_id, i_ep_id, size).val;

    auto *ah = remote_infos_[node_id].ud_conn.AH[i_ep_id][target_ep_id];
    auto qpn = remote_infos_[node_id].ud_conn.QPN[target_ep_id];

    auto &sge = sges_[i_ep_id];
    auto &wr = wrs_[i_ep_id];
    auto &bad_wr = bad_wrs_[i_ep_id];
    sge.addr = (uint64_t) buf;
    sge.length = size;
    wr.wr_id = wrid;
    wr.send_flags = 0;
    wr.wr.ud.ah = ah;
    wr.wr.ud.remote_qpn = qpn;
    wr.imm_data = size;
    if (signal)
    {
        wr.send_flags |= IBV_SEND_SIGNALED;
    }
    if (inlined)
    {
        wr.send_flags |= IBV_SEND_INLINE;
    }
    if (ibv_post_send(QPs_[i_ep_id], &wr, &bad_wr))
    {
        PLOG(FATAL) << "** Send with RDMA_SEND failed. wrid: " << WRID(wrid)
                    << ". To target_ep_id: " << target_ep_id
                    << ", node_id: " << node_id
                    << ", ongoing_signal: " << ongoing_signal
                    << " (effectively " << ongoing_signal * kSendBatch << ")";
    }
}

template <size_t kEndpointNr>
ssize_t UnreliableSendMessageConnection<kEndpointNr>::poll_cq(size_t ep_id)
{
    thread_local static ibv_wc wc[2 * kSendBatch];

    size_t polled = ibv_poll_cq(send_cqs_[ep_id], 2 * kSendBatch, wc);
    if (unlikely(polled < 0))
    {
        PLOG(ERROR) << "Failed to poll cq.";
    }
    for (size_t i = 0; i < polled; ++i)
    {
        if (unlikely(wc[i].status != IBV_WC_SUCCESS))
        {
            LOG(ERROR) << "[umsg] sender wc failed. " << WRID(wc[i].wr_id);
        }
        else
        {
            auto wrid = WRID(wc[i].wr_id);
            auto prefix = wrid.prefix;
            auto dir_id = wrid.u16_a;
            auto ep_id = wrid.u16_b;
            auto size = wrid.u16_c;
            std::ignore = dir_id;
            std::ignore = size;
            DCHECK_EQ(prefix, WRID_PREFIX_RELIABLE_SEND);
            ongoing_signals_[ep_id]--;
            DCHECK_GE(ongoing_signals_[ep_id], 0);
        }
    }
    return polled;
}

#endif