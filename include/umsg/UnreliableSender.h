#pragma once
#ifndef RELIABLE_SENDER_H_
#define RELIABLE_SENDER_H_
#include <infiniband/verbs.h>

#include "Common.h"
#include "Connection.h"
#include "Rdma.h"
#include "umsg/Config.h"
#include "umsg/UnreliableConnection.h"

template <size_t kEndpointNr>
class UnreliableSendMessageConnection
{
public:
    constexpr static size_t kUserMessageSize = config::umsg::kUserMessageSize;
    const static int kSendBatch = config::umsg::kSenderBatchSize;
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
          max_allowed_ongoing(send_depth / kSendBatch)
    {
        send_mr_ =
            CHECK_NOTNULL(createMemoryRegion((uint64_t) mm, mm_size, &ctx_));
        lkey_ = send_mr_->lkey;
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

private:
    ssize_t poll_cq(size_t ep_id);

    std::array<ibv_qp *, kEndpointNr> &QPs_;
    std::array<ibv_cq *, kEndpointNr> send_cqs_;
    const std::vector<RemoteConnection> &remote_infos_;
    RdmaContext &ctx_;
    size_t send_depth_{0};

    ibv_mr *send_mr_{nullptr};
    uint32_t lkey_{0};

    size_t msg_send_index_[kEndpointNr]{};
    // rate limiter for each QP
    const ssize_t max_allowed_ongoing{0};
    ssize_t ongoing_signals_[kEndpointNr]{};
};

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
        DVLOG(3) << "[umsg] triggers one poll for i_ep_id: " << i_ep_id;
        signal = true;
        ongoing_signal++;
        if (unlikely(ongoing_signal >= max_allowed_ongoing))
        {
            do
            {
                poll_cq(i_ep_id);
            } while (ongoing_signal >= max_allowed_ongoing);
        }
        else
        {
            poll_cq(i_ep_id);
        }
    }
    bool inlined = (size <= 32);

    DVLOG(3) << "[umsg] EP: " << i_ep_id << " sending to node " << node_id
             << " QP[" << target_ep_id << "] with size " << size
             << ", inlined: " << inlined << ", signal: " << signal
             << ". Hash: " << std::hex << CityHash64(buf, size);

    auto wrid =
        WRID(WRID_PREFIX_RELIABLE_SEND, target_ep_id, i_ep_id, size).val;

    auto *ah = remote_infos_[node_id].ud_conn.AH[i_ep_id][target_ep_id];
    auto qpn = remote_infos_[node_id].ud_conn.QPN[target_ep_id];
    bool succ = rdmaSend(QPs_[i_ep_id],
                         (uint64_t) buf,
                         size,
                         lkey_,
                         ah,
                         qpn,
                         signal,
                         inlined,
                         wrid,
                         size /* imm */);

    PLOG_IF(FATAL, !succ) << "** Send with RDMA_SEND failed. wrid: "
                          << WRID(wrid) << ". To target_ep_id: " << target_ep_id
                          << ", node_id: " << node_id
                          << ", ongoing_signal: " << ongoing_signal
                          << " (effectively " << ongoing_signal * kSendBatch
                          << ") ";
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