#pragma once
#ifndef RELIABLE_SENDER_H_
#define RELIABLE_SENDER_H_
#include <infiniband/verbs.h>

#include "Common.h"
#include "umsg/Config.h"
#include "umsg/UnreliableMessageConnection.h"

template <size_t kEndpointNr>
class UnreliableSendMessageConnection
{
public:
    constexpr static size_t kUserMessageSize = config::umsg::kUserMessageSize;
    const static int kSendBatch = config::umsg::kSenderBatchSize;
    UnreliableSendMessageConnection(
        std::array<ibv_qp *, kEndpointNr>> &QPs,
        std::array<ibv_cq *> send_cqs,
        const std::vector<RemoteConnection> &remote_info,
        uint32_t lkey,
        size_t send_depth)
        : QPs_(QPs),
          send_cqs_(send_cqs),
          remote_infos_(remote_info),
          lkey_(lkey),
          send_depth_(send_depth),
          max_allowed_ongoing(send_depth / kSendBatch)
    {
    }
    ~UnreliableSendMessageConnection() = default;
    void send(size_t ep_id,
              size_t node_id,
              const char *buf,
              size_t size,
              size_t dir_id);

private:
    ssize_t poll_cq(size_t ep_id);

    std::array<ibv_qp *, kEndpointNr> &QPs_;
    std::array<ibv_cq *, kEndpointNr> send_cqs_;
    const std::vector<RemoteInfo> &remote_infos_;
    uint32_t lkey_{0};
    size_t send_depth_{0};

    size_t msg_send_index_[kEndpointNr]{};
    // rate limiter for each QP
    const ssize_t max_allowed_ongoing{0};
    ssize_t ongoing_signals_[kEndpointNr]{};
};

template <size_t kEndpointNr>
void UnreliableSendMessageConnection<kEndpointNr>::send(
    size_t ep_id, size_t node_id, const char *buf, size_t size, size_t dir_id)
{
    DCHECK_LT(dir_id, NR_DIRECTORY);
    DCHECK_LE(size, kUserMessageSize) << "[rmsg] message size exceed limits";

    auto &msg_idx = msg_send_index_[ep_id];
    msg_idx++;
    bool signal = false;

    DCHECK_LT(ep_id, kEndpointNr);
    auto &ongoing_signal = ongoing_signals_[ep_id];
    if (unlikely(msg_idx % kSendBatch == 0))
    {
        DVLOG(3) << "[rmsg] triggers one poll for ep_id: " << ep_id;
        signal = true;
        ongoing_signal++;
        if (unlikely(ongoing_signal >= max_allowed_ongoing))
        {
            do
            {
                poll_cq(ep_id);
            } while (ongoing_signal >= max_allowed_ongoing);
        }
        else
        {
            poll_cq(ep_id);
        }
    }
    bool inlined = (size <= 32);

    DVLOG(3) << "[rmsg] sending to QP[" << dir_id << "] with size " << size
             << ", inlined: " << inlined << ", signal: " << signal;

    auto wrid = WRID(WRID_PREFIX_RELIABLE_SEND, dir_id, ep_id, size).val;

    auto *ah = remote_infos_[node_id].appToDirAh[ep_id][dir_id];
    auto qpn = remote_infos_[node_id].dirMessageQPN[dir_id];
    bool succ = rdmaSend(QPs_[ep_id],
                         (uint64_t) buf,
                         size,
                         lkey_,
                         ah,
                         qpn,
                         signal,
                         inline,
                         wrid);

    PLOG_IF(FATAL, !succ) << "** Send with RDMA_SEND failed. wrid: "
                          << WRID(wrid) << ". At dir_id: " << dir_id
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
            LOG(ERROR) << "[rmsg] sender wc failed. " << WRID(wc[i].wr_id);
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