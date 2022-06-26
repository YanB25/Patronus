#pragma once
#ifndef RELIABLE_MESSAGE_CONNECTION_H_
#define RELIABLE_MESSAGE_CONNECTION_H_

#include <glog/logging.h>
#include <infiniband/verbs.h>

#include "umsg/Config.h"
#include "umsg/UnreliableReceiver.h"
#include "umsg/UnreliableSender.h"

template <size_t kEndpointNr>
class ReliableRecvMessageConnection;

template <size_t kEndpointNr>
class ReliableSendMessageConnection;

template <size_t kEndpointNr>
class UnreliableConnection
{
public:
    /**
     * @brief Construct a new Reliable Connection object
     *
     * @param mm the rdma buffer
     * @param mmSize
     */
    UnreliableConnection(uint64_t mm,
                         size_t mmSize,
                         const std::vector<RemoteConnection> &remote_infos);
    ~UnreliableConnection();
    void send(size_t i_ep_id,
              const char *buf,
              size_t size,
              uint16_t node_id,
              size_t target_ep_id)
    {
        send_->send(i_ep_id, buf, size, node_id, target_ep_id);
    }
    bool prepare_send(size_t i_ep_id,
                      const char *buf,
                      size_t size,
                      uint16_t node_id,
                      size_t target_ep_id)
    {
        return send_->prepare_send(i_ep_id, buf, size, node_id, target_ep_id);
    }
    void commit_send(size_t i_ep_id)
    {
        send_->commit_send(i_ep_id);
    }
    size_t try_recv(size_t i_ep_id, char *ibuf, size_t limit = 1)
    {
        return recv_->try_recv(i_ep_id, ibuf, limit);
    }
    using msg_desc_t =
        typename UnreliableRecvMessageConnection<kEndpointNr>::msg_desc_t;
    size_t try_recv_no_cpy(size_t i_ep_id,
                           msg_desc_t *msg_descs,
                           size_t msg_limit = 1)
    {
        return recv_->try_recv_no_cpy(i_ep_id, msg_descs, msg_limit);
    }
    void recv(size_t i_ep_id, char *ibuf, size_t limit = 1)
    {
        return recv_->recv(i_ep_id, ibuf, limit);
    }
    void return_buf_no_cpy(size_t th_id, msg_desc_t *msg_descs, size_t size)
    {
        return recv_->return_buf_no_cpy(th_id, msg_descs, size);
    }

private:
    RdmaContext &context()
    {
        return ctx_;
    }
    ibv_qp *get_qp(size_t ep_id)
    {
        DCHECK_LT(ep_id, kEndpointNr);
        return QPs_[ep_id];
    }
    friend class DSMKeeper;
    // for both
    RdmaContext ctx_;

    std::array<ibv_qp *, kEndpointNr> QPs_{};
    std::array<ibv_cq *, kEndpointNr> send_cqs_{};
    std::array<ibv_cq *, kEndpointNr> recv_cqs_{};
    // for sender

    ibv_mr *send_mr_{nullptr};
    uint32_t send_lkey_{0};
    ibv_cq *send_cq_;

    std::unique_ptr<UnreliableRecvMessageConnection<kEndpointNr>> recv_;
    std::unique_ptr<UnreliableSendMessageConnection<kEndpointNr>> send_;
};

template <size_t kEndpointNr>
UnreliableConnection<kEndpointNr>::UnreliableConnection(
    uint64_t mm,
    size_t mmSize,
    const std::vector<RemoteConnection> &remote_infos)
{
    CHECK(createContext(
        &ctx_, 1, 1, 0, IBV_EXP_THREAD_SINGLE, IBV_EXP_MSG_HIGH_BW));

    for (size_t i = 0; i < kEndpointNr; ++i)
    {
        recv_cqs_[i] = CHECK_NOTNULL(
            createCompleteQueue(&ctx_,
                                config::umsg::recv::kMaxCQE,
                                ctx_.res_doms[i] ? ctx_.res_doms[i] : nullptr));
    }
    for (size_t i = 0; i < kEndpointNr; ++i)
    {
        send_cqs_[i] = CHECK_NOTNULL(
            createCompleteQueue(&ctx_,
                                config::umsg::sender::kMaxCQE,
                                ctx_.res_doms[i] ? ctx_.res_doms[i] : nullptr));
    }

    for (size_t i = 0; i < kEndpointNr; ++i)
    {
        CHECK(createQueuePair(&QPs_[i],
                              IBV_QPT_UD,
                              send_cqs_[i],
                              recv_cqs_[i],
                              &ctx_,
                              config::umsg::sender::kMaxSendWr,
                              config::umsg::recv::kMaxRecvWr,
                              config::umsg::kMaxInlinedSize,
                              ctx_.res_doms[i] ? ctx_.res_doms[i] : nullptr));
    }

    for (size_t i = 0; i < kEndpointNr; ++i)
    {
        auto *qp = QPs_[i];
        CHECK_EQ(qp->qp_type, IBV_QPT_UD);
        CHECK(modifyUDtoRTS(qp, &ctx_));
    }

    send_ = std::make_unique<UnreliableSendMessageConnection<kMaxAppThread>>(
        QPs_,
        send_cqs_,
        remote_infos,
        mm,
        mmSize,
        ctx_,
        config::umsg::sender::kMaxSendWr);
    recv_ = std::make_unique<UnreliableRecvMessageConnection<kMaxAppThread>>(
        QPs_, recv_cqs_, ctx_);
}

template <size_t kEndpointNr>
UnreliableConnection<kEndpointNr>::~UnreliableConnection()
{
    // manually call ctor here
    recv_.reset();
    send_.reset();

    for (size_t i = 0; i < kEndpointNr; ++i)
    {
        CHECK(destroyQueuePair(QPs_[i])) << "** Failed to destroy QP";
    }
    for (size_t i = 0; i < kEndpointNr; ++i)
    {
        CHECK(destroyCompleteQueue(send_cqs_[i])) << "** Failed to destroy CQ";
    }
    for (size_t i = 0; i < kEndpointNr; ++i)
    {
        CHECK(destroyCompleteQueue(recv_cqs_[i])) << "** Failed to destroy CQ";
    }
    CHECK(destroyContext(&ctx_)) << "** Failed to destroy context.";
}

#endif