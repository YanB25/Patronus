#ifndef __ABSTRACTMESSAGECONNECTION_H__
#define __ABSTRACTMESSAGECONNECTION_H__

#include "Common.h"

#define SIGNAL_BATCH 31

class Message;

// #messageNR send pool and #messageNR message pool
class AbstractMessageConnection
{

    const static int kBatchCount = 4;

protected:
    ibv_qp *message{nullptr};  // ud or raw packet
    uint16_t messageNR{0};

    ibv_mr *messageMR{nullptr};
    void *messagePool{0};
    uint32_t messageLkey{0};

    uint16_t curMessage{0};

    void *sendPool{nullptr};
    uint16_t curSend{0};

    ibv_recv_wr *recvs[kBatchCount];
    ibv_sge *recv_sgl[kBatchCount];
    uint32_t subNR{0};

    ibv_cq *send_cq{nullptr};
    uint64_t sendCounter{0};

    uint16_t sendPadding{0};  // ud: 0
                           // rp: ?
    uint16_t recvPadding{0};  // ud: 40
                           // rp: ?

public:
    AbstractMessageConnection(ibv_qp_type type,
                              uint16_t sendPadding,
                              uint16_t recvPadding,
                              RdmaContext &ctx,
                              ibv_cq *cq,
                              uint32_t messageNR);
    AbstractMessageConnection(const AbstractMessageConnection&) = delete;
    AbstractMessageConnection& operator=(const AbstractMessageConnection&) = delete;
    AbstractMessageConnection(AbstractMessageConnection&& rhs)
    {
        *this = std::move(rhs);
    }
    AbstractMessageConnection& operator=(AbstractMessageConnection&& rhs)
    {
        message = rhs.message;
        rhs.message = nullptr;
        messageNR = rhs.messageNR;
        messageMR = rhs.messageMR;
        messageMR = nullptr;
        messagePool = rhs.messagePool;
        rhs.messagePool = nullptr;
        messageLkey = rhs.messageLkey;
        curMessage = rhs.curMessage;
        sendPool = rhs.sendPool;
        rhs.sendPool = nullptr;
        curSend = rhs.curSend;
        for (size_t i = 0; i < kBatchCount; ++i)
        {
            recvs[i] = rhs.recvs[i];
            rhs.recvs[i] = nullptr;
            recv_sgl[i] = rhs.recv_sgl[i];
            rhs.recv_sgl[i] = nullptr;
        }
        subNR = rhs.subNR;
        send_cq = rhs.send_cq;
        rhs.send_cq = nullptr;
        sendCounter = rhs.sendCounter;
        sendPadding = rhs.sendPadding;
        recvPadding = rhs.recvPadding;
        return *this;
    }

    void initRecv();

    char *getMessage();
    char *getSendPool();

    void destroy();

    uint32_t getQPN()
    {
        return message->qp_num;
    }
};

#endif /* __ABSTRACTMESSAGECONNECTION_H__ */
