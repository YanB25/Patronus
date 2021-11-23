#include "RawMessageConnection.h"

#include <cassert>

RawMessageConnection::RawMessageConnection(RdmaContext &ctx,
                                           ibv_cq *cq,
                                           uint32_t messageNR)
    : AbstractMessageConnection(IBV_QPT_UD, 0, 40, ctx, cq, messageNR)
{
}
std::shared_ptr<RawMessageConnection> RawMessageConnection::newInstance(
    RdmaContext &ctx, ibv_cq *cq, uint32_t messageNR)
{
    return future::make_shared<RawMessageConnection>(ctx, cq, messageNR);
}

void RawMessageConnection::initSend()
{
}

void RawMessageConnection::sendRawMessage(RawMessage *m,
                                          uint32_t remoteQPN,
                                          ibv_ah *ah)
{
    if ((sendCounter & SIGNAL_BATCH) == 0 && sendCounter > 0)
    {
        ibv_wc wc;
        pollWithCQ(send_cq, 1, &wc);
    }

    rdmaSend(message,
             (uint64_t) m - sendPadding,
             sizeof(RawMessage) + sendPadding,
             messageLkey,
             ah,
             remoteQPN,
             (sendCounter & SIGNAL_BATCH) == 0);

    ++sendCounter;
}

std::ostream &operator<<(std::ostream &os, const RawMessage &msg)
{
    os << "{RawMessage Type: " << msg.type << ", node_id: " << msg.node_id
       << ", app_id: " << msg.app_id << ", addr: " << msg.addr
       << ", level: " << msg.level
       << ", inlined_buffer[0]: " << msg.inlined_buffer[0] << "}";
    return os;
}