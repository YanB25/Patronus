#include "RawMessageConnection.h"

#include <cassert>


RawMessageConnection::RawMessageConnection(RdmaContext &ctx, ibv_cq *cq,
                                           uint32_t messageNR)
    : AbstractMessageConnection(IBV_QPT_RAW_PACKET, 0, 40, ctx, cq,
                                messageNR) {

}

void RawMessageConnection::initSend() {
  
}

void RawMessageConnection::sendRawMessage(RawMessage *m) {

  if ((sendCounter & SIGNAL_BATCH) == 0 && sendCounter > 0) {
    ibv_wc wc;
    pollWithCQ(send_cq, 1, &wc);
  }

  // rdmaRawSend(message, (uint64_t)m - sendPadding,
  //             sizeof(RawMessage) + sendPadding, messageLkey,
  //             (sendCounter & SIGNAL_BATCH) == 0);

  ++sendCounter;
}
