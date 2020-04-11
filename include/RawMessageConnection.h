#ifndef __RAWMESSAGECONNECTION_H__
#define __RAWMESSAGECONNECTION_H__

#include "AbstractMessageConnection.h"

#include <thread>


enum RawMessageType : uint8_t {

};

struct RawMessage {
  uint16_t qpn;
  uint8_t mtype;

  uint32_t dirKey;

  // LITTLE ENDIAN
  uint8_t nodeID : 4;
  uint8_t dirNodeID : 4;

  uint8_t appID;

  union {
    uint16_t mybitmap;
    uint16_t agentID;
  };

  uint8_t state;
  uint16_t bitmap;

  uint8_t is_app_req;

  uint32_t index;
  uint32_t tag;

  uint64_t destAddr;

  
} __attribute__((packed));


class RawMessageConnection : public AbstractMessageConnection {

public:
  RawMessageConnection(RdmaContext &ctx, ibv_cq *cq, uint32_t messageNR);

  void initSend();
  void sendRawMessage(RawMessage *m);
};

#endif /* __RAWMESSAGECONNECTION_H__ */
