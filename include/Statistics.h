#include <cstdint>
#include <iostream>

extern uint64_t dirSendDataCounter;
extern uint64_t dirSendControlCounter;
extern uint64_t dirRecvDataCounter;
extern uint64_t dirRecvControlCounter;

extern uint64_t agentSendDataCounter;
extern uint64_t agentSendControlCounter;
extern uint64_t agentRecvDataCounter;
extern uint64_t agentRecvControlCounter;

extern uint64_t agentWriteShared;
extern uint64_t agentWriteMissDirty;
extern uint64_t agentWriteMissShared;
extern uint64_t agentReadMissDirty;

class Statistics {
public:
  static void clear() {
    dirSendDataCounter = 0;
    dirSendControlCounter = 0;
    dirRecvDataCounter = 0;
    dirRecvControlCounter = 0;

    agentSendDataCounter = 0;
    agentSendControlCounter = 0;
    agentRecvDataCounter = 0;
    agentRecvControlCounter = 0;

    agentWriteShared = 0;
    agentWriteMissDirty = 0;
    agentWriteMissShared = 0;
    agentReadMissDirty = 0;
  }

  static uint64_t dir_recv_all() {
    return (dirRecvDataCounter + dirRecvControlCounter);
  }

  static uint64_t dir_send_all() {
    return (dirSendDataCounter + dirSendControlCounter);
  }

  static void dispaly() {

    // std::cout << "dir recv " << (dirRecvDataCounter + dirRecvControlCounter) << std::endl;
    // std::cout << "dir send " << (dirSendDataCounter + dirSendControlCounter) << std::endl;
    // std::cout << "dir send data counter: " << dirSendDataCounter << "\n";
    // std::cout << "dir send contol counter: " << dirSendControlCounter << "\n";
    // std::cout << "agent send data counter: " << agentSendDataCounter << "\n";
    // std::cout << "agent send contol counter: " << agentSendControlCounter
    //           << "\n";
    // std::cout << "agentWriteShared: " << agentWriteShared << "\n";
    // std::cout << "agentWriteMissDirty: " << agentWriteMissDirty << "\n";
    // std::cout << "agentWriteMissShared: " << agentWriteMissShared << "\n";
    // std::cout << "agentReadMissDirty: " << agentReadMissDirty << "\n";
  }
};
