#include "Directory.h"
#include "Common.h"

#include "Connection.h"

#include <gperftools/profiler.h>

Directory::Directory(DirectoryConnection *dCon, RemoteConnection *remoteInfo,
                     uint32_t machineNR, uint16_t dirID, uint16_t nodeID)
    : dCon(dCon), remoteInfo(remoteInfo), machineNR(machineNR), dirID(dirID),
      nodeID(nodeID), dirTh(nullptr) {


  { // chunck alloctor
    GlobalAddress dsm_start;
    uint64_t per_directory_dsm_size = dCon->dsmSize / NR_DIRECTORY;
    dsm_start.nodeID = nodeID;
    dsm_start.addr = per_directory_dsm_size * dirID;
    chunckAlloc = new GlobalAllocator(dsm_start, per_directory_dsm_size);
  }

  dirTh = new std::thread(&Directory::dirThread, this);
}

Directory::~Directory() { delete chunckAlloc; }

void Directory::dirThread() {

  bindCore(12 - dirID);
  Debug::notifyInfo("dir %d launch!\n", dirID);

  while (true) {
    struct ibv_wc wc;
    pollWithCQ(dCon->cq, 1, &wc);

    switch (int(wc.opcode)) {
    case IBV_WC_RECV: // control message
    {

      break;
    }
    case IBV_WC_RDMA_WRITE: {
      break;
    }
    case IBV_WC_RECV_RDMA_WITH_IMM: {

      break;
    }
    default:
      assert(false);
    }
  }
}

void Directory::sendData2App(const RawMessage *m) {


  rdmaWrite(dCon->data2app[m->appID][m->nodeID],
            (uint64_t)dCon->dsmPool, m->destAddr,
            1024, dCon->dsmLKey,
            remoteInfo[m->nodeID].appRKey[m->appID], 11, true, 0);

  dirSendDataCounter++;
}

void Directory::sendAck2AppByPassSwitch(const RawMessage *from_message,
                                        RawMessageType type, uint64_t value) {
  dirSendControlCounter++;

  RawMessage *m = (RawMessage *)dCon->message->getSendPool();
  memcpy(m, from_message, sizeof(RawMessage));

  m->qpn = dCon->message->getQPN();
  m->mtype = type;
  m->is_app_req = 0;
  m->destAddr = value;

  dCon->sendMessage(m);
}
