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

  // bindCore(12 - dirID);
  Debug::notifyInfo("dir %d launch!\n", dirID);

  while (true) {
    struct ibv_wc wc;
    pollWithCQ(dCon->cq, 1, &wc);

    switch (int(wc.opcode)) {
    case IBV_WC_RECV: // control message
    {
      auto *m = (RawMessage *)dCon->message->getMessage();
      printf("recv %d, [%d, %d]\n", m->num, m->node_id, m->app_id);

  
      RawMessage *send = (RawMessage *)dCon->message->getSendPool();
      send->num = 666;
      dCon->sendMessage2App(send, m->node_id, m->app_id);

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

// void Directory::sendData2App(const RawMessage *m) {
//   rdmaWrite(dCon->data2app[m->appID][m->nodeID], (uint64_t)dCon->dsmPool,
//             m->destAddr, 1024, dCon->dsmLKey,
//             remoteInfo[m->nodeID].appRKey[m->appID], 11, true, 0);
// }
