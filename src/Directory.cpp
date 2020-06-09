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
    dsm_start.offset = per_directory_dsm_size * dirID;
    chunckAlloc = new GlobalAllocator(dsm_start, per_directory_dsm_size);
  }

  dirTh = new std::thread(&Directory::dirThread, this);
}

Directory::~Directory() { delete chunckAlloc; }

void Directory::dirThread() {

  bindCore(23 - dirID);
  Debug::notifyInfo("dir %d launch!\n", dirID);

  while (true) {
    struct ibv_wc wc;
    pollWithCQ(dCon->cq, 1, &wc);
 printf("DD\n");
    switch (int(wc.opcode)) {
    case IBV_WC_RECV: // control message
    {
     
      auto *m = (RawMessage *)dCon->message->getMessage();

      process_message(m);

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

void Directory::process_message(const RawMessage *m) {
   
  printf("BB\n");
  RawMessage *send = nullptr;
  switch (m->type) {
  case RpcType::MALLOC: {

    send = (RawMessage *)dCon->message->getSendPool();

    send->addr = chunckAlloc->alloc_chunck();
    break;
  }

  default:
    assert(false);
  }

  if (send) {
    dCon->sendMessage2App(send, m->node_id, m->app_id);
  }
}

// void Directory::sendData2App(const RawMessage *m) {
//   rdmaWrite(dCon->data2app[m->appID][m->nodeID], (uint64_t)dCon->dsmPool,
//             m->destAddr, 1024, dCon->dsmLKey,
//             remoteInfo[m->nodeID].appRKey[m->appID], 11, true, 0);
// }
