
#include "DSM.h"
#include "Directory.h"
#include "HugePageAlloc.h"

#include "DSMKeeper.h"

#include <algorithm>

thread_local int DSM::thread_id = -1;
thread_local ThreadConnection *DSM::iCon = nullptr;
thread_local char *DSM::rdma_buffer = nullptr;

DSM *DSM::getInstance(const DSMConfig &conf) {
  static DSM *dsm = nullptr;
  static WRLock lock;

  lock.wLock();
  if (!dsm) {
    dsm = new DSM(conf);
  } else {
  }
  lock.wUnlock();

  return dsm;
}

DSM::DSM(const DSMConfig &conf)
    : conf(conf), appID(0), cache(conf.cacheConfig) {

  baseAddr = (uint64_t)hugePageAlloc(conf.dsmSize * define::GB);

  Debug::notifyInfo("shared memory size: %dGB, 0x%lx", conf.dsmSize, baseAddr);
  Debug::notifyInfo("cache size: %dGB", conf.cacheConfig.cacheSize);

  // warmup
  // memset((char *)baseAddr, 0, conf.dsmSize * define::GB);
  for (uint64_t i = baseAddr; i < baseAddr + conf.dsmSize * define::GB;
       i += 2 * define::MB) {
    *(char *)i = 0;
  }

  initRDMAConnection();

  for (int i = 0; i < NR_DIRECTORY; ++i) {
    dirAgent[i] =
        new Directory(dirCon[i], remoteInfo, conf.machineNR, i, myNodeID);
  }

  keeper->barrier("DSM-init");
}

DSM::~DSM() {}

void DSM::registerThread() {
  thread_id = appID.fetch_add(1);
  iCon = thCon[thread_id];

  iCon->message->initRecv();
  iCon->message->initSend();
  rdma_buffer = (char *)cache.data + thread_id * 1 * define::MB;
}

void DSM::initRDMAConnection() {

  Debug::notifyInfo("Machine NR: %d", conf.machineNR);

  remoteInfo = new RemoteConnection[conf.machineNR];

  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    thCon[i] =
        new ThreadConnection(i, (void *)cache.data, cache.size * define::GB,
                             conf.machineNR, remoteInfo);
  }

  for (int i = 0; i < NR_DIRECTORY; ++i) {
    dirCon[i] =
        new DirectoryConnection(i, (void *)baseAddr, conf.dsmSize * define::GB,
                                conf.machineNR, remoteInfo);
  }

  keeper = new DSMKeeper(thCon, dirCon, remoteInfo, conf.machineNR);

  myNodeID = keeper->getMyNodeID();
}

void DSM::read(char *buffer, GlobalAddress gaddr, size_t size) {
  rdmaRead(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
           remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, size,
           iCon->cacheLKey, remoteInfo[gaddr.nodeID].dsmRKey[0]);
}

void DSM::read_sync(char *buffer, GlobalAddress gaddr, size_t size) {
  read(buffer, gaddr, size);

  ibv_wc wc;
  pollWithCQ(iCon->cq, 1, &wc);
}

void DSM::write(const char *buffer, GlobalAddress gaddr, size_t size) {

  rdmaWrite(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
            remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, size,
            iCon->cacheLKey, remoteInfo[gaddr.nodeID].dsmRKey[0]);
}

void DSM::write_sync(const char *buffer, GlobalAddress gaddr, size_t size) {
  write(buffer, gaddr, size);

  ibv_wc wc;
  pollWithCQ(iCon->cq, 1, &wc);
}

void DSM::cas(GlobalAddress gaddr, uint64_t equal, uint64_t val,
              uint64_t *rdma_buffer) {
  rdmaCompareAndSwap(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                     remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, equal,
                     val, iCon->cacheLKey, remoteInfo[gaddr.nodeID].dsmRKey[0]);
}

bool DSM::cas_sync(GlobalAddress gaddr, uint64_t equal, uint64_t val,
uint64_t *rdma_buffer) {
  cas(gaddr, equal, val, rdma_buffer);
  ibv_wc wc;
  pollWithCQ(iCon->cq, 1, &wc);

  return equal == *rdma_buffer;
}

void DSM::cas_mask(GlobalAddress gaddr, uint64_t equal, uint64_t val,
              uint64_t *rdma_buffer, uint64_t mask) {
  rdmaCompareAndSwapMask(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                     remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, equal,
                     val, iCon->cacheLKey, remoteInfo[gaddr.nodeID].dsmRKey[0],
                     mask);
}

bool DSM::cas_mask_sync(GlobalAddress gaddr, uint64_t equal, uint64_t val,
uint64_t *rdma_buffer, uint64_t mask) {
  cas_mask(gaddr, equal, val, rdma_buffer, mask);
  ibv_wc wc;
  pollWithCQ(iCon->cq, 1, &wc);

  return (equal & mask) == (*rdma_buffer & mask);
}