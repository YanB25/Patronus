
#include "DSM.h"
#include "Directory.h"
#include "HugePageAlloc.h"

#include "DSMKeeper.h"

#include <algorithm>

thread_local int DSM::thread_id = -1;
thread_local ThreadConnection *DSM::iCon = nullptr;

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

  memcpy(mac, getMac(), 6);
  baseAddr = (uint64_t)hugePageAlloc(conf.dsmSize * define::GB);

  Debug::notifyInfo("shared memory size: %dGB", conf.dsmSize);
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
}

void DSM::initRDMAConnection() {

  Debug::notifyInfo("Machine NR: %d", conf.machineNR);

  remoteInfo = new RemoteConnection[conf.machineNR];
  // cache.remoteInfo = remoteInfo;

  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    thCon[i] = new ThreadConnection(i, (void *)cache.data, cache.size,
                                    conf.machineNR, remoteInfo, mac);
  }

  for (int i = 0; i < NR_DIRECTORY; ++i) {
    dirCon[i] =
        new DirectoryConnection(i, (void *)baseAddr, conf.dsmSize * define::GB,
                                conf.machineNR, remoteInfo, mac);
  }

  keeper = new DSMKeeper(thCon, dirCon, remoteInfo, conf.machineNR);

  myNodeID = keeper->getMyNodeID();
}