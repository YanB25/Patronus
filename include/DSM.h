#ifndef __DSM_H__
#define __DSM_H__

#include <atomic>

#include "Cache.h"
#include "Config.h"
#include "Connection.h"
#include "DSMKeeper.h"
#include "GlobalAddress.h"

class DSMKeeper;
class Directory;

class DSM {

public:
  void registerThread();
  static DSM *getInstance(const DSMConfig &conf);

  uint16_t getMyNodeID() { return myNodeID; }
  uint16_t getMyThreadID() {
    //  return Cache::iId;
  }

  size_t Put(uint64_t key, const void *value, size_t count) {

    std::string k = std::string("gam-") + std::to_string(key);
    keeper->memSet(k.c_str(), k.size(), (char *)value, count);
    return count;
  }

  size_t Get(uint64_t key, void *value) {

    std::string k = std::string("gam-") + std::to_string(key);
    size_t size;
    char *ret = keeper->memGet(k.c_str(), k.size(), &size);
    memcpy(value, ret, size);

    return size;
  }

private:
  DSM(const DSMConfig &conf);
  ~DSM();

  void initRDMAConnection();

  DSMConfig conf;
  Cache cache;

  static thread_local int thread_id;
  static thread_local ThreadConnection *iCon;

public:
  uint64_t baseAddr;
  uint32_t myNodeID;
  uint8_t mac[6];

  ThreadConnection *thCon[MAX_APP_THREAD];
  DirectoryConnection *dirCon[NR_DIRECTORY];

  RemoteConnection *remoteInfo;

  DSMKeeper *keeper;

  std::atomic_int appID;

  Directory *dirAgent[NR_DIRECTORY];

  void barrier(const std::string &ss) { keeper->barrier(ss); }
};

#endif /* __DSM_H__ */
