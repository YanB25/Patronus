#ifndef __DIRECTORY_H__
#define __DIRECTORY_H__

#include <thread>

#include <unordered_map>

#include "Common.h"

#include "Connection.h"
#include "GlobalAllocator.h"


class Directory {
public:
  Directory(DirectoryConnection *dCon, RemoteConnection *remoteInfo,
            uint32_t machineNR, uint16_t dirID, uint16_t nodeID);

  ~Directory();

private:
  DirectoryConnection *dCon;
  RemoteConnection *remoteInfo;

  uint32_t machineNR;
  uint16_t dirID;
  uint16_t nodeID;

  std::thread *dirTh;

  GlobalAllocator *chunckAlloc;

  void dirThread();

  void sendData2App(const RawMessage *m);
  void sendAck2AppByPassSwitch(const RawMessage *, RawMessageType type,
                               uint64_t value = 0);
};

#endif /* __DIRECTORY_H__ */
