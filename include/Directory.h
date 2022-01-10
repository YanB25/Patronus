#pragma once
#ifndef __DIRECTORY_H__
#define __DIRECTORY_H__

#include <thread>
#include <unordered_map>

#include "Common.h"
#include "Connection.h"
#include "GlobalAllocator.h"

class Directory
{
public:
    Directory(DirectoryConnection &dCon,
              const std::vector<RemoteConnection> &remoteInfo,
              uint32_t machineNR,
              uint16_t dirID,
              uint16_t nodeID);
    static std::shared_ptr<Directory> newInstance(
        DirectoryConnection &dCon,
        const std::vector<RemoteConnection> &remoteInfo,
        uint32_t machineNR,
        uint64_t dirID,
        uint16_t nodeID

    );

    ~Directory();

private:
    DirectoryConnection &dCon;
    const std::vector<RemoteConnection> remoteInfo;

    uint32_t machineNR;
    uint16_t dirID;
    uint16_t nodeID;

    std::thread dirTh;

    std::unique_ptr<GlobalAllocator> chunckAlloc;

    void dirThread();

    void sendData2App(const RawMessage *m);

    void process_message(const RawMessage *m);
};

#endif /* __DIRECTORY_H__ */
