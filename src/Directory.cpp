#include "Directory.h"

#include <gperftools/profiler.h>

#include "Common.h"
#include "Connection.h"

GlobalAddress g_root_ptr = GlobalAddress::Null();
int g_root_level = -1;
bool enable_cache;

std::shared_ptr<Directory> Directory::newInstance(
    DirectoryConnection &dCon,
    const std::vector<RemoteConnection> &remoteInfo,
    uint32_t machineNR,
    uint64_t dirID,
    uint16_t nodeID)
{
    return std::make_shared<Directory>(
        dCon, remoteInfo, machineNR, dirID, nodeID);
}

Directory::Directory(DirectoryConnection &dCon,
                     const std::vector<RemoteConnection> &remoteInfo,
                     uint32_t machineNR,
                     uint16_t dirID,
                     uint16_t nodeID)
    : dCon(dCon),
      remoteInfo(remoteInfo),
      machineNR(machineNR),
      dirID(dirID),
      nodeID(nodeID)
{
    {  // chunck alloctor
        GlobalAddress dsm_start;
        uint64_t per_directory_dsm_size = dCon.dsmSize / NR_DIRECTORY;
        dsm_start.nodeID = nodeID;
        dsm_start.offset = per_directory_dsm_size * dirID;
        chunckAlloc =
            GlobalAllocator::newInstance(dsm_start, per_directory_dsm_size);
    }

    dirTh = std::thread(&Directory::dirThread, this);
}

Directory::~Directory()
{
}

void Directory::dirThread()
{
    CHECK(false) << "TODO:";
    bindCore(23 - dirID);
    LOG(INFO) << "dir " << dirID << " launch!\n";

    while (true)
    {
        struct ibv_wc wc;
        pollWithCQ(dCon.cq, 1, &wc);

        switch (int(wc.opcode))
        {
        case IBV_WC_RECV:  // control message
        {
            auto *m = (RawMessage *) dCon.message->getMessage();

            process_message(m);

            break;
        }
        case IBV_WC_RDMA_WRITE:
        {
            break;
        }
        case IBV_WC_RECV_RDMA_WITH_IMM:
        {
            break;
        }
        default:
            assert(false);
        }
    }
}

void Directory::process_message(const RawMessage *m)
{
    RawMessage *send = nullptr;
    switch (m->type)
    {
    case RpcType::MALLOC:
    {
        send = (RawMessage *) dCon.message->getSendPool();

        send->addr = chunckAlloc->alloc_chunck();
        break;
    }

    case RpcType::NEW_ROOT:
    {
        if (g_root_level < m->level)
        {
            g_root_ptr = m->addr;
            g_root_level = m->level;
            if (g_root_level >= 4)
            {
                enable_cache = true;
            }
        }

        break;
    }

    default:
        assert(false);
    }

    if (send)
    {
        dCon.sendMessage2App(send, m->node_id, m->app_id);
    }
}

// void Directory::sendData2App(const RawMessage *m) {
//   rdmaWrite(dCon->QPs[m->appID][m->nodeID], (uint64_t)dCon->dsmPool,
//             m->destAddr, 1024, dCon->dsmLKey,
//             remoteInfo[m->nodeID].appRKey[m->appID], 11, true, 0);
// }
