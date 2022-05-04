#pragma once
#ifndef __LINEAR_KEEPER__H__
#define __LINEAR_KEEPER__H__

#include <glog/logging.h>

#include <unordered_map>
#include <vector>

#include "Keeper.h"
#include "Timer.h"
#include "umsg/UnreliableConnection.h"

struct ThreadConnection;
struct DirectoryConnection;
struct CacheAgentConnection;
struct RemoteConnection;

struct ExPerThread
{
    uint16_t lid;
    uint8_t gid[16];

    uint32_t rKey;

    uint32_t dm_rkey;  // for directory on-chip memory
} __attribute__((packed));

struct ExUnreliable
{
    uint16_t lid;
    uint8_t gid[16];
    uint32_t qpn;
} __attribute__((packed));
/**
 * @brief an exchange data used in building connection for each `pair` of
 * machines in the system. Keep the struct POD to be memcpy-able
 */
struct ExchangeMeta
{
    uint64_t dsmBase;
    // uint64_t cacheBase;
    uint64_t dmBase;

    /**
     * ThreadConnection exchange data used by DirectoryConnection
     */
    ExPerThread appTh[kMaxAppThread];
    /***
     * DirectoryConnection exchange data used by ThreadConnection
     */
    ExPerThread dirTh[NR_DIRECTORY];

    /**
     * @brief Unreliable Datagram QPN used in raw message transmission.
     * app QPN is used by @see DirectoryConnection
     */
    uint32_t appUdQpn[kMaxAppThread];
    /**
     * @brief Unreliable Datagram QPN used in raw message transmission.
     * dir QPN is used by @see ThreadConnection
     */
    uint32_t dirUdQpn[NR_DIRECTORY];

    /**
     * @brief Reliable Connection QPN used for each local ThreadConnection to
     * remote DirectoryConnetion
     */
    uint32_t appRcQpn2dir[kMaxAppThread][NR_DIRECTORY];

    /**
     * @brief Reliable Connection QPN used for each local DirectoryConnection to
     * remote ThreadConnetion
     */
    uint32_t dirRcQpn2app[NR_DIRECTORY][kMaxAppThread];

    ExUnreliable umsgs[kMaxAppThread];
} __attribute__((packed));

class DSMKeeper : public Keeper
{
public:
    static std::unique_ptr<DSMKeeper> newInstance(
        std::vector<std::unique_ptr<ThreadConnection>> &thCon,
        std::vector<std::unique_ptr<DirectoryConnection>> &dirCon,
        UnreliableConnection<kMaxAppThread> &umsg,
        std::vector<RemoteConnection> &remoteCon,
        uint32_t maxServer = 12)
    {
        return future::make_unique<DSMKeeper>(
            thCon, dirCon, umsg, remoteCon, maxServer);
    }

    DSMKeeper(std::vector<std::unique_ptr<ThreadConnection>> &thCon,
              std::vector<std::unique_ptr<DirectoryConnection>> &dirCon,
              UnreliableConnection<kMaxAppThread> &umsg,
              std::vector<RemoteConnection> &remoteCon,
              uint32_t maxServer = MAX_MACHINE);

    virtual ~DSMKeeper();
    template <typename T>
    void barrier(const std::string &barrierKey, const T &sleep_time)
    {
        std::string key = std::string("__barrier:") + barrierKey;
        auto nid = getMyNodeID();
        if (nid == 0)
        {
            memSet(key.c_str(), key.size(), "0", 1, sleep_time);
        }
        memFetchAndAdd(key.c_str(), key.size(), sleep_time);
        while (true)
        {
            auto *ret = memGet(
                key.c_str(), key.size(), nullptr, sleep_time * (nid + 1));
            uint64_t v = std::stoull(ret);
            free(ret);
            if (v == getServerNR())
            {
                return;
            }
        }
    }
    uint64_t sum(const std::string &sum_key, uint64_t value);
    void connectDir(DirectoryConnection &,
                    int remoteID,
                    int appID,
                    const ExchangeMeta &exMeta);
    void connectThread(ThreadConnection &,
                       int remoteID,
                       int dirID,
                       const ExchangeMeta &exMeta);

    void connectUnreliableMsg(UnreliableConnection<kMaxAppThread> &umsg,
                              int remoteID);

    void updateRemoteConnectionForDir(RemoteConnection &,
                                      const ExchangeMeta &exMeta,
                                      size_t dirID);

    const ExchangeMeta &getExchangeMeta(uint16_t remoteID) const;
    ExchangeMeta updateDirMetadata(const DirectoryConnection &dir,
                                   size_t remoteID);

private:
    static const char *OK;
    static const char *ServerPrefix;

    std::vector<std::unique_ptr<ThreadConnection>> &thCon;
    std::vector<std::unique_ptr<DirectoryConnection>> &dirCon;
    UnreliableConnection<kMaxAppThread> &umsg;
    std::vector<RemoteConnection> &remoteCon;

    // remoteID => remoteMetaData
    std::unordered_map<uint16_t, ExchangeMeta> snapshot_remote_meta_;

    ExchangeMeta exchangeMeta;

    std::vector<std::string> serverList;

    std::string connMetaPersonalKey(uint16_t remoteID)
    {
        return std::to_string(getMyNodeID()) + "-" + std::to_string(remoteID);
    }

    std::string connMetaRemoteKey(uint16_t remoteID)
    {
        return std::to_string(remoteID) + "-" + std::to_string(getMyNodeID());
    }

    void initLocalMeta();
    /**
     * @brief similar to @see connectNode, but this function init connections of
     * itself.
     */
    void connectMySelf();
    void initRouteRule();

    /**
     * @brief set remote machine's qp_num to local's meta data cache
     * @param remoteID the remote machine to set with
     */
    void setExchangeMeta(uint16_t remoteID);
    void snapshotConnectRemoteMeta(uint16_t remoteID, const ExchangeMeta &meta);

    /**
     * @brief This function does the real and dirty jobs to modify the QP state.
     * @see ThreadConnection and @see DirectoryConnection and @see
     * RemoteConnection according to remote meta data.
     * @param remoteID the id of the remote machine
     * @param remoteMeta the remote meta data to refer to
     */
    void applyExchangeMeta(uint16_t remoteID, const ExchangeMeta &remoteMeta);

protected:
    /**
     * @brief connect to the actual node.
     *
     * This function will call @see setExchangeMeta and @see applyExchangeMeta.
     *
     * @param remoteID the node to connect
     */
    virtual bool connectNode(uint16_t remoteID) override;
};

#endif
