#ifndef __LINEAR_KEEPER__H__
#define __LINEAR_KEEPER__H__

#include <vector>
#include <unordered_map>

#include "Keeper.h"
#include <glog/logging.h>

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
    ExPerThread appTh[MAX_APP_THREAD];
    /***
     * DirectoryConnection exchange data used by ThreadConnection
     */
    ExPerThread dirTh[NR_DIRECTORY];

    /**
     * @brief Unreliable Datagram QPN used in raw message transmission.
     * app QPN is used by @see DirectoryConnection
     */
    uint32_t appUdQpn[MAX_APP_THREAD];
    /**
     * @brief Unreliable Datagram QPN used in raw message transmission.
     * dir QPN is used by @see ThreadConnection
     */
    uint32_t dirUdQpn[NR_DIRECTORY];

    /**
     * @brief Reliable Connection QPN used for each local ThreadConnection to
     * remote DirectoryConnetion
     */
    uint32_t appRcQpn2dir[MAX_APP_THREAD][NR_DIRECTORY];

    /**
     * @brief Reliable Connection QPN used for each local DirectoryConnection to
     * remote ThreadConnetion
     */
    uint32_t dirRcQpn2app[NR_DIRECTORY][MAX_APP_THREAD];

} __attribute__((packed));

class DSMKeeper : public Keeper
{
public:
    static std::unique_ptr<DSMKeeper> newInstance(
        std::vector<ThreadConnection> &thCon,
        std::vector<DirectoryConnection> &dirCon,
        std::vector<RemoteConnection> &remoteCon,
        uint32_t maxServer = 12)
    {
        return future::make_unique<DSMKeeper>(
            thCon, dirCon, remoteCon, maxServer);
    }

    DSMKeeper(std::vector<ThreadConnection> &thCon,
              std::vector<DirectoryConnection> &dirCon,
              std::vector<RemoteConnection> &remoteCon,
              uint32_t maxServer = 12)
        : Keeper(maxServer), thCon(thCon), dirCon(dirCon), remoteCon(remoteCon)
    {
        DLOG(INFO) << "DSMKeeper::initLocalMeta()";
        initLocalMeta();

        DLOG(INFO) << "DSMKeeper::connectMemcached";
        if (!connectMemcached())
        {
            LOG(FATAL) << "DSMKeeper:: unable to connect to memcached";
            return;
        }
        serverEnter();

        serverConnect();
        connectMySelf();

        initRouteRule();
    }

    virtual ~DSMKeeper();
    void barrier(const std::string &barrierKey);
    uint64_t sum(const std::string &sum_key, uint64_t value);

    const ExchangeMeta& getExchangeMeta(uint16_t remoteID) const;

private:
    static const char *OK;
    static const char *ServerPrefix;

    std::vector<ThreadConnection> &thCon;
    std::vector<DirectoryConnection> &dirCon;
    std::vector<RemoteConnection> &remoteCon;

    std::unordered_map<uint16_t, ExchangeMeta> snapshot_exchange_meta_;

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
    void snapshotExchangeMeta(uint16_t remoteID, const ExchangeMeta& meta);

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
