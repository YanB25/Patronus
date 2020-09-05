#ifndef __LINEAR_KEEPER__H__
#define __LINEAR_KEEPER__H__

#include <vector>

#include "Keeper.h"

struct ThreadConnection;
struct DirectoryConnection;
struct CacheAgentConnection;
struct RemoteConnection;

struct ExPerThread
{
    uint16_t lid;
    uint8_t gid[16];

    uint32_t rKey;

    uint32_t lock_rkey;  // for directory on-chip memory
} __attribute__((packed));

struct ExchangeMeta
{
    uint64_t dsmBase;
    uint64_t cacheBase;
    uint64_t lockBase;

    ExPerThread appTh[MAX_APP_THREAD];
    ExPerThread dirTh[NR_DIRECTORY];

    uint32_t appUdQpn[MAX_APP_THREAD];
    uint32_t dirUdQpn[NR_DIRECTORY];

    uint32_t appRcQpn2dir[MAX_APP_THREAD][NR_DIRECTORY];

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
        Debug::notifyDebug("DSMKeeper::initLocalMeta()");
        initLocalMeta();

        Debug::notifyDebug("DSMKeeper::connectMemcached");
        if (!connectMemcached())
        {
            Debug::notifyPanic("DSMKeeper:: unable to connect to memcached");
            return;
        }
        serverEnter();

        serverConnect();
        connectMySelf();

        initRouteRule();
    }

    ~DSMKeeper()
    {
        disconnectMemcached();
    }
    void barrier(const std::string &barrierKey);
    uint64_t sum(const std::string &sum_key, uint64_t value);

private:
    static const char *OK;
    static const char *ServerPrefix;

    std::vector<ThreadConnection> &thCon;
    std::vector<DirectoryConnection> &dirCon;
    std::vector<RemoteConnection> &remoteCon;

    ExchangeMeta localMeta;

    std::vector<std::string> serverList;

    std::string setKey(uint16_t remoteID)
    {
        return std::to_string(getMyNodeID()) + "-" + std::to_string(remoteID);
    }

    std::string getKey(uint16_t remoteID)
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
    void setDataToRemote(uint16_t remoteID);

    /**
     * @brief init and setup each QPs in @see ThreadConnection and @see
     * DirectoryConnection and @see RemoteConnection according to remote meta
     * data.
     * @param remoteID the id of the remote machine
     * @param remoteMeta the remote meta data to refer to
     */
    void setDataFromRemote(uint16_t remoteID, ExchangeMeta &remoteMeta);

protected:
    /**
     * @brief connect to the actual node.
     *
     * This function will call @see setDataToRemote and @see setDataFromRemote.
     *
     * @param remoteID the node to connect
     */
    virtual bool connectNode(uint16_t remoteID) override;
};

#endif
