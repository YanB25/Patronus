#include "DSMKeeper.h"

#include "Connection.h"

const char *DSMKeeper::OK = "OK";
const char *DSMKeeper::ServerPrefix = "SPre";

void DSMKeeper::initLocalMeta()
{
    exchangeMeta.dsmBase = (uint64_t) dirCon[0].dsmPool;
    exchangeMeta.dmBase = (uint64_t) dirCon[0].dmPool;

    // per thread APP
    for (size_t i = 0; i < thCon.size(); ++i)
    {
        exchangeMeta.appTh[i].lid = thCon[i].ctx.lid;
        exchangeMeta.appTh[i].rKey = thCon[i].cacheMR->rkey;
        memcpy((char *) exchangeMeta.appTh[i].gid,
               (char *) (&thCon[i].ctx.gid),
               16 * sizeof(uint8_t));

        exchangeMeta.appUdQpn[i] = thCon[i].message->getQPN();
    }

    // per thread DIR
    for (size_t i = 0; i < dirCon.size(); ++i)
    {
        exchangeMeta.dirTh[i].lid = dirCon[i].ctx.lid;
        exchangeMeta.dirTh[i].rKey = dirCon[i].dsmMR->rkey;
        // only enable DM for the first DirCon.
        if (i == 0)
        {
            exchangeMeta.dirTh[i].dm_rkey = dirCon[i].lockMR->rkey;
        }
        else
        {
            exchangeMeta.dirTh[i].dm_rkey = 0;
        }
        memcpy((char *) exchangeMeta.dirTh[i].gid,
               (char *) (&dirCon[i].ctx.gid),
               16 * sizeof(uint8_t));

        exchangeMeta.dirUdQpn[i] = dirCon[i].message->getQPN();
    }
}

bool DSMKeeper::connectNode(uint16_t remoteID)
{
    // press data into local cache
    setExchangeMeta(remoteID);

    // write personal exchange data to memcached
    std::string setK = connMetaPersonalKey(remoteID);
    memSet(setK.c_str(),
           setK.size(),
           (char *) (&exchangeMeta),
           sizeof(exchangeMeta));

    // read peer exchange data from memcached
    std::string getK = connMetaRemoteKey(remoteID);
    ExchangeMeta *remoteMeta =
        (ExchangeMeta *) memGet(getK.c_str(), getK.size());

    // apply the queried ExchangeMeta to update the QPs
    applyExchangeMeta(remoteID, *remoteMeta);

    free(remoteMeta);
    return true;
}

void DSMKeeper::snapshotExchangeMeta(uint16_t remoteID,
                                     const ExchangeMeta &meta)
{
    snapshot_exchange_meta_[remoteID] = meta;
    // should be bit-wise equal.
    DCHECK(memcmp(&snapshot_exchange_meta_[remoteID],
                  &meta,
                  sizeof(ExchangeMeta)) == 0);
}

const ExchangeMeta &DSMKeeper::getExchangeMeta(uint16_t remoteID) const
{
    auto it = snapshot_exchange_meta_.find(remoteID);
    if (it == snapshot_exchange_meta_.end())
    {
        error("failed to fetch exchange meta data for server %u", remoteID);
        throw std::runtime_error("Exchange metadata not found.");
    }
    return it->second;
}

void DSMKeeper::setExchangeMeta(uint16_t remoteID)
{
    for (int i = 0; i < NR_DIRECTORY; ++i)
    {
        const auto &c = dirCon[i];

        for (int k = 0; k < MAX_APP_THREAD; ++k)
        {
            exchangeMeta.dirRcQpn2app[i][k] = c.QPs[k][remoteID]->qp_num;
        }
    }

    for (int i = 0; i < MAX_APP_THREAD; ++i)
    {
        const auto &c = thCon[i];
        for (int k = 0; k < NR_DIRECTORY; ++k)
        {
            //  = thCon[i].QPs[k][remoteID]->qp_num;
            exchangeMeta.appRcQpn2dir[i][k] = c.QPs[k][remoteID]->qp_num;
        }
    }
}

void DSMKeeper::applyExchangeMeta(uint16_t remoteID, const ExchangeMeta &exMeta)
{
    // I believe the exMeta is correct here.
    // so do a snapshot for later retrieval
    snapshotExchangeMeta(remoteID, exMeta);

    // init directory qp
    for (int i = 0; i < NR_DIRECTORY; ++i)
    {
        auto &c = dirCon[i];

        for (int k = 0; k < MAX_APP_THREAD; ++k)
        {
            auto &qp = c.QPs[k][remoteID];

            assert(qp->qp_type == IBV_QPT_RC);
            modifyQPtoInit(qp, &c.ctx);
            modifyQPtoRTR(qp,
                          exMeta.appRcQpn2dir[k][i],
                          exMeta.appTh[k].lid,
                          exMeta.appTh[k].gid,
                          &c.ctx);
            modifyQPtoRTS(qp);
        }
    }

    // init application qp
    for (size_t i = 0; i < thCon.size(); ++i)
    {
        auto &c = thCon[i];
        for (int k = 0; k < NR_DIRECTORY; ++k)
        {
            auto &qp = c.QPs[k][remoteID];

            CHECK(qp->qp_type == IBV_QPT_RC);
            modifyQPtoInit(qp, &c.ctx);
            modifyQPtoRTR(qp,
                          exMeta.dirRcQpn2app[k][i],
                          exMeta.dirTh[k].lid,
                          exMeta.dirTh[k].gid,
                          &c.ctx);
            modifyQPtoRTS(qp);
        }
    }

    // init remote connections
    auto &remote = remoteCon[remoteID];
    remote.dsmBase = exMeta.dsmBase;
    dinfo("remote %p set dsmBase to %p", &remote, (char *) remote.dsmBase);
    // remote.cacheBase = exMeta.cacheBase;
    remote.dmBase = exMeta.dmBase;

    for (int i = 0; i < NR_DIRECTORY; ++i)
    {
        remote.dsmRKey[i] = exMeta.dirTh[i].rKey;
        remote.dmRKey[i] = exMeta.dirTh[i].dm_rkey;
        remote.dirMessageQPN[i] = exMeta.dirUdQpn[i];

        for (int k = 0; k < MAX_APP_THREAD; ++k)
        {
            struct ibv_ah_attr ahAttr;
            fillAhAttr(&ahAttr,
                       exMeta.dirTh[i].lid,
                       exMeta.dirTh[i].gid,
                       &thCon[k].ctx);
            remote.appToDirAh[k][i] = ibv_create_ah(thCon[k].ctx.pd, &ahAttr);

            CHECK(remote.appToDirAh[k][i]);
        }
    }

    for (int i = 0; i < MAX_APP_THREAD; ++i)
    {
        remote.appRKey[i] = exMeta.appTh[i].rKey;
        remote.appMessageQPN[i] = exMeta.appUdQpn[i];

        for (int k = 0; k < NR_DIRECTORY; ++k)
        {
            struct ibv_ah_attr ahAttr;
            fillAhAttr(&ahAttr,
                       exMeta.appTh[i].lid,
                       exMeta.appTh[i].gid,
                       &dirCon[k].ctx);
            remote.dirToAppAh[k][i] = ibv_create_ah(dirCon[k].ctx.pd, &ahAttr);

            assert(remote.dirToAppAh[k][i]);
        }
    }
}

void DSMKeeper::connectMySelf()
{
    setExchangeMeta(getMyNodeID());
    applyExchangeMeta(getMyNodeID(), exchangeMeta);
}

void DSMKeeper::initRouteRule()
{
    std::string k =
        std::string(ServerPrefix) + std::to_string(this->getMyNodeID());
    memSet(k.c_str(), k.size(), getMyIP().c_str(), getMyIP().size());
}

void DSMKeeper::barrier(const std::string &barrierKey)
{
    std::string key = std::string("barrier-") + barrierKey;
    if (this->getMyNodeID() == 0)
    {
        memSet(key.c_str(), key.size(), "0", 1);
    }
    memFetchAndAdd(key.c_str(), key.size());
    while (true)
    {
        auto *ret = memGet(key.c_str(), key.size());
        uint64_t v = std::stoull(ret);
        free(ret);
        if (v == this->getServerNR())
        {
            return;
        }
    }
}

uint64_t DSMKeeper::sum(const std::string &sum_key, uint64_t value)
{
    std::string key_prefix = std::string("sum-") + sum_key;

    std::string key = key_prefix + std::to_string(this->getMyNodeID());
    memSet(key.c_str(), key.size(), (char *) &value, sizeof(value));

    uint64_t ret = 0;
    for (int i = 0; i < this->getServerNR(); ++i)
    {
        key = key_prefix + std::to_string(i);
        auto buf_ret = memGet(key.c_str(), key.size());
        ret += *(uint64_t *) buf_ret;
        free(buf_ret);
    }

    return ret;
}

DSMKeeper::~DSMKeeper()
{
    disconnectMemcached();
    for (auto &rc : remoteCon)
    {
        rc.destroy();
    }
}