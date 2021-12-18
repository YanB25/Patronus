#include "DSMKeeper.h"

#include "Connection.h"

const char *DSMKeeper::OK = "OK";
const char *DSMKeeper::ServerPrefix = "SPre";

DSMKeeper::DSMKeeper(std::vector<std::unique_ptr<ThreadConnection>> &thCon,
                     std::vector<std::unique_ptr<DirectoryConnection>> &dirCon,
                     std::vector<RemoteConnection> &remoteCon,
                     ReliableConnection &reliableCon,
                     uint32_t maxServer)
    : Keeper(maxServer),
      thCon(thCon),
      dirCon(dirCon),
      reliableCon(reliableCon),
      remoteCon(remoteCon)
{
    ContTimer<config::kMonitorControlPath> timer("DSMKeeper::DSMKeeper(...)");

    DLOG(INFO) << "DSMKeeper::initLocalMeta()";
    initLocalMeta();
    timer.pin("initLocalMeta()");

    DLOG(INFO) << "DSMKeeper::connectMemcached";
    if (!connectMemcached())
    {
        LOG(FATAL) << "DSMKeeper:: unable to connect to memcached";
        return;
    }
    timer.pin("connectMemcached");
    serverEnter();
    timer.pin("serverEnter");

    serverConnect();
    connectMySelf();
    timer.pin("connect");

    initRouteRule();
    timer.pin("initRouteRule");

    reliableCon.recv_.init();
    timer.pin("init reliable recv");
    timer.report();
}

void DSMKeeper::initLocalMeta()
{
    exchangeMeta.dsmBase = (uint64_t) dirCon[0]->dsmPool;
    exchangeMeta.dmBase = (uint64_t) dirCon[0]->dmPool;

    // per thread APP
    for (size_t i = 0; i < thCon.size(); ++i)
    {
        exchangeMeta.appTh[i].lid = thCon[i]->ctx.lid;
        exchangeMeta.appTh[i].rKey = thCon[i]->cacheMR->rkey;
        memcpy((char *) exchangeMeta.appTh[i].gid,
               (char *) (&thCon[i]->ctx.gid),
               16 * sizeof(uint8_t));

        exchangeMeta.appUdQpn[i] = thCon[i]->message->getQPN();
    }

    // reliable send
    auto &send = reliableCon.send_;
    auto &send_ctx = send.context();
    exchangeMeta.rsend.lid = send_ctx.lid;
    exchangeMeta.rsend.rkey = send.rkey();
    memcpy((char *) exchangeMeta.rsend.gid,
           (char *) (&send_ctx.gid),
           sizeof(uint8_t) * 16);

    // per thread DIR
    for (size_t i = 0; i < dirCon.size(); ++i)
    {
        exchangeMeta.dirTh[i].lid = dirCon[i]->ctx.lid;
        exchangeMeta.dirTh[i].rKey = dirCon[i]->dsmMR->rkey;
        // only enable DM for the first DirCon.
        if (i == 0)
        {
            exchangeMeta.dirTh[i].dm_rkey = dirCon[i]->lockMR->rkey;
        }
        else
        {
            exchangeMeta.dirTh[i].dm_rkey = 0;
        }
        memcpy((char *) exchangeMeta.dirTh[i].gid,
               (char *) (&dirCon[i]->ctx.gid),
               16 * sizeof(uint8_t));

        exchangeMeta.dirUdQpn[i] = dirCon[i]->message->getQPN();
    }

    // reliable recv
    auto &recv = reliableCon.recv_;
    auto &recv_ctx = recv.context();
    exchangeMeta.rrecv.lid = recv_ctx.lid;
    exchangeMeta.rrecv.rkey = recv.rkey();
    memcpy((char *) exchangeMeta.rrecv.gid,
           (char *) (&recv_ctx.gid),
           sizeof(uint8_t) * 16);
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

ExchangeMeta DSMKeeper::updateDirMetadata(const DirectoryConnection &dir,
                                          size_t remoteID)
{
    ExchangeMeta meta = getExchangeMeta(remoteID);

    auto dirID = dir.dirID;

    meta.dsmBase = (uint64_t) dirCon[0]->dsmPool;
    meta.dmBase = (uint64_t) dirCon[0]->dmPool;

    meta.dirTh[dirID].lid = dir.ctx.lid;
    meta.dirTh[dirID].rKey = dir.dsmMR->rkey;
    if (dirID == 0)
    {
        meta.dirTh[dirID].dm_rkey = dir.lockMR->rkey;
    }
    else
    {
        meta.dirTh[dirID].dm_rkey = 0;
    }
    memcpy((char *) meta.dirTh[dirID].gid,
           (char *) &(dir.ctx.gid),
           16 * sizeof(uint8_t));
    meta.dirUdQpn[dirID] = dir.message->getQPN();

    for (int k = 0; k < MAX_APP_THREAD; ++k)
    {
        meta.dirRcQpn2app[dirID][k] = dir.QPs[k][remoteID]->qp_num;
    }

    return meta;
}

void DSMKeeper::snapshotConnectRemoteMeta(uint16_t remoteID,
                                          const ExchangeMeta &meta)
{
    snapshot_remote_meta_[remoteID] = meta;
    // should be bit-wise equal.
    DCHECK(memcmp(&snapshot_remote_meta_[remoteID],
                  &meta,
                  sizeof(ExchangeMeta)) == 0);
}

/**
 * @brief the exchange meta data used from connecting local to @remoteID
 *
 * @param remoteID
 * @return const ExchangeMeta&
 */
const ExchangeMeta &DSMKeeper::getExchangeMeta(uint16_t remoteID) const
{
    auto it = snapshot_remote_meta_.find(remoteID);
    if (it == snapshot_remote_meta_.end())
    {
        LOG(FATAL) << "failed to fetch exchange meta data for server "
                   << remoteID;
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
            exchangeMeta.dirRcQpn2app[i][k] = c->QPs[k][remoteID]->qp_num;
        }
    }
    exchangeMeta.rrecv.qpn = reliableCon.recv_.QPs()[remoteID]->qp_num;

    for (int i = 0; i < MAX_APP_THREAD; ++i)
    {
        const auto &c = thCon[i];
        for (int k = 0; k < NR_DIRECTORY; ++k)
        {
            //  = thCon[i].QPs[k][remoteID]->qp_num;
            exchangeMeta.appRcQpn2dir[i][k] = c->QPs[k][remoteID]->qp_num;
        }
    }
    exchangeMeta.rsend.qpn = reliableCon.send_.QPs()[remoteID]->qp_num;
}

void DSMKeeper::connectSend(ReliableSendMessageConnection &send,
                            int remoteID,
                            const ExchangeMeta &exMeta)
{
    auto &qp = send.QPs()[remoteID];
    auto &ctx = send.context();
    CHECK_EQ(qp->qp_type, IBV_QPT_RC);
    CHECK(modifyQPtoInit(qp, &ctx));
    CHECK(modifyQPtoRTR(
        qp, exMeta.rrecv.qpn, exMeta.rrecv.lid, exMeta.rrecv.gid, &ctx));
    CHECK(modifyQPtoRTS(qp));

    LOG(INFO) << "[debug] Send connect to remote " << remoteID
              << ", hash(rrecv): " << std::hex
              << djb2_digest((char *) &exMeta.rrecv, sizeof(exMeta.rrecv));
}

void DSMKeeper::connectThread(ThreadConnection &th,
                              int remoteID,
                              int dirID,
                              const ExchangeMeta &exMeta)
{
    auto &qp = th.QPs[dirID][remoteID];
    CHECK_EQ(qp->qp_type, IBV_QPT_RC);
    CHECK(modifyQPtoInit(qp, &th.ctx));
    CHECK(modifyQPtoRTR(qp,
                        exMeta.dirRcQpn2app[dirID][th.threadID],
                        exMeta.dirTh[dirID].lid,
                        exMeta.dirTh[dirID].gid,
                        &th.ctx));
    CHECK(modifyQPtoRTS(qp));
    VLOG(1) << "[keeper] (re)connection ThreadConnection[" << th.threadID
            << "]. for remoteID " << remoteID << ", DIR " << dirID
            << ". dirRcQpn2app: " << exMeta.dirRcQpn2app[dirID][th.threadID]
            << ", lid: " << exMeta.dirTh[dirID].lid
            << ", gid: " << exMeta.dirTh[dirID].gid;
}

void DSMKeeper::updateRemoteConnectionForDir(RemoteConnection &remote,
                                             const ExchangeMeta &meta,
                                             size_t dirID)
{
    remote.dsmBase = meta.dsmBase;
    remote.dmBase = meta.dmBase;

    remote.dsmRKey[dirID] = meta.dirTh[dirID].rKey;
    remote.dmRKey[dirID] = meta.dirTh[dirID].dm_rkey;
    remote.dirMessageQPN[dirID] = meta.dirUdQpn[dirID];

    for (int k = 0; k < MAX_APP_THREAD; ++k)
    {
        struct ibv_ah_attr ahAttr;
        fillAhAttr(&ahAttr,
                   meta.dirTh[dirID].lid,
                   meta.dirTh[dirID].gid,
                   &thCon[k]->ctx);
        PLOG_IF(ERROR, ibv_destroy_ah(remote.appToDirAh[k][dirID]))
            << "failed to destroy ah.";
        remote.appToDirAh[k][dirID] =
            CHECK_NOTNULL(ibv_create_ah(thCon[k]->ctx.pd, &ahAttr));
    }
}

void DSMKeeper::connectRecv(ReliableRecvMessageConnection &recv,
                            int remoteID,
                            const ExchangeMeta &exMeta)
{
    auto &qp = recv.QPs()[remoteID];
    auto &ctx = recv.context();
    CHECK_EQ(qp->qp_type, IBV_QPT_RC);
    CHECK(modifyQPtoInit(qp, &ctx));
    CHECK(modifyQPtoRTR(
        qp, exMeta.rsend.qpn, exMeta.rsend.lid, exMeta.rsend.gid, &ctx));
    CHECK(modifyQPtoRTS(qp));
    LOG(INFO) << "[debug] Recv connect to remote " << remoteID
              << ", hash(rsend): " << std::hex
              << djb2_digest((char *) &exMeta.rsend, sizeof(exMeta.rsend));
}

void DSMKeeper::connectDir(DirectoryConnection &dir,
                           int remoteID,
                           int appID,
                           const ExchangeMeta &exMeta)
{
    auto &qp = dir.QPs[appID][remoteID];
    CHECK(qp->qp_type == IBV_QPT_RC);
    CHECK(modifyQPtoInit(qp, &dir.ctx));
    CHECK(modifyQPtoRTR(qp,
                        exMeta.appRcQpn2dir[appID][dir.dirID],
                        exMeta.appTh[appID].lid,
                        exMeta.appTh[appID].gid,
                        &dir.ctx));
    CHECK(modifyQPtoRTS(qp));
    VLOG(1) << "[keeper] (re)connection DirectoryConnection[" << dir.dirID
            << "]. for remoteID " << remoteID << ", Th " << appID
            << ". dirRcQpn2app: " << exMeta.appRcQpn2dir[appID][dir.dirID]
            << ", lid: " << exMeta.appTh[appID].lid
            << ", gid: " << (void*) exMeta.appTh[appID].gid;
}

void DSMKeeper::applyExchangeMeta(uint16_t remoteID, const ExchangeMeta &exMeta)
{
    // I believe the exMeta is correct here.
    // so do a snapshot for later retrieval
    snapshotConnectRemoteMeta(remoteID, exMeta);

    // init directory qp
    for (int i = 0; i < NR_DIRECTORY; ++i)
    {
        auto &dirC = *dirCon[i];
        CHECK_EQ(dirC.dirID, i);
        for (size_t appID = 0; appID < MAX_APP_THREAD; ++appID)
        {
            connectDir(dirC, remoteID, appID, exMeta);
        }
    }

    // reliable recv
    connectRecv(reliableCon.recv_, remoteID, exMeta);

    // init application qp
    for (size_t i = 0; i < thCon.size(); ++i)
    {
        auto &thC = *thCon[i];
        CHECK_EQ(thC.threadID, i);
        for (size_t dirID = 0; dirID < NR_DIRECTORY; ++dirID)
        {
            connectThread(thC, remoteID, dirID, exMeta);
        }
    }

    // reliable send
    connectSend(reliableCon.send_, remoteID, exMeta);

    // init remote connections
    auto &remote = remoteCon[remoteID];
    remote.dsmBase = exMeta.dsmBase;
    LOG_FIRST_N(INFO, 1) << "[system] set dsmBase to "
                         << (void *) remote.dsmBase;
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
                       &thCon[k]->ctx);
            remote.appToDirAh[k][i] = ibv_create_ah(thCon[k]->ctx.pd, &ahAttr);

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
                       &dirCon[k]->ctx);
            remote.dirToAppAh[k][i] = ibv_create_ah(dirCon[k]->ctx.pd, &ahAttr);

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