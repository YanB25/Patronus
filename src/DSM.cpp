#include "DSM.h"

#include <algorithm>
#include <memory>

#include "DSMKeeper.h"
#include "Directory.h"
#include "HugePageAlloc.h"
#include "Rdma.h"
#include "Timer.h"
#include "umsg/UnreliableConnection.h"
#include "util/Util.h"

thread_local int DSM::thread_id_ = -1;
thread_local int DSM::thread_name_id_ = -1;
thread_local ThreadConnection *DSM::iCon_ = nullptr;
thread_local char *DSM::rdma_buffer_ = nullptr;
thread_local LocalAllocator DSM::local_allocator_;
thread_local RdmaBuffer DSM::rbuf_[define::kMaxCoroNr];
thread_local uint64_t DSM::thread_tag_ = 0;

std::shared_ptr<DSM> DSM::getInstance(const DSMConfig &conf)
{
    return std::make_unique<DSM>(conf);
}

DSM::DSM(const DSMConfig &conf) : conf(conf), cache(conf.cacheConfig)
{
    baseAddrSize = dsm_reserve_size() + user_reserve_size() + buffer_size();
    baseAddr = (uint64_t) hugePageAlloc(baseAddrSize);
    while ((void *) baseAddr == nullptr)
    {
        LOG(ERROR) << "[dsm] Failed to hugePageAlloc for size " << baseAddrSize
                   << ". Sleep for a while and retry";
        std::this_thread::sleep_for(5s);
        baseAddr = (uint64_t) hugePageAlloc(baseAddrSize);
    }
    LOG(INFO) << "[DSM] Total buffer: "
              << Buffer((char *) baseAddr, baseAddrSize);

    // warmup
    for (uint64_t i = baseAddr; i < baseAddr + baseAddrSize; i += 2_MB)
    {
        *(char *) i = 0;
    }

    // clear up first chunk
    memset((char *) baseAddr, 0, define::kChunkSize);

    initRDMAConnection();

    keeper->barrier("DSM-init", 1ms);

    auto nid = get_node_id();
    LOG(WARNING) << "[system] DSM ready. node_id: " << nid << ". "
                 << (::config::is_client(nid) ? "Client" : "Server");

    explain();

    using namespace std::chrono_literals;
    std::this_thread::sleep_for(1s);
}

void DSM::initExchangeMetadataBootstrap()
{
    CHECK(hasRegistered());

    for (size_t node_id = 0; node_id < getClusterSize(); ++node_id)
    {
        const auto &src_meta = keeper->getExchangeMeta(node_id);
        auto &dst_meta = getExchangeMetaBootstrap(node_id);
        memcpy(&dst_meta, &src_meta, sizeof(ExchangeMeta));
    }
}

void DSM::syncMetadataBootstrap(const ExchangeMeta &self_meta, size_t remoteID)
{
    const auto &src_meta = self_meta;

    if (remoteID == get_node_id())
    {
        auto &dst_meta = getExchangeMetaBootstrap(remoteID);
        memcpy(&dst_meta, &src_meta, sizeof(dst_meta));
    }
    else
    {
        static auto wc_err_h = [](ibv_wc *wc) {
            LOG(ERROR)
                << "[keeper] failed to broadcast metadata bootstrap. wr_id: "
                << wc->wr_id;
        };

        GlobalAddress gaddr;
        gaddr.nodeID = remoteID;
        gaddr.offset = get_node_id() * sizeof(ExchangeMeta);
        auto rdma_buffer = get_rdma_buffer();
        auto *buffer = rdma_buffer.buffer;
        DCHECK_LT(sizeof(ExchangeMeta), rdma_buffer.size);
        memcpy(buffer, &src_meta, sizeof(src_meta));
        write_sync(buffer, gaddr, sizeof(ExchangeMeta), nullptr, 0, wc_err_h);
    }
}

DSM::~DSM()
{
    for (size_t node_id = 0; node_id < remoteInfo.size(); ++node_id)
    {
        remoteInfo[node_id].destroy();
    }
}

ExchangeMeta &DSM::getExchangeMetaBootstrap(size_t node_id) const
{
    size_t my_node_id = get_node_id();
    char *start_addr = (char *) remoteInfo[my_node_id].dsmBase;
    char *meta_start_addr = start_addr + node_id * sizeof(ExchangeMeta);
    return *(ExchangeMeta *) meta_start_addr;
}

bool DSM::reinitializeDir(size_t dirID)
{
    ContTimer<config::kMonitorReconnection> timer("DSM::reinitialzeDir");
    LOG(INFO) << "[DSM] Reinitialize DirectoryConnetion[" << dirID << "]";

    CHECK_LT(dirID, dirCon.size());
    // here destroy connection
    dirCon[dirID].reset();
    dirCon[dirID] = std::make_unique<DirectoryConnection>(
        dirID, (void *) baseAddr, baseAddrSize, conf.machineNR, remoteInfo);

    timer.pin("Reinit DirConnection");

    // update the boostrapped exchangeMeta for all the peers
    for (size_t remoteID = 0; remoteID < getClusterSize(); ++remoteID)
    {
        auto ex = keeper->updateDirMetadata(*dirCon[dirID], remoteID);
        // DVLOG(1) << "[DSM] update dir meta for " << remoteID
        //          << ", hash: " << std::hex
        //          << util::djb2_digest((char *) &ex, sizeof(ex))
        //          << ", rkey: " << ex.dirTh[dirID].rKey;
        syncMetadataBootstrap(ex, remoteID);
        auto connect_dir_ex = getExchangeMetaBootstrap(remoteID);

        for (size_t appID = 0; appID < kMaxAppThread; ++appID)
        {
            keeper->connectDir(*dirCon[dirID], remoteID, appID, connect_dir_ex);
        }
    }
    timer.pin("Reconnect to ThreadConnections");
    timer.report();
    return true;
}

bool DSM::reconnectThreadToDir(size_t node_id, size_t dirID)
{
    ContTimer<config::kMonitorReconnection> timer("DSM::reconnectThreadToDir");
    DCHECK_LT(dirID, NR_DIRECTORY);

    LOG(INFO) << "[DSM] reconnect ThreadConnection for node " << node_id
              << ", dir " << dirID;

    for (size_t i = 0; i < kMaxAppThread; ++i)
    {
        if (!thCon[i]->resetQP(node_id, dirID))
        {
            LOG(WARNING)
                << "[DSM] failed to resetQP for ThreadConnection. thCon[" << i
                << "]";
            return false;
        }
        const auto &cur_meta = getExchangeMetaBootstrap(node_id);

        DVLOG(::config::verbose::kSystem)
            << "[DSM] reconnecting ThreadConnection[" << i
            << "]. node_id: " << node_id << ", dirID: " << dirID
            << ", meta digest: " << std::hex
            << util::djb2_digest((char *) &cur_meta, sizeof(cur_meta))
            << ", rkey: " << cur_meta.dirTh[dirID].rKey;

        keeper->connectThread(*thCon[i], node_id, dirID, cur_meta);

        keeper->updateRemoteConnectionForDir(
            remoteInfo[node_id], cur_meta, dirID);
    }
    timer.pin("end");
    timer.report();
    return true;
}

bool DSM::recoverThreadQP(int node_id, size_t dirID, util::TraceView v)
{
    auto tid = get_thread_id();
    ibv_qp *qp = get_th_qp(node_id, dirID);
    DVLOG(::config::verbose::kSystem)
        << "Recovering th qp: " << qp << ". node_id: " << node_id
        << ", thread_id: " << tid;
    const auto &ex = getExchangeMetaBootstrap(node_id);

    if (!modifyErrQPtoNormal(qp,
                             ex.dirRcQpn2app[dirID][tid],
                             ex.dirTh[dirID].lid,
                             ex.dirTh[dirID].gid,
                             &iCon_->ctx))
    {
        LOG(ERROR) << "failed to modify th QP to normal state. node_id: "
                   << node_id << ", thread_id: " << tid;
        return false;
    }
    rdmaQueryQueuePair(qp);

    v.pin("client-recovery");
    return true;
}

bool DSM::recoverDirQP(int node_id, int thread_id, size_t dirID)
{
    ibv_qp *qp = get_dir_qp(node_id, thread_id, dirID);
    DVLOG(::config::verbose::kSystem)
        << "Recovering dir qp " << qp << ". node_id: " << node_id
        << ", thread_id: " << thread_id;
    const auto &ex = getExchangeMetaBootstrap(node_id);
    if (!modifyErrQPtoNormal(qp,
                             ex.appRcQpn2dir[thread_id][dirID],
                             ex.appTh[thread_id].lid,
                             ex.appTh[thread_id].gid,
                             &dirCon[dirID]->ctx))
    {
        LOG(ERROR) << "failed to modify dir QP to normal state. node: "
                   << node_id << ", tid: " << thread_id;
        return false;
    }

    return true;
}

ThreadResourceDesc DSM::getCurrentThreadDesc()
{
    ThreadResourceDesc desc;
    desc.thread_id = thread_id_;
    desc.thread_tag = thread_tag_;
    desc.icon = iCon_;
    desc.rdma_buffer = rdma_buffer_;
    desc.rdma_buffer_size = define::kRDMABufferSize;
    return desc;
}

ThreadResourceDesc DSM::prepareThread()
{
    ThreadResourceDesc desc;

    desc.thread_id = appID.fetch_add(1);
    CHECK_LT(desc.thread_id, (int) thCon.size())
        << "Can not allocate more threads";
    desc.thread_tag =
        desc.thread_id + (((uint64_t) this->getMyNodeID()) << 32) + 1;

    desc.icon = thCon[desc.thread_id].get();

    desc.icon->message->initRecv();
    desc.icon->message->initSend();

    CHECK_LT(desc.thread_id * define::kRDMABufferSize, cache.size)
        << "Run out of cache size for offset = "
        << desc.thread_id * define::kRDMABufferSize;
    desc.rdma_buffer =
        (char *) cache.data + desc.thread_id * define::kRDMABufferSize;
    desc.rdma_buffer_size = define::kRDMABufferSize;
    return desc;
}

bool DSM::hasRegistered() const
{
    return thread_id_ != -1;
}

bool DSM::applyResource(const ThreadResourceDesc &desc, bool bind_core)
{
    thread_id_ = desc.thread_id;
    if (unlikely(thread_name_id_ == -1))
    {
        thread_name_id_ = thread_id_;
    }
    thread_tag_ = desc.thread_tag;
    iCon_ = desc.icon;
    rdma_buffer_ = desc.rdma_buffer;

    if (bind_core)
    {
        bindCore(thread_id_);
    }
    VLOG(SV) << "[DSM] thread applying to: " << desc;

    if (unlikely(thread_id_ == 0))
    {
        // After the system boot, we do not rely on memcached anymore (and it is
        // slow.) we use RDMA itself to maintain metadata (i.e., in the
        // *bootstrap* way).
        LOG(INFO)
            << "[DSM] thread tid == 0 initing exchange metadata (bootstrap)...";
        initExchangeMetadataBootstrap();
    }

    return true;
}

bool DSM::registerThread()
{
    if (hasRegistered())
    {
        return false;
    }

    auto desc = prepareThread();
    auto succ = applyResource(desc, true);
    LOG_IF(WARNING, !succ) << "[DSM] failed to apply resource.";
    return true;
}

void DSM::initRDMAConnection()
{
    ContTimer<config::kMonitorControlPath> timer("DSM::initRDMAConnection()");

    LOG(INFO) << "Machine NR: " << conf.machineNR;

    remoteInfo.resize(conf.machineNR);

    for (int i = 0; i < kMaxAppThread; ++i)
    {
        thCon.emplace_back(std::make_unique<ThreadConnection>(
            i, (void *) cache.data, cache.size, conf.machineNR, remoteInfo));
    }
    timer.pin("thCons " + std::to_string(kMaxAppThread));

    for (int i = 0; i < NR_DIRECTORY; ++i)
    {
        dirCon.emplace_back(std::make_unique<DirectoryConnection>(
            i, (void *) baseAddr, baseAddrSize, conf.machineNR, remoteInfo));
    }
    timer.pin("dirCons " + std::to_string(NR_DIRECTORY));

    umsg_ = std::make_unique<UnreliableConnection<kMaxAppThread>>(
        cache.data, cache.size, remoteInfo);

    timer.pin("keeper init");

    // thCon, dirCon, remoteInfo set up here.
    keeper = DSMKeeper::newInstance(
        thCon, dirCon, *umsg_, remoteInfo, conf.machineNR);
    timer.pin("keeper init");

    myNodeID = keeper->getMyNodeID();
    timer.report();
}

void DSM::rkey_read(uint32_t rkey,
                    char *buffer,
                    GlobalAddress gaddr,
                    size_t size,
                    size_t dirID,
                    bool signal,
                    CoroContext *ctx,
                    uint64_t wr_id)
{
    DCHECK_LT(dirID, iCon_->QPs.size());
    DCHECK_LT(gaddr.nodeID, iCon_->QPs[dirID].size());
    if (ctx == nullptr)
    {
        rdmaRead(iCon_->QPs[dirID][gaddr.nodeID],
                 (uint64_t) buffer,
                 gaddr_to_addr(gaddr),
                 size,
                 iCon_->cacheLKey,
                 rkey,
                 signal,
                 wr_id);
    }
    else
    {
        DCHECK(signal) << "** should signal for coroutine";
        rdmaRead(iCon_->QPs[dirID][gaddr.nodeID],
                 (uint64_t) buffer,
                 gaddr_to_addr(gaddr),
                 size,
                 iCon_->cacheLKey,
                 rkey,
                 true /* has to signal for coroutine */,
                 wr_id);
        ctx->yield_to_master();
    }
}

bool DSM::rkey_read_sync(uint32_t rkey,
                         char *buffer,
                         GlobalAddress gaddr,
                         size_t size,
                         size_t dirID,
                         CoroContext *ctx,
                         uint64_t wr_id,
                         const WcErrHandler &handler)
{
    rkey_read(rkey, buffer, gaddr, size, dirID, true, ctx, wr_id);

    if (ctx == nullptr)
    {
        ibv_wc wc;
        int ret = pollWithCQ(iCon_->cq, 1, &wc, handler);
        if (ret < 0)
        {
            LOG(WARNING) << "[qp] state err. iCon->QPs[" << dirID << "]["
                         << gaddr.nodeID << "]";
            DCHECK(rdmaQueryQueuePair(iCon_->QPs[dirID][gaddr.nodeID]) ==
                   IBV_QPS_ERR);
            if (!recoverThreadQP(gaddr.nodeID, dirID))
            {
                LOG(ERROR) << "[qp] failed to recovery. iCon->QPs[" << dirID
                           << "][" << gaddr.nodeID << "]";
                return false;
            }
            return false;
        }
    }
    return true;
}

void DSM::rkey_write(uint32_t rkey,
                     const char *buffer,
                     GlobalAddress gaddr,
                     size_t size,
                     size_t dirID,
                     bool signal,
                     CoroContext *ctx,
                     uint64_t wr_id)
{
    DCHECK_LT(dirID, iCon_->QPs.size());
    DCHECK_LT(gaddr.nodeID, iCon_->QPs[dirID].size());
    if (ctx == nullptr)
    {
        rdmaWrite(iCon_->QPs[dirID][gaddr.nodeID],
                  (uint64_t) buffer,
                  gaddr_to_addr(gaddr),
                  size,
                  iCon_->cacheLKey,
                  rkey,
                  -1,
                  signal,
                  wr_id);
    }
    else
    {
        DCHECK(signal) << "** should signal for coroutine.";
        rdmaWrite(iCon_->QPs[dirID][gaddr.nodeID],
                  (uint64_t) buffer,
                  gaddr_to_addr(gaddr),
                  size,
                  iCon_->cacheLKey,
                  rkey,
                  -1,
                  true /* has to signal for coroutine */,
                  wr_id);
        ctx->yield_to_master();
    }
}

bool DSM::rkey_write_sync(uint32_t rkey,
                          const char *buffer,
                          GlobalAddress gaddr,
                          size_t size,
                          size_t dirID,
                          CoroContext *ctx,
                          uint64_t wr_id,
                          const WcErrHandler &handler)
{
    rkey_write(rkey, buffer, gaddr, size, dirID, true, ctx, wr_id);

    if (ctx == nullptr)
    {
        ibv_wc wc;
        int ret = pollWithCQ(iCon_->cq, 1, &wc, handler);
        if (ret < 0)
        {
            LOG(WARNING) << "[qp] state err. iCon->QPs[" << dirID << "]["
                         << gaddr.nodeID << "]";
            DCHECK(rdmaQueryQueuePair(iCon_->QPs[dirID][gaddr.nodeID]) ==
                   IBV_QPS_ERR);
            if (!recoverThreadQP(gaddr.nodeID, dirID))
            {
                LOG(ERROR) << "[qp] failed to recover iCon->QPs[" << dirID
                           << "][" << gaddr.nodeID << "]";
                return false;
            }
            return false;
        }
    }
    return true;
}

void DSM::fill_keys_dest(RdmaOpRegion &ror,
                         GlobalAddress gaddr,
                         bool is_chip,
                         size_t dirID)
{
    DCHECK_LT(dirID, NR_DIRECTORY);
    ror.lkey = iCon_->cacheLKey;
    if (is_chip)
    {
        ror.dest = gaddr_to_addr(gaddr);
        ror.remoteRKey = remoteInfo[gaddr.nodeID].dmRKey[dirID];
    }
    else
    {
        ror.dest = gaddr_to_addr(gaddr);
        ror.remoteRKey = remoteInfo[gaddr.nodeID].dsmRKey[dirID];
    }
}

void DSM::write_batch(RdmaOpRegion *rs, int k, bool signal, CoroContext *ctx)
{
    int node_id = -1;
    for (int i = 0; i < k; ++i)
    {
        GlobalAddress gaddr;
        gaddr.val = rs[i].dest;
        node_id = gaddr.nodeID;
        fill_keys_dest(rs[i], gaddr, rs[i].is_on_chip);
    }

    size_t cur_dir = get_cur_dir();
    if (ctx == nullptr)
    {
        rdmaWriteBatch(iCon_->QPs[cur_dir][node_id], rs, k, signal);
    }
    else
    {
        rdmaWriteBatch(
            iCon_->QPs[cur_dir][node_id], rs, k, true, ctx->coro_id());
        ctx->yield_to_master();
    }
}

void DSM::write_batch_sync(RdmaOpRegion *rs, int k, CoroContext *ctx)
{
    write_batch(rs, k, true, ctx);

    if (ctx == nullptr)
    {
        ibv_wc wc;
        pollWithCQ(iCon_->cq, 1, &wc);
    }
}

void DSM::write_faa(RdmaOpRegion &write_ror,
                    RdmaOpRegion &faa_ror,
                    uint64_t add_val,
                    bool signal,
                    CoroContext *ctx)
{
    size_t cur_dir = get_cur_dir();

    int node_id;
    {
        GlobalAddress gaddr;
        gaddr.val = write_ror.dest;
        node_id = gaddr.nodeID;

        fill_keys_dest(write_ror, gaddr, write_ror.is_on_chip);
    }
    {
        GlobalAddress gaddr;
        gaddr.val = faa_ror.dest;

        fill_keys_dest(faa_ror, gaddr, faa_ror.is_on_chip);
    }
    if (ctx == nullptr)
    {
        rdmaWriteFaa(
            iCon_->QPs[cur_dir][node_id], write_ror, faa_ror, add_val, signal);
    }
    else
    {
        rdmaWriteFaa(iCon_->QPs[cur_dir][node_id],
                     write_ror,
                     faa_ror,
                     add_val,
                     true,
                     ctx->coro_id());
        ctx->yield_to_master();
    }
}
void DSM::write_faa_sync(RdmaOpRegion &write_ror,
                         RdmaOpRegion &faa_ror,
                         uint64_t add_val,
                         CoroContext *ctx)
{
    write_faa(write_ror, faa_ror, add_val, true, ctx);
    if (ctx == nullptr)
    {
        ibv_wc wc;
        pollWithCQ(iCon_->cq, 1, &wc);
    }
}

void DSM::write_cas(RdmaOpRegion &write_ror,
                    RdmaOpRegion &cas_ror,
                    uint64_t equal,
                    uint64_t val,
                    bool signal,
                    CoroContext *ctx)
{
    size_t cur_dir = get_cur_dir();

    int node_id;
    {
        GlobalAddress gaddr;
        gaddr.val = write_ror.dest;
        node_id = gaddr.nodeID;

        fill_keys_dest(write_ror, gaddr, write_ror.is_on_chip);
    }
    {
        GlobalAddress gaddr;
        gaddr.val = cas_ror.dest;

        fill_keys_dest(cas_ror, gaddr, cas_ror.is_on_chip);
    }
    if (ctx == nullptr)
    {
        rdmaWriteCas(iCon_->QPs[cur_dir][node_id],
                     write_ror,
                     cas_ror,
                     equal,
                     val,
                     signal);
    }
    else
    {
        rdmaWriteCas(iCon_->QPs[cur_dir][node_id],
                     write_ror,
                     cas_ror,
                     equal,
                     val,
                     true,
                     ctx->coro_id());
        ctx->yield_to_master();
    }
}
void DSM::write_cas_sync(RdmaOpRegion &write_ror,
                         RdmaOpRegion &cas_ror,
                         uint64_t equal,
                         uint64_t val,
                         CoroContext *ctx)
{
    write_cas(write_ror, cas_ror, equal, val, true, ctx);
    if (ctx == nullptr)
    {
        ibv_wc wc;
        pollWithCQ(iCon_->cq, 1, &wc);
    }
}

void DSM::cas_read(RdmaOpRegion &cas_ror,
                   RdmaOpRegion &read_ror,
                   uint64_t equal,
                   uint64_t val,
                   bool signal,
                   CoroContext *ctx)
{
    size_t cur_dir = get_cur_dir();
    int node_id;
    {
        GlobalAddress gaddr;
        gaddr.val = cas_ror.dest;
        node_id = gaddr.nodeID;
        fill_keys_dest(cas_ror, gaddr, cas_ror.is_on_chip);
    }
    {
        GlobalAddress gaddr;
        gaddr.val = read_ror.dest;
        fill_keys_dest(read_ror, gaddr, read_ror.is_on_chip);
    }

    if (ctx == nullptr)
    {
        rdmaCasRead(iCon_->QPs[cur_dir][node_id],
                    cas_ror,
                    read_ror,
                    equal,
                    val,
                    signal);
    }
    else
    {
        rdmaCasRead(iCon_->QPs[cur_dir][node_id],
                    cas_ror,
                    read_ror,
                    equal,
                    val,
                    true,
                    ctx->coro_id());
        ctx->yield_to_master();
    }
}

bool DSM::cas_read_sync(RdmaOpRegion &cas_ror,
                        RdmaOpRegion &read_ror,
                        uint64_t equal,
                        uint64_t val,
                        CoroContext *ctx)
{
    cas_read(cas_ror, read_ror, equal, val, true, ctx);

    if (ctx == nullptr)
    {
        ibv_wc wc;
        pollWithCQ(iCon_->cq, 1, &wc);
    }

    return equal == *(uint64_t *) cas_ror.source;
}

void DSM::rkey_cas(uint32_t rkey,
                   char *rdma_buffer,
                   GlobalAddress gaddr,
                   size_t dir_id,
                   uint64_t compare,
                   uint64_t swap,
                   bool is_signal,
                   uint64_t wr_id,
                   CoroContext *ctx)
{
    DCHECK_LT(dir_id, iCon_->QPs.size());
    DCHECK_LT(gaddr.nodeID, iCon_->QPs[dir_id].size());
    if (unlikely(ctx == nullptr))
    {
        rdmaCompareAndSwap(iCon_->QPs[dir_id][gaddr.nodeID],
                           (uint64_t) rdma_buffer,
                           gaddr_to_addr(gaddr),
                           compare,
                           swap,
                           iCon_->cacheLKey,
                           rkey,
                           is_signal,
                           wr_id);
    }
    else
    {
        DCHECK(is_signal) << "** should signal for coroutine";
        rdmaCompareAndSwap(iCon_->QPs[dir_id][gaddr.nodeID],
                           (uint64_t) rdma_buffer,
                           gaddr_to_addr(gaddr),
                           compare,
                           swap,
                           iCon_->cacheLKey,
                           rkey,
                           is_signal,
                           wr_id);
        ctx->yield_to_master();
    }
}

void DSM::cas(GlobalAddress gaddr,
              uint64_t equal,
              uint64_t val,
              uint64_t *rdma_buffer,
              bool signal,
              uint64_t wr_id,
              CoroContext *ctx)
{
    size_t cur_dir = get_cur_dir();
    if (ctx == nullptr)
    {
        rdmaCompareAndSwap(iCon_->QPs[cur_dir][gaddr.nodeID],
                           (uint64_t) rdma_buffer,
                           gaddr_to_addr(gaddr),
                           equal,
                           val,
                           iCon_->cacheLKey,
                           remoteInfo[gaddr.nodeID].dsmRKey[cur_dir],
                           signal,
                           wr_id);
    }
    else
    {
        rdmaCompareAndSwap(iCon_->QPs[cur_dir][gaddr.nodeID],
                           (uint64_t) rdma_buffer,
                           gaddr_to_addr(gaddr),
                           equal,
                           val,
                           iCon_->cacheLKey,
                           remoteInfo[gaddr.nodeID].dsmRKey[cur_dir],
                           true,
                           wr_id);
        ctx->yield_to_master();
    }
}

bool DSM::cas_sync(GlobalAddress gaddr,
                   uint64_t equal,
                   uint64_t val,
                   uint64_t *rdma_buffer,
                   CoroContext *ctx)
{
    auto wr_id = ctx ? ctx->coro_id() : 0;
    cas(gaddr, equal, val, rdma_buffer, true, wr_id, ctx);

    if (ctx == nullptr)
    {
        ibv_wc wc;
        pollWithCQ(iCon_->cq, 1, &wc);
    }

    return equal == *rdma_buffer;
}

void DSM::cas_mask(GlobalAddress gaddr,
                   uint64_t equal,
                   uint64_t val,
                   uint64_t *rdma_buffer,
                   uint64_t mask,
                   bool signal)
{
    size_t cur_dir = get_cur_dir();
    rdmaCompareAndSwapMask(iCon_->QPs[cur_dir][gaddr.nodeID],
                           (uint64_t) rdma_buffer,
                           gaddr_to_addr(gaddr),
                           equal,
                           val,
                           iCon_->cacheLKey,
                           remoteInfo[gaddr.nodeID].dsmRKey[cur_dir],
                           mask,
                           signal);
}

bool DSM::cas_mask_sync(GlobalAddress gaddr,
                        uint64_t equal,
                        uint64_t val,
                        uint64_t *rdma_buffer,
                        uint64_t mask)
{
    cas_mask(gaddr, equal, val, rdma_buffer, mask);
    ibv_wc wc;
    pollWithCQ(iCon_->cq, 1, &wc);

    return (equal & mask) == (*rdma_buffer & mask);
}

void DSM::faa_boundary(GlobalAddress gaddr,
                       uint64_t add_val,
                       uint64_t *rdma_buffer,
                       uint64_t mask,
                       bool signal,
                       CoroContext *ctx)
{
    size_t cur_dir = get_cur_dir();
    if (ctx == nullptr)
    {
        rdmaFetchAndAddBoundary(iCon_->QPs[cur_dir][gaddr.nodeID],
                                (uint64_t) rdma_buffer,
                                gaddr_to_addr(gaddr),
                                add_val,
                                iCon_->cacheLKey,
                                remoteInfo[gaddr.nodeID].dsmRKey[cur_dir],
                                mask,
                                signal);
    }
    else
    {
        rdmaFetchAndAddBoundary(iCon_->QPs[cur_dir][gaddr.nodeID],
                                (uint64_t) rdma_buffer,
                                gaddr_to_addr(gaddr),
                                add_val,
                                iCon_->cacheLKey,
                                remoteInfo[gaddr.nodeID].dsmRKey[cur_dir],
                                mask,
                                true,
                                ctx->coro_id());
        ctx->yield_to_master();
    }
}
void DSM::faa_boundary_sync(GlobalAddress gaddr,
                            uint64_t add_val,
                            uint64_t *rdma_buffer,
                            uint64_t mask,
                            CoroContext *ctx)
{
    faa_boundary(gaddr, add_val, rdma_buffer, mask, true, ctx);
    if (ctx == nullptr)
    {
        ibv_wc wc;
        pollWithCQ(iCon_->cq, 1, &wc);
    }
}

void DSM::read_dm(char *buffer,
                  GlobalAddress gaddr,
                  size_t size,
                  bool signal,
                  CoroContext *ctx)
{
    // only dirID == 0 has dm

    size_t cur_dir = 0;
    if (ctx == nullptr)
    {
        rdmaRead(iCon_->QPs[cur_dir][gaddr.nodeID],
                 (uint64_t) buffer,
                 remoteInfo[gaddr.nodeID].dmBase + gaddr.offset,
                 size,
                 iCon_->cacheLKey,
                 remoteInfo[gaddr.nodeID].dmRKey[cur_dir],
                 signal);
    }
    else
    {
        rdmaRead(iCon_->QPs[cur_dir][gaddr.nodeID],
                 (uint64_t) buffer,
                 remoteInfo[gaddr.nodeID].dmBase + gaddr.offset,
                 size,
                 iCon_->cacheLKey,
                 remoteInfo[gaddr.nodeID].dmRKey[cur_dir],
                 true,
                 ctx->coro_id());
        ctx->yield_to_master();
    }
}

void DSM::read_dm_sync(char *buffer,
                       GlobalAddress gaddr,
                       size_t size,
                       CoroContext *ctx)
{
    read_dm(buffer, gaddr, size, true, ctx);

    if (ctx == nullptr)
    {
        ibv_wc wc;
        pollWithCQ(iCon_->cq, 1, &wc);
    }
}

void DSM::write_dm(const char *buffer,
                   GlobalAddress gaddr,
                   size_t size,
                   bool signal,
                   CoroContext *ctx)
{
    size_t cur_dir = 0;
    if (ctx == nullptr)
    {
        rdmaWrite(iCon_->QPs[cur_dir][gaddr.nodeID],
                  (uint64_t) buffer,
                  remoteInfo[gaddr.nodeID].dmBase + gaddr.offset,
                  size,
                  iCon_->cacheLKey,
                  remoteInfo[gaddr.nodeID].dmRKey[cur_dir],
                  -1,
                  signal);
    }
    else
    {
        rdmaWrite(iCon_->QPs[cur_dir][gaddr.nodeID],
                  (uint64_t) buffer,
                  remoteInfo[gaddr.nodeID].dmBase + gaddr.offset,
                  size,
                  iCon_->cacheLKey,
                  remoteInfo[gaddr.nodeID].dmRKey[cur_dir],
                  -1,
                  true,
                  ctx->coro_id());
        ctx->yield_to_master();
    }
}

void DSM::write_dm_sync(const char *buffer,
                        GlobalAddress gaddr,
                        size_t size,
                        CoroContext *ctx)
{
    write_dm(buffer, gaddr, size, true, ctx);

    if (ctx == nullptr)
    {
        ibv_wc wc;
        pollWithCQ(iCon_->cq, 1, &wc);
    }
}

void DSM::cas_dm(GlobalAddress gaddr,
                 uint64_t equal,
                 uint64_t val,
                 uint64_t *rdma_buffer,
                 bool signal,
                 CoroContext *ctx)
{
    size_t cur_dir = 0;
    if (ctx == nullptr)
    {
        rdmaCompareAndSwap(iCon_->QPs[cur_dir][gaddr.nodeID],
                           (uint64_t) rdma_buffer,
                           remoteInfo[gaddr.nodeID].dmBase + gaddr.offset,
                           equal,
                           val,
                           iCon_->cacheLKey,
                           remoteInfo[gaddr.nodeID].dmRKey[cur_dir],
                           signal);
    }
    else
    {
        rdmaCompareAndSwap(iCon_->QPs[cur_dir][gaddr.nodeID],
                           (uint64_t) rdma_buffer,
                           remoteInfo[gaddr.nodeID].dmBase + gaddr.offset,
                           equal,
                           val,
                           iCon_->cacheLKey,
                           remoteInfo[gaddr.nodeID].dmRKey[cur_dir],
                           true,
                           ctx->coro_id());
        ctx->yield_to_master();
    }
}

bool DSM::cas_dm_sync(GlobalAddress gaddr,
                      uint64_t equal,
                      uint64_t val,
                      uint64_t *rdma_buffer,
                      CoroContext *ctx)
{
    cas_dm(gaddr, equal, val, rdma_buffer, true, ctx);

    if (ctx == nullptr)
    {
        ibv_wc wc;
        pollWithCQ(iCon_->cq, 1, &wc);
    }

    return equal == *rdma_buffer;
}

void DSM::cas_dm_mask(GlobalAddress gaddr,
                      uint64_t equal,
                      uint64_t val,
                      uint64_t *rdma_buffer,
                      uint64_t mask,
                      bool signal)
{
    size_t cur_dir = 0;
    rdmaCompareAndSwapMask(iCon_->QPs[cur_dir][gaddr.nodeID],
                           (uint64_t) rdma_buffer,
                           remoteInfo[gaddr.nodeID].dmBase + gaddr.offset,
                           equal,
                           val,
                           iCon_->cacheLKey,
                           remoteInfo[gaddr.nodeID].dmRKey[cur_dir],
                           mask,
                           signal);
}

bool DSM::cas_dm_mask_sync(GlobalAddress gaddr,
                           uint64_t equal,
                           uint64_t val,
                           uint64_t *rdma_buffer,
                           uint64_t mask)
{
    cas_dm_mask(gaddr, equal, val, rdma_buffer, mask);
    ibv_wc wc;
    pollWithCQ(iCon_->cq, 1, &wc);

    return (equal & mask) == (*rdma_buffer & mask);
}

void DSM::faa_dm_boundary(GlobalAddress gaddr,
                          uint64_t add_val,
                          uint64_t *rdma_buffer,
                          uint64_t mask,
                          bool signal,
                          CoroContext *ctx)
{
    size_t cur_dir = 0;
    if (ctx == nullptr)
    {
        rdmaFetchAndAddBoundary(iCon_->QPs[cur_dir][gaddr.nodeID],
                                (uint64_t) rdma_buffer,
                                remoteInfo[gaddr.nodeID].dmBase + gaddr.offset,
                                add_val,
                                iCon_->cacheLKey,
                                remoteInfo[gaddr.nodeID].dmRKey[cur_dir],
                                mask,
                                signal);
    }
    else
    {
        rdmaFetchAndAddBoundary(iCon_->QPs[cur_dir][gaddr.nodeID],
                                (uint64_t) rdma_buffer,
                                remoteInfo[gaddr.nodeID].dmBase + gaddr.offset,
                                add_val,
                                iCon_->cacheLKey,
                                remoteInfo[gaddr.nodeID].dmRKey[cur_dir],
                                mask,
                                true,
                                ctx->coro_id());
        ctx->yield_to_master();
    }
}

void DSM::faa_dm_boundary_sync(GlobalAddress gaddr,
                               uint64_t add_val,
                               uint64_t *rdma_buffer,
                               uint64_t mask,
                               CoroContext *ctx)
{
    faa_dm_boundary(gaddr, add_val, rdma_buffer, mask, true, ctx);
    if (ctx == nullptr)
    {
        ibv_wc wc;
        pollWithCQ(iCon_->cq, 1, &wc);
    }
}

ibv_mw *DSM::alloc_mw(size_t dirID)
{
    DCHECK_LT(dirID, dirCon.size());
    struct RdmaContext *ctx = &dirCon[dirID]->ctx;
    // dinfo("[dsm] dirCon ID: %d, pd: %p", dirCon[cur_dir].dirID, ctx->pd);
    struct ibv_mw *mw = ibv_alloc_mw(ctx->pd, ctx->mw_type);
    if (!mw)
    {
        PLOG(ERROR) << "failed to create memory window.";
    }
    // dinfo("allocating mw at pd: %p, type: %d", ctx->pd, ctx->mw_type);
    return mw;
}

void DSM::free_mw(struct ibv_mw *mw)
{
    PCHECK(ibv_dealloc_mw(mw) == 0) << "failed to destroy mw";
}

bool DSM::bind_memory_region(struct ibv_mw *mw,
                             size_t target_node_id,
                             size_t target_thread_id,
                             const char *buffer,
                             size_t size,
                             size_t dirID,
                             size_t wr_id,
                             bool signal)
{
    DCHECK_LT(dirID, dirCon.size());
    struct ibv_qp *qp = dirCon[dirID]->QPs[target_thread_id][target_node_id];
    uint32_t rkey = rdmaAsyncBindMemoryWindow(
        qp, mw, dirCon[dirID]->dsmMR, (uint64_t) buffer, size, signal, wr_id);
    return rkey != 0;
}
bool DSM::bind_memory_region_sync(struct ibv_mw *mw,
                                  size_t target_node_id,
                                  size_t target_thread_id,
                                  const char *buffer,
                                  size_t size,
                                  size_t dirID,
                                  uint64_t wr_id,
                                  CoroContext *ctx)
{
    DCHECK_LT(dirID, dirCon.size());
    struct ibv_qp *qp = dirCon[dirID]->QPs[target_thread_id][target_node_id];
    uint32_t rkey = rdmaAsyncBindMemoryWindow(
        qp, mw, dirCon[dirID]->dsmMR, (uint64_t) buffer, size, true, wr_id);
    if (rkey == 0)
    {
        return false;
    }
    if (unlikely(ctx == nullptr))
    {
        struct ibv_wc wc;
        int ret = pollWithCQ(dirCon[dirID]->cq, 1, &wc) == 1;
        if (ret < 0)
        {
            rdmaQueryQueuePair(qp);
            return false;
        }
        return true;
    }
    else
    {
        ctx->yield_to_master();
    }
    return true;
}