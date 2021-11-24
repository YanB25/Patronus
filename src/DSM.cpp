#include "DSM.h"

#include <algorithm>
#include <memory>

#include "DSMKeeper.h"
#include "Directory.h"
#include "HugePageAlloc.h"
#include "Rdma.h"
#include "Util.h"

thread_local int DSM::thread_id = -1;
thread_local ThreadConnection *DSM::iCon = nullptr;
thread_local char *DSM::rdma_buffer = nullptr;
thread_local LocalAllocator DSM::local_allocator;
thread_local RdmaBuffer DSM::rbuf[define::kMaxCoro];
thread_local uint64_t DSM::thread_tag = 0;

std::shared_ptr<DSM> DSM::getInstance(const DSMConfig &conf)
{
    return future::make_unique<DSM>(conf);
}

DSM::DSM(const DSMConfig &conf) : conf(conf), cache(conf.cacheConfig)
{
    baseAddr = (uint64_t) hugePageAlloc(conf.dsmSize);

    // TODO: use smart print
    info("shared memory size: %s, 0x%lx",
         smart::smartSize(conf.dsmSize).c_str(),
         baseAddr);
    info("cache size: %s",
         smart::smartSize(conf.cacheConfig.cacheSize).c_str());

    // warmup
    // memset((char *)baseAddr, 0, conf.dsmSize * define::GB);
    for (uint64_t i = baseAddr; i < baseAddr + conf.dsmSize * define::GB;
         i += 2 * define::MB)
    {
        *(char *) i = 0;
    }

    // clear up first chunk
    memset((char *) baseAddr, 0, define::kChunkSize);

    initRDMAConnection();

    keeper->barrier("DSM-init");
}

DSM::~DSM()
{
}

void DSM::registerThread()
{
    if (thread_id != -1)
    {
        error("Thread already registered.");
        return;
    }

    thread_id = appID.fetch_add(1);
    check(thread_id < (int) thCon.size(), "Can not allocate more threads");
    thread_tag = thread_id + (((uint64_t) this->getMyNodeID()) << 32) + 1;

    iCon = &thCon[thread_id];
    // dinfo("register tid %d, iCon: %p, QPs[0][1]: %p", thread_id, iCon,
    // iCon->QPs[0][1]);

    iCon->message->initRecv();
    iCon->message->initSend();

    check(thread_id * define::kRDMABufferSize < cache.size,
          "Run out of cache size for offset = %" PRIu32,
          thread_id * define::kRDMABufferSize);
    rdma_buffer = (char *) cache.data + thread_id * define::kRDMABufferSize;

    for (int i = 0; i < define::kMaxCoro; ++i)
    {
        check(i * define::kPerCoroRdmaBuf < define::kRDMABufferSize,
              "Run out of RDMA buffer when allocating coroutine buffer.");
        rbuf[i].set_buffer(rdma_buffer + i * define::kPerCoroRdmaBuf);
    }
}

void DSM::initRDMAConnection()
{
    info("Machine NR: %d", conf.machineNR);

    remoteInfo.resize(conf.machineNR);

    for (int i = 0; i < MAX_APP_THREAD; ++i)
    {
        thCon.emplace_back(
            i, (void *) cache.data, cache.size, conf.machineNR, remoteInfo);
    }

    for (int i = 0; i < NR_DIRECTORY; ++i)
    {
        dirCon.emplace_back(
            i, (void *) baseAddr, conf.dsmSize, conf.machineNR, remoteInfo);
    }

    // thCon, dirCon, remoteInfo set up here.
    keeper = DSMKeeper::newInstance(thCon, dirCon, remoteInfo, conf.machineNR);

    myNodeID = keeper->getMyNodeID();
}

void DSM::rkey_read(uint32_t rkey,
                    char *buffer,
                    GlobalAddress gaddr,
                    size_t size,
                    bool signal,
                    CoroContext *ctx)
{
    if (ctx == nullptr)
    {
        rdmaRead(iCon->QPs[0][gaddr.nodeID],
                 (uint64_t) buffer,
                 remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset,
                 size,
                 iCon->cacheLKey,
                 rkey,
                 signal);
    }
    else
    {
        rdmaRead(iCon->QPs[0][gaddr.nodeID],
                 (uint64_t) buffer,
                 remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset,
                 size,
                 iCon->cacheLKey,
                 rkey,
                 true,
                 ctx->coro_id);
        (*ctx->yield)(*ctx->master);
    }
}

void DSM::rkey_read_sync(uint32_t rkey,
                         char *buffer,
                         GlobalAddress gaddr,
                         size_t size,
                         CoroContext *ctx)
{
    rkey_read(rkey, buffer, gaddr, size, true, ctx);

    if (ctx == nullptr)
    {
        ibv_wc wc;
        pollWithCQ(iCon->cq, 1, &wc);
    }
}

void DSM::read(char *buffer,
               GlobalAddress gaddr,
               size_t size,
               bool signal,
               CoroContext *ctx)
{
    uint32_t rkey = remoteInfo[gaddr.nodeID].dsmRKey[0];
    rkey_read(rkey, buffer, gaddr, size, signal, ctx);
}

void DSM::read_sync(char *buffer,
                    GlobalAddress gaddr,
                    size_t size,
                    CoroContext *ctx)
{
    uint32_t rkey = remoteInfo[gaddr.nodeID].dsmRKey[0];
    rkey_read_sync(rkey, buffer, gaddr, size, ctx);
}

void DSM::write(const char *buffer,
                GlobalAddress gaddr,
                size_t size,
                bool signal,
                CoroContext *ctx)
{
    uint32_t rkey = remoteInfo[gaddr.nodeID].dsmRKey[0];
    return rkey_write(rkey, buffer, gaddr, size, signal, ctx);
}

void DSM::write_sync(const char *buffer,
                     GlobalAddress gaddr,
                     size_t size,
                     CoroContext *ctx)
{
    uint32_t rkey = remoteInfo[gaddr.nodeID].dsmRKey[0];
    return rkey_write_sync(rkey, buffer, gaddr, size, ctx);
}

void DSM::rkey_write(uint32_t rkey,
                     const char *buffer,
                     GlobalAddress gaddr,
                     size_t size,
                     bool signal,
                     CoroContext *ctx)
{
    if (ctx == nullptr)
    {
        rdmaWrite(iCon->QPs[0][gaddr.nodeID],
                  (uint64_t) buffer,
                  remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset,
                  size,
                  iCon->cacheLKey,
                  rkey,
                  -1,
                  signal);
    }
    else
    {
        rdmaWrite(iCon->QPs[0][gaddr.nodeID],
                  (uint64_t) buffer,
                  remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset,
                  size,
                  iCon->cacheLKey,
                  rkey,
                  -1,
                  true,
                  ctx->coro_id);
        (*ctx->yield)(*ctx->master);
    }
}

void DSM::rkey_write_sync(uint32_t rkey,
                          const char *buffer,
                          GlobalAddress gaddr,
                          size_t size,
                          CoroContext *ctx)
{
    rkey_write(rkey, buffer, gaddr, size, true, ctx);

    if (ctx == nullptr)
    {
        ibv_wc wc;
        pollWithCQ(iCon->cq, 1, &wc);
    }
}

void DSM::fill_keys_dest(RdmaOpRegion &ror, GlobalAddress gaddr, bool is_chip)
{
    ror.lkey = iCon->cacheLKey;
    if (is_chip)
    {
        ror.dest = remoteInfo[gaddr.nodeID].dmBase + gaddr.offset;
        ror.remoteRKey = remoteInfo[gaddr.nodeID].dmRKey[0];
    }
    else
    {
        ror.dest = remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset;
        ror.remoteRKey = remoteInfo[gaddr.nodeID].dsmRKey[0];
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

    if (ctx == nullptr)
    {
        rdmaWriteBatch(iCon->QPs[0][node_id], rs, k, signal);
    }
    else
    {
        rdmaWriteBatch(iCon->QPs[0][node_id], rs, k, true, ctx->coro_id);
        (*ctx->yield)(*ctx->master);
    }
}

void DSM::write_batch_sync(RdmaOpRegion *rs, int k, CoroContext *ctx)
{
    write_batch(rs, k, true, ctx);

    if (ctx == nullptr)
    {
        ibv_wc wc;
        pollWithCQ(iCon->cq, 1, &wc);
    }
}

void DSM::write_faa(RdmaOpRegion &write_ror,
                    RdmaOpRegion &faa_ror,
                    uint64_t add_val,
                    bool signal,
                    CoroContext *ctx)
{
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
            iCon->QPs[0][node_id], write_ror, faa_ror, add_val, signal);
    }
    else
    {
        rdmaWriteFaa(iCon->QPs[0][node_id],
                     write_ror,
                     faa_ror,
                     add_val,
                     true,
                     ctx->coro_id);
        (*ctx->yield)(*ctx->master);
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
        pollWithCQ(iCon->cq, 1, &wc);
    }
}

void DSM::write_cas(RdmaOpRegion &write_ror,
                    RdmaOpRegion &cas_ror,
                    uint64_t equal,
                    uint64_t val,
                    bool signal,
                    CoroContext *ctx)
{
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
        rdmaWriteCas(
            iCon->QPs[0][node_id], write_ror, cas_ror, equal, val, signal);
    }
    else
    {
        rdmaWriteCas(iCon->QPs[0][node_id],
                     write_ror,
                     cas_ror,
                     equal,
                     val,
                     true,
                     ctx->coro_id);
        (*ctx->yield)(*ctx->master);
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
        pollWithCQ(iCon->cq, 1, &wc);
    }
}

void DSM::cas_read(RdmaOpRegion &cas_ror,
                   RdmaOpRegion &read_ror,
                   uint64_t equal,
                   uint64_t val,
                   bool signal,
                   CoroContext *ctx)
{
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
        rdmaCasRead(
            iCon->QPs[0][node_id], cas_ror, read_ror, equal, val, signal);
    }
    else
    {
        rdmaCasRead(iCon->QPs[0][node_id],
                    cas_ror,
                    read_ror,
                    equal,
                    val,
                    true,
                    ctx->coro_id);
        (*ctx->yield)(*ctx->master);
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
        pollWithCQ(iCon->cq, 1, &wc);
    }

    return equal == *(uint64_t *) cas_ror.source;
}

void DSM::cas(GlobalAddress gaddr,
              uint64_t equal,
              uint64_t val,
              uint64_t *rdma_buffer,
              bool signal,
              CoroContext *ctx)
{
    if (ctx == nullptr)
    {
        rdmaCompareAndSwap(iCon->QPs[0][gaddr.nodeID],
                           (uint64_t) rdma_buffer,
                           remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset,
                           equal,
                           val,
                           iCon->cacheLKey,
                           remoteInfo[gaddr.nodeID].dsmRKey[0],
                           signal);
    }
    else
    {
        rdmaCompareAndSwap(iCon->QPs[0][gaddr.nodeID],
                           (uint64_t) rdma_buffer,
                           remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset,
                           equal,
                           val,
                           iCon->cacheLKey,
                           remoteInfo[gaddr.nodeID].dsmRKey[0],
                           true,
                           ctx->coro_id);
        (*ctx->yield)(*ctx->master);
    }
}

bool DSM::cas_sync(GlobalAddress gaddr,
                   uint64_t equal,
                   uint64_t val,
                   uint64_t *rdma_buffer,
                   CoroContext *ctx)
{
    cas(gaddr, equal, val, rdma_buffer, true, ctx);

    if (ctx == nullptr)
    {
        ibv_wc wc;
        pollWithCQ(iCon->cq, 1, &wc);
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
    rdmaCompareAndSwapMask(iCon->QPs[0][gaddr.nodeID],
                           (uint64_t) rdma_buffer,
                           remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset,
                           equal,
                           val,
                           iCon->cacheLKey,
                           remoteInfo[gaddr.nodeID].dsmRKey[0],
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
    pollWithCQ(iCon->cq, 1, &wc);

    return (equal & mask) == (*rdma_buffer & mask);
}

void DSM::faa_boundary(GlobalAddress gaddr,
                       uint64_t add_val,
                       uint64_t *rdma_buffer,
                       uint64_t mask,
                       bool signal,
                       CoroContext *ctx)
{
    if (ctx == nullptr)
    {
        rdmaFetchAndAddBoundary(iCon->QPs[0][gaddr.nodeID],
                                (uint64_t) rdma_buffer,
                                remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset,
                                add_val,
                                iCon->cacheLKey,
                                remoteInfo[gaddr.nodeID].dsmRKey[0],
                                mask,
                                signal);
    }
    else
    {
        rdmaFetchAndAddBoundary(iCon->QPs[0][gaddr.nodeID],
                                (uint64_t) rdma_buffer,
                                remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset,
                                add_val,
                                iCon->cacheLKey,
                                remoteInfo[gaddr.nodeID].dsmRKey[0],
                                mask,
                                true,
                                ctx->coro_id);
        (*ctx->yield)(*ctx->master);
    }
}
Buffer DSM::get_server_internal_buffer()
{
    size_t node_id = get_node_id();
    Buffer ret((char *) remoteInfo[node_id].dsmBase, 16 * define::GB);
    return ret;
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
        pollWithCQ(iCon->cq, 1, &wc);
    }
}

void DSM::read_dm(char *buffer,
                  GlobalAddress gaddr,
                  size_t size,
                  bool signal,
                  CoroContext *ctx)
{
    if (ctx == nullptr)
    {
        rdmaRead(iCon->QPs[0][gaddr.nodeID],
                 (uint64_t) buffer,
                 remoteInfo[gaddr.nodeID].dmBase + gaddr.offset,
                 size,
                 iCon->cacheLKey,
                 remoteInfo[gaddr.nodeID].dmRKey[0],
                 signal);
    }
    else
    {
        rdmaRead(iCon->QPs[0][gaddr.nodeID],
                 (uint64_t) buffer,
                 remoteInfo[gaddr.nodeID].dmBase + gaddr.offset,
                 size,
                 iCon->cacheLKey,
                 remoteInfo[gaddr.nodeID].dmRKey[0],
                 true,
                 ctx->coro_id);
        (*ctx->yield)(*ctx->master);
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
        pollWithCQ(iCon->cq, 1, &wc);
    }
}

void DSM::write_dm(const char *buffer,
                   GlobalAddress gaddr,
                   size_t size,
                   bool signal,
                   CoroContext *ctx)
{
    if (ctx == nullptr)
    {
        rdmaWrite(iCon->QPs[0][gaddr.nodeID],
                  (uint64_t) buffer,
                  remoteInfo[gaddr.nodeID].dmBase + gaddr.offset,
                  size,
                  iCon->cacheLKey,
                  remoteInfo[gaddr.nodeID].dmRKey[0],
                  -1,
                  signal);
    }
    else
    {
        rdmaWrite(iCon->QPs[0][gaddr.nodeID],
                  (uint64_t) buffer,
                  remoteInfo[gaddr.nodeID].dmBase + gaddr.offset,
                  size,
                  iCon->cacheLKey,
                  remoteInfo[gaddr.nodeID].dmRKey[0],
                  -1,
                  true,
                  ctx->coro_id);
        (*ctx->yield)(*ctx->master);
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
        pollWithCQ(iCon->cq, 1, &wc);
    }
}

void DSM::cas_dm(GlobalAddress gaddr,
                 uint64_t equal,
                 uint64_t val,
                 uint64_t *rdma_buffer,
                 bool signal,
                 CoroContext *ctx)
{
    if (ctx == nullptr)
    {
        rdmaCompareAndSwap(iCon->QPs[0][gaddr.nodeID],
                           (uint64_t) rdma_buffer,
                           remoteInfo[gaddr.nodeID].dmBase + gaddr.offset,
                           equal,
                           val,
                           iCon->cacheLKey,
                           remoteInfo[gaddr.nodeID].dmRKey[0],
                           signal);
    }
    else
    {
        rdmaCompareAndSwap(iCon->QPs[0][gaddr.nodeID],
                           (uint64_t) rdma_buffer,
                           remoteInfo[gaddr.nodeID].dmBase + gaddr.offset,
                           equal,
                           val,
                           iCon->cacheLKey,
                           remoteInfo[gaddr.nodeID].dmRKey[0],
                           true,
                           ctx->coro_id);
        (*ctx->yield)(*ctx->master);
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
        pollWithCQ(iCon->cq, 1, &wc);
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
    rdmaCompareAndSwapMask(iCon->QPs[0][gaddr.nodeID],
                           (uint64_t) rdma_buffer,
                           remoteInfo[gaddr.nodeID].dmBase + gaddr.offset,
                           equal,
                           val,
                           iCon->cacheLKey,
                           remoteInfo[gaddr.nodeID].dmRKey[0],
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
    pollWithCQ(iCon->cq, 1, &wc);

    return (equal & mask) == (*rdma_buffer & mask);
}

void DSM::faa_dm_boundary(GlobalAddress gaddr,
                          uint64_t add_val,
                          uint64_t *rdma_buffer,
                          uint64_t mask,
                          bool signal,
                          CoroContext *ctx)
{
    if (ctx == nullptr)
    {
        rdmaFetchAndAddBoundary(iCon->QPs[0][gaddr.nodeID],
                                (uint64_t) rdma_buffer,
                                remoteInfo[gaddr.nodeID].dmBase + gaddr.offset,
                                add_val,
                                iCon->cacheLKey,
                                remoteInfo[gaddr.nodeID].dmRKey[0],
                                mask,
                                signal);
    }
    else
    {
        rdmaFetchAndAddBoundary(iCon->QPs[0][gaddr.nodeID],
                                (uint64_t) rdma_buffer,
                                remoteInfo[gaddr.nodeID].dmBase + gaddr.offset,
                                add_val,
                                iCon->cacheLKey,
                                remoteInfo[gaddr.nodeID].dmRKey[0],
                                mask,
                                true,
                                ctx->coro_id);
        (*ctx->yield)(*ctx->master);
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
        pollWithCQ(iCon->cq, 1, &wc);
    }
}

uint64_t DSM::poll_rdma_cq(int count)
{
    ibv_wc wc;
    // dinfo("Polling cq %p", iCon->cq);
    pollWithCQ(iCon->cq, count, &wc);

    return wc.wr_id;
}

bool DSM::poll_rdma_cq_once(uint64_t &wr_id)
{
    ibv_wc wc;
    int res = pollOnce(iCon->cq, 1, &wc);

    wr_id = wc.wr_id;

    return res == 1;
}
ibv_mw *DSM::alloc_mw()
{
    struct RdmaContext *ctx = &iCon->ctx;
    struct ibv_mw *mw = ibv_alloc_mw(ctx->pd, ctx->mw_type);
    if (!mw)
    {
        perror("failed to create memory window.");
    }
    return mw;
}

void DSM::free_mw(struct ibv_mw *mw)
{
    if (ibv_dealloc_mw(mw))
    {
        perror("failed to destroy memory window");
    }
}

void DSM::bind_memory_region(struct ibv_mw *mw,
                             const char *buffer,
                             size_t size,
                             size_t target_node_id)
{
    // dinfo("iCon->QPS[%lu][%lu]. accessing[0][1]. iCon @%p", iCon->QPs.size(),
    // iCon->QPs[0].size(), iCon);
    rdmaAsyncBindMemoryWindow(iCon->QPs[0][target_node_id],
                              mw,
                              iCon->cacheMR,
                              (uint64_t) buffer,
                              size,
                              true);
}