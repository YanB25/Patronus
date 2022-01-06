#ifndef __DSM_H__
#define __DSM_H__

#include <glog/logging.h>

#include <atomic>

#include "Cache.h"
#include "ClockManager.h"
#include "Config.h"
#include "Connection.h"
#include "DSMKeeper.h"
#include "GlobalAddress.h"
#include "LocalAllocator.h"
#include "Pool.h"
#include "RdmaBuffer.h"

class DSMKeeper;
class Directory;

struct Identify
{
    int thread_id;
    int node_id;
};

class DSM : public std::enable_shared_from_this<DSM>
{
public:
    using pointer = std::shared_ptr<DSM>;
    using WcErrHandler = WcErrHandler;

    void registerThread();
    static std::shared_ptr<DSM> getInstance(const DSMConfig &conf);

    uint16_t getMyNodeID()
    {
        return myNodeID;
    }
    uint16_t getMyThreadID()
    {
        return thread_id;
    }
    uint16_t getClusterSize()
    {
        return conf.machineNR;
    }
    uint64_t getThreadTag()
    {
        return thread_tag;
    }

    void syncMetadataBootstrap(const ExchangeMeta &, size_t remoteID);

    bool reconnectThreadToDir(size_t node_id, size_t dirID);

    /**
     * @brief Reinitialize the DirectoryConnection
     *
     * This function is helpful to dealloc the whole PD and re-init the Dir from
     * the ground. After called, all the peers should be notified to call
     * threadReconnectDir.
     *
     * @param dirID
     * @return true
     * @return false
     */
    bool reinitializeDir(size_t dirID);

    void debug_show_exchanges()
    {
        for (size_t i = 0; i < getClusterSize(); ++i)
        {
            auto ex = getExchangeMetaBootstrap(i);
            uint64_t digest = djb2_digest((char *) &ex, sizeof(ex));
            LOG(INFO) << "[debug] node: " << i << " digest: " << std::hex
                      << digest;
        }
    }

    // RDMA operations
    // buffer is registered memory

    void rkey_read(uint32_t rkey,
                   char *buffer,
                   GlobalAddress gaddr,
                   size_t size,
                   size_t dirID,
                   bool signal = true,
                   CoroContext *ctx = nullptr,
                   uint64_t wr_id = 0);
    bool rkey_read_sync(uint32_t rkey,
                        char *buffer,
                        GlobalAddress gaddr,
                        size_t size,
                        size_t dirID,
                        CoroContext *ctx = nullptr,
                        uint64_t wr_id = 0,
                        const WcErrHandler &handler = empty_wc_err_handler);
    void read(char *buffer,
              GlobalAddress gaddr,
              size_t size,
              bool signal = true,
              CoroContext *ctx = nullptr,
              uint64_t wr_id = 0);
    bool read_sync(char *buffer,
                   GlobalAddress gaddr,
                   size_t size,
                   CoroContext *ctx = nullptr,
                   uint64_t wr_id = 0,
                   const WcErrHandler &handler = empty_wc_err_handler);
    void rkey_write(uint32_t rkey,
                    const char *buffer,
                    GlobalAddress gaddr,
                    size_t size,
                    size_t dirID,
                    bool signal = true,
                    CoroContext *ctx = nullptr,
                    uint64_t wr_id = 0);
    bool rkey_write_sync(uint32_t rkey,
                         const char *buffer,
                         GlobalAddress gaddr,
                         size_t size,
                         size_t dirID,
                         CoroContext *ctx = nullptr,
                         uint64_t wr_id = 0,
                         const WcErrHandler &handler = empty_wc_err_handler);
    void write(const char *buffer,
               GlobalAddress gaddr,
               size_t size,
               bool signal = true,
               CoroContext *ctx = nullptr,
               uint64_t wr_id = 0);
    bool write_sync(const char *buffer,
                    GlobalAddress gaddr,
                    size_t size,
                    CoroContext *ctx = nullptr,
                    uint64_t wc_id = 0,
                    const WcErrHandler &handler = empty_wc_err_handler);

    void write_batch(RdmaOpRegion *rs,
                     int k,
                     bool signal = true,
                     CoroContext *ctx = nullptr);
    void write_batch_sync(RdmaOpRegion *rs, int k, CoroContext *ctx = nullptr);

    void write_faa(RdmaOpRegion &write_ror,
                   RdmaOpRegion &faa_ror,
                   uint64_t add_val,
                   bool signal = true,
                   CoroContext *ctx = nullptr);
    void write_faa_sync(RdmaOpRegion &write_ror,
                        RdmaOpRegion &faa_ror,
                        uint64_t add_val,
                        CoroContext *ctx = nullptr);

    void write_cas(RdmaOpRegion &write_ror,
                   RdmaOpRegion &cas_ror,
                   uint64_t equal,
                   uint64_t val,
                   bool signal = true,
                   CoroContext *ctx = nullptr);
    void write_cas_sync(RdmaOpRegion &write_ror,
                        RdmaOpRegion &cas_ror,
                        uint64_t equal,
                        uint64_t val,
                        CoroContext *ctx = nullptr);
    ibv_qp *get_dir_qp(int node_id, int thread_id, size_t dirID);
    ibv_qp *get_th_qp(int node_id, size_t dirID);

    void cas(GlobalAddress gaddr,
             uint64_t equal,
             uint64_t val,
             uint64_t *rdma_buffer,
             bool signal = true,
             CoroContext *ctx = nullptr);
    bool cas_sync(GlobalAddress gaddr,
                  uint64_t equal,
                  uint64_t val,
                  uint64_t *rdma_buffer,
                  CoroContext *ctx = nullptr);

    void cas_read(RdmaOpRegion &cas_ror,
                  RdmaOpRegion &read_ror,
                  uint64_t equal,
                  uint64_t val,
                  bool signal = true,
                  CoroContext *ctx = nullptr);
    bool cas_read_sync(RdmaOpRegion &cas_ror,
                       RdmaOpRegion &read_ror,
                       uint64_t equal,
                       uint64_t val,
                       CoroContext *ctx = nullptr);

    void cas_mask(GlobalAddress gaddr,
                  uint64_t equal,
                  uint64_t val,
                  uint64_t *rdma_buffer,
                  uint64_t mask = ~(0ull),
                  bool signal = true);
    bool cas_mask_sync(GlobalAddress gaddr,
                       uint64_t equal,
                       uint64_t val,
                       uint64_t *rdma_buffer,
                       uint64_t mask = ~(0ull));

    void faa_boundary(GlobalAddress gaddr,
                      uint64_t add_val,
                      uint64_t *rdma_buffer,
                      uint64_t mask = 63,
                      bool signal = true,
                      CoroContext *ctx = nullptr);
    void faa_boundary_sync(GlobalAddress gaddr,
                           uint64_t add_val,
                           uint64_t *rdma_buffer,
                           uint64_t mask = 63,
                           CoroContext *ctx = nullptr);

    // for on-chip device memory
    void read_dm(char *buffer,
                 GlobalAddress gaddr,
                 size_t size,
                 bool signal = true,
                 CoroContext *ctx = nullptr);
    void read_dm_sync(char *buffer,
                      GlobalAddress gaddr,
                      size_t size,
                      CoroContext *ctx = nullptr);

    void write_dm(const char *buffer,
                  GlobalAddress gaddr,
                  size_t size,
                  bool signal = true,
                  CoroContext *ctx = nullptr);
    void write_dm_sync(const char *buffer,
                       GlobalAddress gaddr,
                       size_t size,
                       CoroContext *ctx = nullptr);

    void cas_dm(GlobalAddress gaddr,
                uint64_t equal,
                uint64_t val,
                uint64_t *rdma_buffer,
                bool signal = true,
                CoroContext *ctx = nullptr);
    bool cas_dm_sync(GlobalAddress gaddr,
                     uint64_t equal,
                     uint64_t val,
                     uint64_t *rdma_buffer,
                     CoroContext *ctx = nullptr);

    void cas_dm_mask(GlobalAddress gaddr,
                     uint64_t equal,
                     uint64_t val,
                     uint64_t *rdma_buffer,
                     uint64_t mask = ~(0ull),
                     bool signal = true);
    bool cas_dm_mask_sync(GlobalAddress gaddr,
                          uint64_t equal,
                          uint64_t val,
                          uint64_t *rdma_buffer,
                          uint64_t mask = ~(0ull));

    void faa_dm_boundary(GlobalAddress gaddr,
                         uint64_t add_val,
                         uint64_t *rdma_buffer,
                         uint64_t mask = 63,
                         bool signal = true,
                         CoroContext *ctx = nullptr);
    void faa_dm_boundary_sync(GlobalAddress gaddr,
                              uint64_t add_val,
                              uint64_t *rdma_buffer,
                              uint64_t mask = 63,
                              CoroContext *ctx = nullptr);
    ibv_mw *alloc_mw(size_t dirID);
    void free_mw(struct ibv_mw *mw);
    bool bind_memory_region(struct ibv_mw *mw,
                            size_t target_node_id,
                            size_t target_thread_id,
                            const char *buffer,
                            size_t size,
                            size_t dirID,
                            size_t wr_id,
                            bool signal);
    bool bind_memory_region_sync(struct ibv_mw *mw,
                                 size_t target_node_id,
                                 size_t target_thread_id,
                                 const char *buffer,
                                 size_t size,
                                 size_t dirID,
                                 uint64_t wr_id,
                                 CoroContext* ctx = nullptr);

    /**
     * @brief poll rdma cq
     *
     * @param buf buffer of at least sizeof(ibv_wc) * @limit length
     * @param limit
     * @return size_t the number of wc actually polled
     */
    size_t try_poll_rdma_cq(ibv_wc *buf, size_t limit);
    uint64_t poll_rdma_cq(int count = 1);
    bool poll_rdma_cq_once(uint64_t &wr_id);
    int poll_dir_cq(size_t dirID, size_t count);
    size_t try_poll_dir_cq(ibv_wc *buf, size_t dirID, size_t limit);

    uint64_t sum(uint64_t value)
    {
        static uint64_t count = 0;
        return keeper->sum(std::string("sum-") + std::to_string(count++),
                           value);
    }
    size_t server_internal_buffer_reserve_size()
    {
        size_t reserve = getClusterSize() * sizeof(ExchangeMeta);
        return ROUND_UP(reserve, 4096);
    }

    uint64_t dsm_pool_addr(const GlobalAddress &gaddr)
    {
        auto base = remoteInfo[gaddr.nodeID].dsmBase +
                    server_internal_buffer_reserve_size();
        return base + gaddr.offset;
    }

    ExchangeMeta &getExchangeMetaBootstrap(size_t node_id) const;

    // Memcached operations for sync
    size_t Put(uint64_t key, const void *value, size_t count)
    {
        std::string k = std::string("gam-") + std::to_string(key);
        keeper->memSet(k.c_str(), k.size(), (char *) value, count);
        return count;
    }

    size_t Get(uint64_t key, void *value)
    {
        std::string k = std::string("gam-") + std::to_string(key);
        size_t size;
        char *ret = keeper->memGet(k.c_str(), k.size(), &size);
        memcpy(value, ret, size);

        return size;
    }

    // ClockManager &clock_manager()
    // {
    //     return clock_manager_;
    // }

    DSM(const DSMConfig &conf);
    virtual ~DSM();

    size_t get_node_id() const
    {
        return keeper->getMyNodeID();
    }
    int get_thread_id() const
    {
        return thread_id;
    }
    Identify get_identify() const
    {
        Identify id;
        id.node_id = get_node_id();
        id.thread_id = get_thread_id();
        return id;
    }
    bool recoverDirQP(int node_id, int thread_id, size_t dirID);
    bool recoverThreadQP(int node_id, size_t dirID);

    void roll_dir()
    {
        cur_dir_ = (cur_dir_ + 1) % NR_DIRECTORY;
    }
    /**
     * @brief only call this if you know what you are doing.
     *
     * @param dir
     */
    void force_set_dir(size_t dir)
    {
        CHECK_LT(dir, dirCon.size());
        cur_dir_ = dir;
    }

    void reliable_send(const char *buf,
                       size_t size,
                       uint16_t node_id,
                       size_t targetID)
    {
        reliable_msg_->send(thread_id, buf, size, node_id, targetID);
    }
    void reliable_recv(size_t from_mid, char *ibuf, size_t limit = 1)
    {
        reliable_msg_->recv(from_mid, ibuf, limit);
    }
    size_t reliable_try_recv(size_t from_mid, char *ibuf, size_t limit = 1)
    {
        return reliable_msg_->try_recv(from_mid, ibuf, limit);
    }

    // below used as lease

private:
    void initRDMAConnection();
    void initExchangeMetadataBootstrap();
    void fill_keys_dest(RdmaOpRegion &ror,
                        GlobalAddress addr,
                        bool is_chip,
                        size_t dirID = 0);

    size_t get_cur_dir() const
    {
        return cur_dir_;
    }

    DSMConfig conf;
    std::atomic<int> appID{0};
    Cache cache;

    static thread_local int thread_id;
    static thread_local ThreadConnection *iCon;
    static thread_local char *rdma_buffer;
    static thread_local LocalAllocator local_allocator;
    static thread_local RdmaBuffer rbuf[define::kMaxCoro];
    static thread_local uint64_t thread_tag;

    uint64_t baseAddr;
    uint32_t myNodeID;

    std::vector<RemoteConnection> remoteInfo;
    std::vector<std::unique_ptr<ThreadConnection>> thCon;
    std::vector<std::unique_ptr<DirectoryConnection>> dirCon;
    std::unique_ptr<DSMKeeper> keeper;

    // if NR_DIRECTORY is not 1, there is multiple dir to use.
    // this val chooses the current in-use dir.
    std::atomic<size_t> cur_dir_{0};

    // ClockManager clock_manager_;
    std::unique_ptr<ReliableConnection> reliable_msg_;

public:
    bool is_register()
    {
        return thread_id != -1;
    }
    void barrier(const std::string &ss)
    {
        keeper->barrier(ss);
    }
    /**
     * @brief The per-thread temporary buffer for client.
     * The thread must call DSM::registerThread() before calling this method.
     *
     * @return char*
     */
    char *get_rdma_buffer()
    {
        return rdma_buffer;
    }
    Buffer get_server_internal_buffer();
    RdmaBuffer &get_rbuf(coro_t coro_id)
    {
        DCHECK(coro_id < define::kMaxCoro)
            << "coro_id should be < define::kMaxCoro";
        return rbuf[coro_id];
    }

    GlobalAddress alloc(size_t size);
    void free(GlobalAddress addr);

    void rpc_call_dir(const RawMessage &m,
                      uint16_t node_id,
                      uint16_t dir_id = 0)
    {
        auto buffer = (RawMessage *) iCon->message->getSendPool();

        memcpy(buffer, &m, sizeof(RawMessage));
        buffer->node_id = myNodeID;
        buffer->app_id = thread_id;

        iCon->sendMessage2Dir(buffer, node_id, dir_id);
    }
    /**
     * Do not use in the critical path.
     * A handy control path for sending/recving messages.
     * msg should be no longer than 32 byte.
     *
     * Do not interleave @sync=true and @sync=false.
     */
    void send(const char *buf,
              size_t size,
              uint16_t node_id,
              uint16_t dir_id = 0,
              bool sync = false)
    {
        auto buffer = (RawMessage *) iCon->message->getSendPool();
        buffer->node_id = myNodeID;
        buffer->app_id = thread_id;
        CHECK_LT(size, sizeof(RawMessage::inlined_buffer));
        memcpy(buffer->inlined_buffer, buf, size);
        iCon->sendMessage2Dir(buffer, node_id, dir_id, sync);
    }
    char *try_recv()
    {
        // size_t cur_dir = get_cur_dir();
        size_t cur_dir = 0;
        struct ibv_wc wc;
        ibv_cq *cq = dirCon[cur_dir]->rpc_cq;
        int ret = ibv_poll_cq(cq, 1, &wc);
        if (ret < 0)
        {
            LOG(ERROR) << "failed to poll cq. cq: " << cq << ". ret: " << ret;
            return nullptr;
        }
        if (ret == 1)
        {
            CHECK(wc.status == IBV_WC_SUCCESS);
            CHECK(wc.opcode == IBV_WC_RECV);
            auto *m = (RawMessage *) dirCon[cur_dir]->message->getMessage();
            return m->inlined_buffer;
        }
        return nullptr;
    }
    char *recv()
    {
        // size_t cur_dir = get_cur_dir();
        // dwarn("TODO: now always use the first as message dir.");
        size_t cur_dir = 0;

        struct ibv_wc wc;
        pollWithCQ(dirCon[cur_dir]->rpc_cq, 1, &wc);
        switch (int(wc.opcode))
        {
        case IBV_WC_RECV:
        {
            auto *m = (RawMessage *) dirCon[cur_dir]->message->getMessage();
            return m->inlined_buffer;
        }
        default:
        {
            assert(false);
        }
        }
        return nullptr;
    }

    RawMessage *rpc_wait()
    {
        ibv_wc wc;

        pollWithCQ(iCon->rpc_cq, 1, &wc);
        return (RawMessage *) iCon->message->getMessage();
    }
};

inline GlobalAddress DSM::alloc(size_t size)
{
    thread_local int next_target_node =
        (getMyThreadID() + getMyNodeID()) % conf.machineNR;
    thread_local int next_target_dir_id =
        (getMyThreadID() + getMyNodeID()) % NR_DIRECTORY;

    bool need_chunk = false;
    auto addr = local_allocator.malloc(size, need_chunk);
    if (need_chunk)
    {
        RawMessage m;
        m.type = RpcType::MALLOC;

        this->rpc_call_dir(m, next_target_node, next_target_dir_id);
        local_allocator.set_chunck(rpc_wait()->addr);

        if (++next_target_dir_id == NR_DIRECTORY)
        {
            next_target_node = (next_target_node + 1) % conf.machineNR;
            next_target_dir_id = 0;
        }

        // retry
        addr = local_allocator.malloc(size, need_chunk);
    }

    return addr;
}

inline void DSM::free(GlobalAddress addr)
{
    local_allocator.free(addr);
}
#endif /* __DSM_H__ */
