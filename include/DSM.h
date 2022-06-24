#pragma once
#ifndef __DSM_H__
#define __DSM_H__

#include <glog/logging.h>

#include <atomic>
#include <cstring>

#include "Cache.h"
#include "ClockManager.h"
#include "Config.h"
#include "Connection.h"
#include "CoroContext.h"
#include "DSMKeeper.h"
#include "GlobalAddress.h"
#include "LocalAllocator.h"
#include "Pool.h"
#include "RdmaBuffer.h"
#include "util/Tracer.h"

class DSMKeeper;
class Directory;

struct Identify
{
    int thread_id;
    int node_id;
};

// what we get from a DSM::registerThread
struct ThreadResourceDesc
{
    uint64_t thread_id{0};
    uint64_t thread_name_id{0};
    uint64_t thread_tag{0};
    ThreadConnection *icon{nullptr};
    char *rdma_buffer{nullptr};
    size_t rdma_buffer_size{0};
};

inline std::ostream &operator<<(std::ostream &os,
                                const ThreadResourceDesc &desc)
{
    os << "{ThreadResourceDesc tid: " << desc.thread_id
       << ", tag: " << desc.thread_tag << ", icon: " << (void *) desc.icon
       << ", rdma_buffer: " << (void *) desc.rdma_buffer;
    return os;
}

class DSM : public std::enable_shared_from_this<DSM>
{
public:
    using pointer = std::shared_ptr<DSM>;
    using WcErrHandler = WcErrHandler;

    ThreadResourceDesc prepareThread();
    ThreadResourceDesc getCurrentThreadDesc();
    bool applyResource(const ThreadResourceDesc &, bool bind_core);
    bool registerThread();
    bool hasRegistered() const;

    static std::shared_ptr<DSM> getInstance(const DSMConfig &conf);

    uint16_t getMyNodeID() const
    {
        return myNodeID;
    }
    uint16_t getMyThreadID() const
    {
        return thread_id_;
    }
    uint16_t getMyThreadNameID() const
    {
        return thread_name_id_;
    }
    uint16_t getClusterSize() const
    {
        return conf.machineNR;
    }
    uint64_t getThreadTag() const
    {
        return thread_tag_;
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
            uint64_t digest = util::djb2_digest((char *) &ex, sizeof(ex));
            LOG(INFO) << "[boot-meta] node: " << i << " digest: " << std::hex
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
    inline void read(char *buffer,
                     GlobalAddress gaddr,
                     size_t size,
                     bool signal = true,
                     CoroContext *ctx = nullptr,
                     uint64_t wr_id = 0);
    inline bool read_sync(char *buffer,
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
    inline void write(const char *buffer,
                      GlobalAddress gaddr,
                      size_t size,
                      bool signal = true,
                      CoroContext *ctx = nullptr,
                      uint64_t wr_id = 0);
    inline bool write_sync(const char *buffer,
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

    void rkey_cas(uint32_t rkey,
                  char *rdma_buffer,
                  GlobalAddress gaddr,
                  size_t dir_id,
                  uint64_t compare,
                  uint64_t swap,
                  bool is_signal,
                  uint64_t wr_id,
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
    inline ibv_qp *get_dir_qp(int node_id, int thread_id, size_t dirID);
    inline ibv_cq *get_dir_cq(size_t dirID);
    inline ibv_qp *get_th_qp(int node_id, size_t dirID);
    inline ibv_mr *get_dir_mr(size_t dirID);
    inline ibv_mr *get_dm_mr()
    {
        return dirCon.front()->lockMR;
    }
    Buffer get_dm()
    {
        char *dm_addr = (char *) dirCon.front()->dmPool;
        size_t dm_size = dirCon.front()->lockSize;
        return {dm_addr, dm_size};
    }
    inline RdmaContext *get_rdma_context(size_t dirID);
    inline uint32_t get_rkey(size_t node_id, size_t dir_id);

    void cas(GlobalAddress gaddr,
             uint64_t equal,
             uint64_t val,
             uint64_t *rdma_buffer,
             bool signal,
             uint64_t wr_id,
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
                                 CoroContext *ctx = nullptr);

    /**
     * @brief poll rdma cq
     *
     * @param buf buffer of at least sizeof(ibv_wc) * @limit length
     * @param limit
     * @return size_t the number of wc actually polled
     */
    inline size_t try_poll_rdma_cq(ibv_wc *buf, size_t limit);
    inline uint64_t poll_rdma_cq(int count = 1);
    inline bool poll_rdma_cq_once(uint64_t &wr_id);
    inline int poll_dir_cq(size_t dirID, size_t count);
    inline size_t try_poll_dir_cq(ibv_wc *buf, size_t dirID, size_t limit);

    uint64_t sum(uint64_t value)
    {
        static uint64_t count = 0;
        return keeper->sum(std::string("sum-") + std::to_string(count++),
                           value);
    }
    // clang-format off
    /**
     * The layout of buffer:
     * [dsm_reserve_size()] [user_reserve_size()] [buffer_size()]
     * ^-- dsm_base()                             ^-- get_server_buffer();
     *                      ^-- get_server_reserved_buffer()
     * ^-- get_server_internal_dsm_buffer()
     * 
     * DSM offset:
     *                      |<---------------------------->|
     * call dsm_offset_to_addr(dsm_offset) / addr_to_dsm_offset(addr)
     * 
     * Buffer offset:
     *                                             |<----->|
     * call buffer_offset_to_addr(buf_offset) / addr_to_buffer_offset(addr)
     * 
     * The DSM reserved region:
     * |<---------------->|
     * It is reserved by DSM. Will never expose to the outer world
     * 
     */
    // clang-format on
    size_t dsm_reserve_size() const
    {
        // an area for ExchangeMeta data
        size_t reserve_for_bootstrap = getClusterSize() * sizeof(ExchangeMeta);
        reserve_for_bootstrap = ROUND_UP(reserve_for_bootstrap, 4096);
        return reserve_for_bootstrap;
    }
    size_t user_reserve_size() const
    {
        return conf.dsmReserveSize;
    }
    size_t total_reserve_size() const
    {
        auto dsm_reserve = dsm_reserve_size();
        auto user_reserve = user_reserve_size();
        auto reserve = dsm_reserve + user_reserve;

        DCHECK_LT(reserve, total_dsm_buffer_size());
        return reserve;
    }
    size_t buffer_size() const
    {
        return conf.dsmSize;
    }
    size_t total_dsm_buffer_size() const
    {
        DCHECK_EQ(baseAddrSize,
                  dsm_reserve_size() + user_reserve_size() + buffer_size());
        return baseAddrSize;
    }

    uint64_t gaddr_to_addr(const GlobalAddress &gaddr)
    {
        auto base =
            (uint64_t) remoteInfo[gaddr.nodeID].dsmBase + dsm_reserve_size();
        return base + gaddr.offset;
    }
    void *dsm_offset_to_addr(uint64_t offset)
    {
        return (char *) dsm_base() + dsm_reserve_size() + offset;
    }
    uint64_t addr_to_dsm_offset(void *addr) const
    {
        auto exposed_base = baseAddr + dsm_reserve_size();
        CHECK_GE((void *) addr, (void *) exposed_base) << "** addr underflow";
        return (uint64_t) addr - exposed_base;
    }
    void *buffer_offset_to_addr(uint64_t offset) const
    {
        return (char *) dsm_base() + offset + total_reserve_size();
    }
    uint64_t addr_to_buffer_offset(void *addr) const
    {
        void *base = (char *) dsm_base() + total_reserve_size();
        CHECK_GE(addr, base);
        return (char *) addr - (char *) base;
    }
    uint64_t buffer_offset_to_dsm_offset(uint64_t buf_offset) const
    {
        return buf_offset + user_reserve_size();
    }
    uint64_t dsm_offset_to_buffer_offset(uint64_t dsm_offset) const
    {
        CHECK_GE(dsm_offset, user_reserve_size());
        return dsm_offset - user_reserve_size();
    }

    bool valid_buffer_offset(uint64_t buffer_offset) const
    {
        return buffer_offset < buffer_size();
    }
    bool valid_buffer_addr(void *addr) const
    {
        return valid_buffer_offset(addr_to_buffer_offset(addr));
    }
    bool valid_dsm_offset(uint64_t dsm_offset) const
    {
        return dsm_offset < user_reserve_size() + buffer_size();
    }
    bool valid_dsm_addr(void *addr) const
    {
        return valid_dsm_offset(addr_to_dsm_offset(addr));
    }

    ExchangeMeta &getExchangeMetaBootstrap(size_t node_id) const;

    // Memcached operations for sync
    template <typename T>
    void put(const std::string &key,
             const std::string &value,
             const T &sleep_time)
    {
        auto actual_key = "__DSM:" + key;
        keeper->memSet(actual_key.c_str(),
                       actual_key.size(),
                       value.c_str(),
                       value.size(),
                       sleep_time);
    }
    template <typename T>
    std::string try_get(const std::string &key, const T &sleep_time)
    {
        auto actual_key = "__DSM:" + key;
        size_t size = 0;
        auto *ret = keeper->memTryGet(
            actual_key.c_str(), actual_key.size(), &size, sleep_time);
        std::string value;
        value.resize(size);
        memcpy(value.data(), ret, size);
        ::free(ret);
        return value;
    }
    template <typename T>
    std::string get(const std::string &key, const T &sleep_time)
    {
        auto actual_key = "__DSM:" + key;
        size_t size = 0;
        auto *ret = keeper->memGet(
            actual_key.c_str(), actual_key.size(), &size, sleep_time);
        std::string value;
        value.resize(size);
        memcpy(value.data(), ret, size);
        ::free(ret);
        return value;
    }
    template <typename Duration>
    void keeper_barrier(const std::string &key, Duration sleep_time)
    {
        VLOG(1) << "[DSM] Entering Barrier " << key;
        keeper->barrier(key, sleep_time);
        VLOG(1) << "[DSM] Leaving Barrier " << key;
    }
    template <typename Duration>
    void keeper_partial_barrier(const std::string &key,
                                size_t expect_nr,
                                bool is_master,
                                Duration sleep_time)
    {
        VLOG(1) << "[DSM] Entering Barrier " << key << " (expect " << expect_nr
                << ", is_master: " << is_master << ")";
        keeper->partial_barrier(key, expect_nr, is_master, sleep_time);
        VLOG(1) << "[DSM] Leaving Barrier " << key << " (expect " << expect_nr
                << ", is_master: " << is_master << ")";
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
        return thread_id_;
    }
    int get_thread_name_id() const
    {
        return thread_name_id_;
    }
    Identify get_identify() const
    {
        Identify id;
        id.node_id = get_node_id();
        id.thread_id = get_thread_id();
        return id;
    }
    bool recoverDirQP(int node_id, int thread_id, size_t dirID);
    bool recoverThreadQP(int node_id,
                         size_t dirID,
                         util::TraceView v = util::nulltrace);

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
    bool unreliable_prepare_send(const char *buf,
                                 size_t size,
                                 uint16_t node_id,
                                 size_t dir_id)
    {
        DCHECK(false) << "Empirically: batching does not make performance "
                         "better. Please do not use me.";
        return umsg_->prepare_send(get_thread_id(), buf, size, node_id, dir_id);
    }
    void unreliable_commit_send()
    {
        return umsg_->commit_send(get_thread_id());
    }

    void unreliable_send(const char *buf,
                         size_t size,
                         uint16_t node_id,
                         size_t dir_id)
    {
        return umsg_->send(get_thread_id(), buf, size, node_id, dir_id);
    }
    size_t unreliable_try_recv(char *ibuf, size_t limit = 1)
    {
        return umsg_->try_recv(get_thread_id(), ibuf, limit);
    }
    using umsg_ptr_t = UnreliableConnection<kMaxAppThread>::ptr_t;
    size_t unreliable_try_recv_no_cpy(umsg_ptr_t *ptr_buf, size_t limit = 1)
    {
        CHECK(false) << "[DSM] using no-copy is not safe. Not "
                        "finish the code for reuse protection.";
        return umsg_->try_recv_no_cpy(get_thread_id(), ptr_buf, limit);
    }
    void unreliable_recv(char *ibuf, size_t limit = 1)
    {
        return umsg_->recv(get_thread_id(), ibuf, limit);
    }
    inline uint32_t get_icon_lkey();

    inline bool modify_th_qp_access_flag(int node_id,
                                         size_t dir_id,
                                         uint64_t flags);
    inline bool modify_dir_qp_access_flag(size_t node_id,
                                          size_t thread_id,
                                          size_t dir_id,
                                          uint64_t flags);

    // below used as lease
private:
    inline bool modify_qp_access_flag(ibv_qp *, uint64_t flags);

    inline void *dsm_base() const
    {
        DCHECK_EQ(baseAddr, remoteInfo[get_node_id()].dsmBase);
        return (void *) baseAddr;
    }
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

    // thread_id_ is high coupled to the resources, i.e, QPs
    static thread_local int thread_id_;
    // thread name id is the id used for debugging. The unique identify of the
    // thread.
    static thread_local int thread_name_id_;
    static thread_local ThreadConnection *iCon_;
    static thread_local char *rdma_buffer_;
    static thread_local LocalAllocator local_allocator_;
    static thread_local RdmaBuffer rbuf_[define::kMaxCoroNr];
    static thread_local uint64_t thread_tag_;

    uint64_t baseAddr;
    uint64_t baseAddrSize;
    uint32_t myNodeID;

    std::vector<RemoteConnection> remoteInfo;
    std::vector<std::unique_ptr<ThreadConnection>> thCon;
    std::vector<std::unique_ptr<DirectoryConnection>> dirCon;
    std::unique_ptr<DSMKeeper> keeper;

    // if NR_DIRECTORY is not 1, there is multiple dir to use.
    // this val chooses the current in-use dir.
    std::atomic<size_t> cur_dir_{0};

    // ClockManager clock_manager_;
    std::unique_ptr<UnreliableConnection<kMaxAppThread>> umsg_;

public:
    uint64_t get_base_addr() const
    {
        return baseAddr;
    }
    uint64_t get_base_size() const
    {
        return baseAddrSize;
    }
    bool is_register()
    {
        return thread_id_ != -1;
    }
    /**
     * @brief The per-thread temporary buffer for client.
     * The thread must call DSM::registerThread() before calling this method.
     *
     * @return char*
     */
    Buffer get_rdma_buffer()
    {
        return Buffer(rdma_buffer_, define::kRDMABufferSize);
    }
    inline Buffer get_server_internal_dsm_buffer();
    inline Buffer get_server_reserved_buffer();
    inline Buffer get_server_buffer();
    RdmaBuffer &get_rbuf(coro_t coro_id)
    {
        DCHECK(coro_id < define::kMaxCoroNr)
            << "coro_id should be < define::kMaxCoroNr";
        return rbuf_[coro_id];
    }

    GlobalAddress alloc(size_t size);
    void free(GlobalAddress addr);

    size_t get_icon_nr() const
    {
        return thCon.size();
    }

    void rpc_call_dir(const RawMessage &m,
                      uint16_t node_id,
                      uint16_t dir_id = 0)
    {
        auto buffer = (RawMessage *) iCon_->message->getSendPool();

        memcpy(buffer, &m, sizeof(RawMessage));
        buffer->node_id = myNodeID;
        buffer->app_id = thread_id_;

        iCon_->sendMessage2Dir(buffer, node_id, dir_id);
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
        auto buffer = (RawMessage *) iCon_->message->getSendPool();
        buffer->node_id = myNodeID;
        buffer->app_id = thread_id_;
        CHECK_LT(size, sizeof(RawMessage::inlined_buffer));
        memcpy(buffer->inlined_buffer, buf, size);
        iCon_->sendMessage2Dir(buffer, node_id, dir_id, sync);
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

        pollWithCQ(iCon_->rpc_cq, 1, &wc);
        return (RawMessage *) iCon_->message->getMessage();
    }

    void explain()
    {
        auto dsm_buffer = get_server_internal_dsm_buffer();
        auto resv_buffer = get_server_reserved_buffer();
        auto int_buffer = get_server_buffer();
        LOG(INFO) << "[DSM] node_id: " << get_node_id()
                  << ", dsm_internal_base: " << (void *) baseAddr
                  << ", dsm_internal_len: " << baseAddrSize
                  << ", dsm reserved buffer: " << dsm_buffer
                  << ", user reserved buffer: " << resv_buffer
                  << ", DSM buffer: " << int_buffer;
        CHECK_GE((uint64_t) dsm_buffer.buffer, baseAddr);
        CHECK_GE(resv_buffer.buffer, dsm_buffer.buffer);
        CHECK_GE(int_buffer.buffer, resv_buffer.buffer);
        CHECK_EQ(int_buffer.buffer - resv_buffer.buffer, resv_buffer.size);
        CHECK_EQ(resv_buffer.buffer - dsm_buffer.buffer, dsm_buffer.size);
        CHECK_EQ(resv_buffer.size + int_buffer.size + dsm_buffer.size,
                 baseAddrSize);
    }
};

ibv_qp *DSM::get_dir_qp(int node_id, int thread_id, size_t dirID)
{
    DCHECK_LT(dirID, dirCon.size());
    return dirCon[dirID]->QPs[thread_id][node_id];
}
ibv_cq *DSM::get_dir_cq(size_t dirID)
{
    DCHECK_LT(dirID, dirCon.size());
    return dirCon[dirID]->cq;
}
ibv_qp *DSM::get_th_qp(int node_id, size_t dirID)
{
    DCHECK_LT(dirID, iCon_->QPs.size());
    return iCon_->QPs[dirID][node_id];
}
uint32_t DSM::get_rkey(size_t node_id, size_t dir_id)
{
    return remoteInfo[node_id].dsmRKey[dir_id];
}
ibv_mr *DSM::get_dir_mr(size_t dirID)
{
    DCHECK_LT(dirID, dirCon.size());
    return dirCon[dirID]->dsmMR;
}
RdmaContext *DSM::get_rdma_context(size_t dirID)
{
    DCHECK_LT(dirID, dirCon.size());
    return &dirCon[dirID]->ctx;
}

inline uint32_t DSM::get_icon_lkey()
{
    return DCHECK_NOTNULL(iCon_)->cacheLKey;
}

void DSM::read(char *buffer,
               GlobalAddress gaddr,
               size_t size,
               bool signal,
               CoroContext *ctx,
               uint64_t wr_id)
{
    size_t dirID = get_cur_dir();
    uint32_t rkey = remoteInfo[gaddr.nodeID].dsmRKey[dirID];
    rkey_read(rkey, buffer, gaddr, size, dirID, signal, ctx, wr_id);
}

bool DSM::read_sync(char *buffer,
                    GlobalAddress gaddr,
                    size_t size,
                    CoroContext *ctx,
                    uint64_t wr_id,
                    const WcErrHandler &handler)
{
    size_t dirID = get_cur_dir();
    uint32_t rkey = remoteInfo[gaddr.nodeID].dsmRKey[dirID];
    return rkey_read_sync(
        rkey, buffer, gaddr, size, dirID, ctx, wr_id, handler);
}

void DSM::write(const char *buffer,
                GlobalAddress gaddr,
                size_t size,
                bool signal,
                CoroContext *ctx,
                uint64_t wc_id)
{
    size_t dirID = get_cur_dir();
    uint32_t rkey = remoteInfo[gaddr.nodeID].dsmRKey[dirID];
    return rkey_write(rkey, buffer, gaddr, size, dirID, signal, ctx, wc_id);
}

bool DSM::write_sync(const char *buffer,
                     GlobalAddress gaddr,
                     size_t size,
                     CoroContext *ctx,
                     uint64_t wc_id,
                     const WcErrHandler &handler)
{
    size_t dirID = get_cur_dir();
    uint32_t rkey = remoteInfo[gaddr.nodeID].dsmRKey[dirID];
    return rkey_write_sync(
        rkey, buffer, gaddr, size, dirID, ctx, wc_id, handler);
}
Buffer DSM::get_server_internal_dsm_buffer()
{
    size_t buffer_len = dsm_reserve_size();
    Buffer ret((char *) dsm_base(), buffer_len);
    return ret;
}
Buffer DSM::get_server_buffer()
{
    size_t rv = total_reserve_size();
    Buffer ret((char *) dsm_base() + rv, buffer_size());
    return ret;
}
Buffer DSM::get_server_reserved_buffer()
{
    size_t dsm_rv = dsm_reserve_size();
    size_t buffer_len = user_reserve_size();
    Buffer ret((char *) dsm_base() + dsm_rv, buffer_len);
    return ret;
}

size_t DSM::try_poll_rdma_cq(ibv_wc *buf, size_t limit)
{
    return ibv_poll_cq(iCon_->cq, limit, buf);
}

uint64_t DSM::poll_rdma_cq(int count)
{
    ibv_wc wc;
    // dinfo("Polling cq %p", iCon_->cq);
    pollWithCQ(iCon_->cq, count, &wc);

    return wc.wr_id;
}

size_t DSM::try_poll_dir_cq(ibv_wc *wcs, size_t dirID, size_t limit)
{
    DCHECK_LT(dirID, dirCon.size());
    return ibv_poll_cq(dirCon[dirID]->cq, limit, wcs);
}

int DSM::poll_dir_cq(size_t dirID, size_t count)
{
    ibv_wc wc;
    DCHECK_LT(dirID, dirCon.size());
    return pollWithCQ(dirCon[dirID]->cq, count, &wc);
}

bool DSM::poll_rdma_cq_once(uint64_t &wr_id)
{
    ibv_wc wc;
    int res = pollOnce(iCon_->cq, 1, &wc);

    wr_id = wc.wr_id;

    return res == 1;
}

inline GlobalAddress DSM::alloc(size_t size)
{
    thread_local int next_target_node =
        (getMyThreadID() + getMyNodeID()) % conf.machineNR;
    thread_local int next_target_dir_id =
        (getMyThreadID() + getMyNodeID()) % NR_DIRECTORY;

    bool need_chunk = false;
    auto addr = local_allocator_.malloc(size, need_chunk);
    if (need_chunk)
    {
        RawMessage m;
        m.type = RpcType::MALLOC;

        this->rpc_call_dir(m, next_target_node, next_target_dir_id);
        local_allocator_.set_chunck(rpc_wait()->addr);

        if (++next_target_dir_id == NR_DIRECTORY)
        {
            next_target_node = (next_target_node + 1) % conf.machineNR;
            next_target_dir_id = 0;
        }

        // retry
        addr = local_allocator_.malloc(size, need_chunk);
    }

    return addr;
}

inline void DSM::free(GlobalAddress addr)
{
    local_allocator_.free(addr);
}

bool DSM::modify_th_qp_access_flag(int node_id, size_t dir_id, uint64_t f)
{
    auto *qp = get_th_qp(node_id, dir_id);
    return modify_qp_access_flag(qp, f);
}
bool DSM::modify_dir_qp_access_flag(size_t node_id,
                                    size_t thread_id,
                                    size_t dir_id,
                                    uint64_t f)
{
    auto *qp = get_dir_qp(node_id, thread_id, dir_id);
    return modify_qp_access_flag(qp, f);
}

bool DSM::modify_qp_access_flag(ibv_qp *qp, uint64_t flags)
{
    ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_access_flags = flags;
    auto ret = ibv_modify_qp(qp, &attr, IBV_QP_ACCESS_FLAGS);
    if (ret)
    {
        DLOG(WARNING) << "[DSM} failed to modify qp access flag. QP: " << qp
                      << ", flags: " << (uint64_t) flags;
        return false;
    }
    return true;
}

#endif /* __DSM_H__ */
