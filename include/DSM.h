#ifndef __DSM_H__
#define __DSM_H__

#include <atomic>

#include "Cache.h"
#include "Config.h"
#include "Connection.h"
#include "DSMKeeper.h"
#include "GlobalAddress.h"
#include "LocalAllocator.h"
#include "RdmaBuffer.h"

class DSMKeeper;
class Directory;

struct Identify
{
    int thread_id;
    int node_id;
};

class DSM
{
public:
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

    // RDMA operations
    // buffer is registered memory

    // TODO(Bin Yan): finish the rkey_* API for memory windows.
    void rkey_read(uint32_t rkey,
                   char *buffer,
                   GlobalAddress gaddr,
                   size_t size,
                   bool signal = true,
                   CoroContext *ctx = nullptr);
    void rkey_read_sync(uint32_t rkey,
                        char *buffer,
                        GlobalAddress gaddr,
                        size_t size,
                        CoroContext *ctx = nullptr);
    void read(char *buffer,
              GlobalAddress gaddr,
              size_t size,
              bool signal = true,
              CoroContext *ctx = nullptr);
    void read_sync(char *buffer,
                   GlobalAddress gaddr,
                   size_t size,
                   CoroContext *ctx = nullptr);
    void rkey_write(uint32_t rkey,
                    const char *buffer,
                    GlobalAddress gaddr,
                    size_t size,
                    bool signal = true,
                    CoroContext *ctx = nullptr);
    void rkey_write_sync(uint32_t rkey,
                         const char *buffer,
                         GlobalAddress gaddr,
                         size_t size,
                         CoroContext *ctx = nullptr);
    void write(const char *buffer,
               GlobalAddress gaddr,
               size_t size,
               bool signal = true,
               CoroContext *ctx = nullptr);
    void write_sync(const char *buffer,
                    GlobalAddress gaddr,
                    size_t size,
                    CoroContext *ctx = nullptr);

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
    ibv_mw *alloc_mw();
    void free_mw(struct ibv_mw *mw);
    bool bind_memory_region(struct ibv_mw *mw,
                            size_t target_node_id,
                            size_t target_thread_id,
                            const char *buffer,
                            size_t size);
    bool bind_memory_region_sync(struct ibv_mw *mw,
                                 size_t target_node_id,
                                 size_t target_thread_id,
                                 const char *buffer,
                                 size_t size);

    uint64_t poll_rdma_cq(int count = 1);
    bool poll_rdma_cq_once(uint64_t &wr_id);

    uint64_t sum(uint64_t value)
    {
        static uint64_t count = 0;
        return keeper->sum(std::string("sum-") + std::to_string(count++),
                           value);
    }

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

private:
    void initRDMAConnection();
    void fill_keys_dest(RdmaOpRegion &ror, GlobalAddress addr, bool is_chip);

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

    // RemoteConnection *remoteInfo;
    std::vector<RemoteConnection> remoteInfo;
    std::vector<ThreadConnection> thCon;
    std::vector<DirectoryConnection> dirCon;
    std::unique_ptr<DSMKeeper> keeper;

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
     *
     * @return char*
     */
    char *get_rdma_buffer()
    {
        return rdma_buffer;
    }
    Buffer get_server_internal_buffer();
    RdmaBuffer &get_rbuf(int coro_id)
    {
        dcheck(coro_id < define::kMaxCoro,
               "coro_id should be < define::kMaxCoro");
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
     */
    void send(const char *buf,
              size_t size,
              uint16_t node_id,
              uint16_t dir_id = 0)
    {
        auto buffer = (RawMessage *) iCon->message->getSendPool();
        buffer->node_id = myNodeID;
        buffer->app_id = thread_id;
        check(size < sizeof(RawMessage::inlined_buffer));
        memcpy(buffer->inlined_buffer, buf, size);
        iCon->sendMessage2Dir(buffer, node_id, dir_id);
    }
    char *recv()
    {
        check(dirCon.size() == 1);
        struct ibv_wc wc;
        pollWithCQ(dirCon[0].rpc_cq, 1, &wc);
        switch (int(wc.opcode))
        {
        case IBV_WC_RECV:
        {
            auto *m = (RawMessage *) dirCon[0].message->getMessage();
            return m->inlined_buffer;
        }
        default:
        {
            assert(false);
        }
        }
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
