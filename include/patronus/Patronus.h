#pragma once
#ifndef PATRONUS_H_
#define PATRONUS_H_

#include <mutex>
#include <set>
#include <unordered_set>

#include "Common.h"
#include "DSM.h"
#include "Result.h"
#include "patronus/Batch.h"
#include "patronus/Config.h"
#include "patronus/Coro.h"
#include "patronus/DDLManager.h"
#include "patronus/Lease.h"
#include "patronus/LeaseContext.h"
#include "patronus/LockManager.h"
#include "patronus/ProtectionRegion.h"
#include "patronus/TimeSyncer.h"
#include "patronus/Type.h"
#include "patronus/memory/allocator.h"
#include "patronus/memory/slab_allocator.h"
#include "util/Debug.h"
#include "util/PerformanceReporter.h"
#include "util/RetCode.h"
#include "util/Tracer.h"

namespace patronus
{
using TraceView = util::TraceView;
using namespace define::literals;

class Patronus;
class ServerCoroBatchExecutionContext;
struct PatronusConfig
{
    size_t machine_nr{0};
    size_t lease_buffer_size{(kDSMCacheSize - 1_GB) / 2};
    size_t alloc_buffer_size{(kDSMCacheSize - 1_GB) / 2};
    size_t reserved_buffer_size{1_GB};
    // for sync time
    size_t time_parent_node_id{0};
    // for default allocator
    // see also @alloc_buffer_size
    std::vector<size_t> block_class{2_MB};
    std::vector<double> block_ratio{1};

    size_t total_buffer_size() const
    {
        return lease_buffer_size + alloc_buffer_size + reserved_buffer_size;
    }
};
inline std::ostream &operator<<(std::ostream &os, const PatronusConfig &conf)
{
    os << "{PatronusConfig machine_nr: " << conf.machine_nr
       << ", lease_buffer_size: " << conf.lease_buffer_size
       << ", alloc_buffer_size: " << conf.alloc_buffer_size
       << ", reserved_buffer_size: " << conf.reserved_buffer_size
       << ". (Total: " << conf.total_buffer_size() << ")"
       << "}";
    return os;
}

struct TraceContext
{
    uint64_t thread_name_id;
    uint32_t coro_id;
    RetrieveTimer timer;
};

struct RpcContext
{
    Lease *ret_lease{nullptr};
    BaseMessage *request{nullptr};
    std::atomic<bool> ready{false};
    size_t dir_id{0};
    RetCode ret_code{RC::kOk};
    char *buffer_addr{nullptr};  // for rpc_{read|write|cas}
};

struct RWContext
{
    ibv_wc_status wc_status{IBV_WC_SUCCESS};
    std::atomic<bool> ready{false};
    coro_t coro_id;
    size_t target_node;
    size_t dir_id;
    util::TraceView trace_view{util::nulltrace};
};

struct PatronusThreadResourceDesc
{
    ThreadResourceDesc dsm_desc;
    std::unique_ptr<ThreadUnsafeBufferPool<config::patronus::kMessageSize>>
        rdma_message_buffer_pool;
    char *client_rdma_buffer;
    size_t client_rdma_buffer_size;
    std::unique_ptr<
        ThreadUnsafeBufferPool<config::patronus::kClientRdmaBufferSize>>
        rdma_client_buffer;
    std::unique_ptr<ThreadUnsafeBufferPool<8>> rdma_client_buffer_8B;
};

class pre_patronus_explain;

class Patronus
{
public:
    using pointer = std::shared_ptr<Patronus>;

    constexpr static size_t kMaxCoroNr = ::config::patronus::kMaxCoroNr;
    constexpr static size_t kMessageSize = ::config::patronus::kMessageSize;

    // TODO(patronus): try to tune this parameter up.
    constexpr static size_t kMwPoolSizePerThread = 50 * define::K;
    constexpr static uint64_t kDefaultHint = 0;
    constexpr static size_t kMaxSyncKey = 128;

    using umsg_ptr_t = UnreliableConnection<kMaxAppThread>::ptr_t;

    static pointer ins(const PatronusConfig &conf)
    {
        return std::make_shared<Patronus>(conf);
    }
    Patronus &operator=(const Patronus &) = delete;
    Patronus(const Patronus &) = delete;
    Patronus(const PatronusConfig &conf);
    ~Patronus();

    /**
     * @brief Get the rlease object
     *
     * @param node_id the node_id
     * @param dir_id  the dir_id.
     * @param bind_gaddr
     * - w/ allocation sementics: should be nullgaddr
     * - w/o allocation semantics: should be one valid global address
     * @param alloc_hint
     * - w/ allocation semantics: the hint to select allocators
     * - w/o allocation semantics: should be 0.
     * @param size the length of the object / address
     * @param ns the length of term requesting for protection
     * @param flag @see AcquireRequestFlag
     * @param ctx sync call if ctx is nullptr. Otherwise coroutine context.
     * @return Read Lease
     */
    inline Lease get_rlease(uint16_t node_id,
                            uint16_t dir_id,
                            GlobalAddress bind_gaddr,
                            uint64_t alloc_hint,
                            size_t size,
                            std::chrono::nanoseconds ns,
                            flag_t flag /* AcquireRequestFlag */,
                            CoroContext *ctx);
    /**
     * @brief Alloc a piece of remote memory, without any permission binded
     *
     * @param node_id the target node id
     * @param dir_id the directory id
     * @param size the requested size
     * @param hint the hint to the allocator
     * @param ctx
     * @return @see GlobalAddress
     */
    inline GlobalAddress alloc(uint16_t node_id,
                               uint16_t dir_id,
                               size_t size,
                               uint64_t hint,
                               CoroContext *ctx);
    /**
     * @brief @see get_rlease
     */
    inline Lease get_wlease(uint16_t node_id,
                            uint16_t dir_id,
                            GlobalAddress bind_gaddr,
                            uint64_t alloc_hint,
                            size_t size,
                            std::chrono::nanoseconds ns,
                            flag_t flag /* AcquireRequestFlag */,
                            CoroContext *ctx = nullptr);
    inline Lease upgrade(Lease &lease,
                         flag_t flag /*LeaseModifyFlag */,
                         CoroContext *ctx = nullptr);
    /**
     * @brief extend the lifecycle of lease for another more ns.
     */
    inline RetCode extend(Lease &lease,
                          std::chrono::nanoseconds ns,
                          flag_t flag /* LeaseModifyFlag */,
                          CoroContext *ctx = nullptr);
    inline RetCode rpc_extend(Lease &lease,
                              std::chrono::nanoseconds ns,
                              flag_t flag /* LeaseModifyFlag */,
                              CoroContext *ctx = nullptr);
    /**
     * when allocation is ON, hint is sent to the server
     */
    inline void relinquish(Lease &lease,
                           uint64_t hint,
                           flag_t flag /* LeaseModifyFlag */,
                           CoroContext *ctx = nullptr);
    /**
     * @brief Deallocate to a piece of remote memory, without any permission
     * modification
     *
     * @param gaddr the address got from p->get_gaddr(lease)
     * @param dir_id the directory id
     * @param size the size of that piece of memory
     * @param hint a hint to the allocator
     * @param ctx
     */
    inline void dealloc(GlobalAddress gaddr,
                        uint16_t dir_id,
                        size_t size,
                        uint64_t hint,
                        CoroContext *ctx = nullptr);
    inline RetCode read(Lease &lease,
                        char *obuf,
                        size_t size,
                        size_t offset,
                        flag_t flag /* RWFlag */,
                        CoroContext *ctx,
                        TraceView = util::nulltrace);
    inline RetCode rpc_read(
        Lease &lease, char *obuf, size_t size, size_t offset, CoroContext *ctx);
    inline RetCode write(Lease &lease,
                         const char *ibuf,
                         size_t size,
                         size_t offset,
                         flag_t flag /* RWFlag */,
                         CoroContext *ctx,
                         TraceView = util::nulltrace);
    inline RetCode rpc_write(Lease &lease,
                             const char *ibuf,
                             size_t size,
                             size_t offset,
                             CoroContext *ctx);
    inline RetCode cas(Lease &lease,
                       char *iobuf,
                       size_t offset,
                       uint64_t compare,
                       uint64_t swap,
                       flag_t flag /* RWFlag */,
                       CoroContext *ctx,
                       TraceView = util::nulltrace);
    inline RetCode rpc_cas(Lease &lease,
                           char *iobuf,
                           size_t offset,
                           uint64_t compare,
                           uint64_t swap,
                           CoroContext *ctx);
    // below for batch API
    RetCode prepare_write(PatronusBatchContext &batch,
                          Lease &lease,
                          const char *ibuf,
                          size_t size,
                          size_t offset,
                          flag_t flag,
                          CoroContext *ctx = nullptr);
    RetCode prepare_read(PatronusBatchContext &batch,
                         Lease &lease,
                         char *obuf,
                         size_t size,
                         size_t offset,
                         flag_t flag,
                         CoroContext *ctx = nullptr);
    RetCode prepare_cas(PatronusBatchContext &batch,
                        Lease &lease,
                        char *iobuf,
                        size_t offset,
                        uint64_t compare,
                        uint64_t swap,
                        flag_t flag,
                        CoroContext *ctx = nullptr);
    RetCode prepare_faa(PatronusBatchContext &batch,
                        Lease &lease,
                        char *iobuf,
                        size_t offset,
                        int64_t value,
                        flag_t flag,
                        CoroContext *ctx = nullptr);
    RetCode commit(PatronusBatchContext &batch, CoroContext *ctx = nullptr);
    auto patronus_now() const
    {
        return time_syncer_->patronus_now();
    }

    // handy cluster-wide put/get. Not very performance, but convenient
    template <typename V,
              typename T,
              std::enable_if_t<std::is_trivially_copyable_v<V>, bool> = true,
              std::enable_if_t<!std::is_array_v<V>, bool> = true>
    void put(const std::string &key, const V &v, const T &sleep_time)
    {
        DVLOG(1) << "[keeper] PUT key: `" << key << "` value: `" << v;
        auto value = std::string((const char *) &v, sizeof(V));
        return dsm_->put(key, value, sleep_time);
    }
    template <typename T>
    void put(const std::string &key,
             const std::string &value,
             const T &sleep_time)
    {
        DVLOG(1) << "[keeper] PUT key: `" << key << "` value: `" << value;
        return dsm_->put(key, value, sleep_time);
    }
    template <typename T>
    std::string try_get(const std::string &key, const T &sleep_time)
    {
        return dsm_->try_get(key, sleep_time);
    }
    template <typename T>
    std::string get(const std::string &key, const T &sleep_time)
    {
        auto value = dsm_->get(key, sleep_time);
        DVLOG(1) << "[keeper] GET key: `" << key << "` value: `" << value;
        return value;
    }
    template <typename V, typename T>
    V get_object(const std::string &key, const T &sleep_time)
    {
        auto v = dsm_->get(key, sleep_time);
        V ret;
        CHECK_GE(v.size(), sizeof(V));
        memcpy((char *) &ret, v.data(), sizeof(V));
        DVLOG(1) << "[keeper] GET_OBJ key: " << key << ", obj: " << ret;
        return ret;
    }

    template <typename T>
    void keeper_barrier(const std::string &key, const T &sleep_time)
    {
        return dsm_->barrier(key, sleep_time);
    }

    /**
     * @brief After all the node call this function, @should_exit() will
     * return true
     *
     * @param ctx
     */
    void finished(uint64_t key);
    bool should_exit(uint64_t key) const
    {
        return should_exit_[key].load(std::memory_order_relaxed);
    }

    /**
     * @brief give the server thread to Patronus.
     *
     */
    void server_serve(uint64_t key);

    size_t try_get_client_continue_coros(coro_t *coro_buf, size_t limit);

    PatronusThreadResourceDesc prepare_client_thread(
        bool is_registering_thread);
    bool apply_client_resource(PatronusThreadResourceDesc &&,
                               bool bind_core,
                               TraceView v = util::nulltrace);
    bool has_registered() const;

    /**
     * @brief Any thread should call this function before calling any
     * function of Patronus
     *
     */
    void registerServerThread();
    /**
     * @brief see @registerServerThread
     *
     */
    void registerClientThread();

    size_t get_node_id() const
    {
        return dsm_->get_node_id();
    }
    size_t get_thread_id() const
    {
        return dsm_->get_thread_id();
    }
    size_t get_thread_name_id() const
    {
        return dsm_->get_thread_name_id();
    }
    Buffer get_server_internal_buffer()
    {
        return dsm_->get_server_buffer();
    }

    void prepare_handle_request_messages(const char *msg_buf,
                                         size_t msg_nr,
                                         ServerCoroBatchExecutionContext &,
                                         CoroContext *ctx);
    void commit_handle_request_messages(ServerCoroBatchExecutionContext &,
                                        CoroContext *ctx);
    void post_handle_request_messages(const char *msg_buf,
                                      size_t msg_nr,
                                      ServerCoroBatchExecutionContext &ex_ctx,
                                      CoroContext *ctx);
    void post_handle_request_acquire(AcquireRequest *req,
                                     HandleReqContext &req_ctx,
                                     CoroContext *ctx);
    void post_handle_request_lease_relinquish(LeaseModifyRequest *req,
                                              HandleReqContext &req_ctx,
                                              CoroContext *ctx);
    void post_handle_request_memory_access(MemoryRequest *req,
                                           CoroContext *ctx);
    void post_handle_request_lease_extend(LeaseModifyRequest *req,
                                          CoroContext *ctx);
    void post_handle_request_admin(AdminRequest *req, CoroContext *ctx);

    Buffer get_rdma_buffer_8B()
    {
        CHECK(!self_managing_client_rdma_buffer_);
        auto *ret = (char *) rdma_client_buffer_8B_->get();
        if (likely(ret != nullptr))
        {
            return Buffer(ret, 8);
        }
        return Buffer(nullptr, 0);
    }
    void put_rdma_buffer_8B(Buffer buffer)
    {
        if (buffer.buffer)
        {
            DCHECK_EQ(buffer.size, 8);
            rdma_client_buffer_8B_->put(buffer.buffer);
        }
    }

    Buffer get_self_managed_rdma_buffer()
    {
        CHECK(!self_managing_client_rdma_buffer_);
        self_managing_client_rdma_buffer_ = true;
        return Buffer(client_rdma_buffer_, client_rdma_buffer_size_);
    }
    void put_self_managed_rdma_buffer(Buffer buffer)
    {
        CHECK_EQ(buffer.buffer, client_rdma_buffer_);
        CHECK_EQ(buffer.size, client_rdma_buffer_size_);
        CHECK(self_managing_client_rdma_buffer_);
        self_managing_client_rdma_buffer_ = false;
    }

    Buffer get_rdma_buffer(size_t size)
    {
        CHECK(!self_managing_client_rdma_buffer_);
        if (size <= 8)
        {
            return get_rdma_buffer_8B();
        }
        auto *buf = (char *) rdma_client_buffer_->get();
        DCHECK_GE(kClientRdmaBufferSize, size);
        if (likely(buf != nullptr))
        {
            return Buffer(buf, kClientRdmaBufferSize);
        }
        return Buffer(nullptr, 0);
    }
    void put_rdma_buffer(Buffer buffer)
    {
        if (buffer.buffer)
        {
            if (buffer.size <= 8)
            {
                put_rdma_buffer_8B(buffer);
            }
            else
            {
                rdma_client_buffer_->put(buffer.buffer);
            }
        }
    }

    void hack_trigger_rdma_protection_error(size_t node_id,
                                            size_t dir_id,
                                            CoroContext *ctx);

    DSM::pointer get_dsm()
    {
        return dsm_;
    }

    const time::TimeSyncer &time_syncer() const
    {
        return *time_syncer_;
    }

    using PatronusLockManager = LockManager<NR_DIRECTORY, 4096 * 8>;

    constexpr std::pair<PatronusLockManager::bucket_t,
                        PatronusLockManager::slot_t>
    locate_key(id_t key) const
    {
        auto hash = key_hash(key);
        auto bucket_nr = lock_manager_.bucket_nr();
        auto slot_nr = lock_manager_.slot_nr();
        auto bucket_id = (hash / slot_nr) % bucket_nr;
        auto slot_id = hash % slot_nr;
        return {bucket_id, slot_id};
    }
    constexpr size_t admin_dir_id() const
    {
        return 0;
    }
    size_t lease_buffer_size() const
    {
        return conf_.lease_buffer_size;
    }
    size_t alloc_buffer_size() const
    {
        return conf_.alloc_buffer_size;
    }
    size_t user_reserved_buffer_size() const
    {
        return conf_.reserved_buffer_size;
    }
    Buffer get_lease_buffer() const
    {
        auto server_buf = dsm_->get_server_buffer();
        auto *buf_addr = server_buf.buffer;
        auto buf_size = lease_buffer_size();
        DCHECK_GE(server_buf.size, buf_size);
        return Buffer(buf_addr, buf_size);
    }
    Buffer get_alloc_buffer() const
    {
        auto server_buf = dsm_->get_server_buffer();
        auto *buf_addr = server_buf.buffer + lease_buffer_size();
        auto buf_size = alloc_buffer_size();
        DCHECK_GE(server_buf.size, lease_buffer_size() + alloc_buffer_size());
        return Buffer(buf_addr, buf_size);
    }
    Buffer get_user_reserved_buffer() const
    {
        auto server_buffer = dsm_->get_server_buffer();
        auto *buf_addr =
            server_buffer.buffer + lease_buffer_size() + alloc_buffer_size();
        auto buf_size = user_reserved_buffer_size();
        DCHECK_GE(lease_buffer_size() + alloc_buffer_size() + buf_size,
                  server_buffer.size);
        return Buffer(buf_addr, buf_size);
    }

    void thread_explain() const;
    inline GlobalAddress get_gaddr(const Lease &lease) const;

    // exposed buffer offset
    GlobalAddress to_exposed_gaddr(void *addr)
    {
        return GlobalAddress(0 /* node_id */,
                             dsm_->addr_to_buffer_offset(addr));
    }
    void *from_exposed_gaddr(GlobalAddress gaddr)
    {
        return dsm_->buffer_offset_to_addr(gaddr.offset);
    }

    inline void *patronus_alloc(size_t size, uint64_t hint);
    inline void patronus_free(void *addr, size_t size, uint64_t hint);

    void reg_allocator(uint64_t hint, mem::IAllocator::pointer allocator);
    mem::IAllocator::pointer get_allocator(uint64_t hint);

    friend std::ostream &operator<<(std::ostream &,
                                    const pre_patronus_explain &);

    void prepare_fast_backup_recovery(size_t prepare_nr);

    inline RetCode signal_modify_qp_flag(size_t node_id,
                                         size_t dir_id,
                                         bool to_ro,
                                         CoroContext *);
    inline RetCode signal_reinit_qp(size_t node_id,
                                    size_t dir_id,
                                    CoroContext *);
    friend class ServerCoroBatchExecutionContext;

    void set_configure_reuse_mw_opt(bool val);
    bool get_configure_reuse_mw_opt();

private:
    PatronusConfig conf_;
    // the default_allocator_ is set on registering server thread.
    // With the config from PatronusConfig.
    static thread_local mem::SlabAllocator::pointer default_allocator_;
    static thread_local std::unordered_map<uint64_t, mem::IAllocator::pointer>
        reg_allocators_;
    void *allocator_buf_addr_{nullptr};
    size_t allocator_buf_size_{0};
    // How many leases on average may a tenant hold?
    // It determines how much resources we should reserve
    constexpr static size_t kGuessActiveLeasePerCoro = 16;
    constexpr static size_t kClientThreadPerServerThread =
        ::config::patronus::kClientThreadPerServerThread;
    constexpr static size_t kClientRdmaBufferSize =
        ::config::patronus::kClientRdmaBufferSize;
    constexpr static size_t kLeaseContextNr =
        kMaxCoroNr * kGuessActiveLeasePerCoro * kClientThreadPerServerThread;
    static_assert(
        kLeaseContextNr <
        std::numeric_limits<decltype(AcquireResponse::lease_id)>::max());
    constexpr static size_t kServerCoroNr = kMaxCoroNr;
    constexpr static size_t kProtectionRegionPerThreadNr =
        NR_DIRECTORY * kMaxCoroNr * kGuessActiveLeasePerCoro *
        kClientThreadPerServerThread;
    constexpr static size_t kTotalProtectionRegionNr =
        kProtectionRegionPerThreadNr * kMaxAppThread;

    // kMaxCoroNr
    thread_local static std::vector<ServerCoroBatchExecutionContext>
        coro_batch_ex_ctx_;
    ServerCoroBatchExecutionContext &coro_ex_ctx(size_t coro_id)
    {
        DCHECK_LT(coro_id, kMaxCoroNr);
        return coro_batch_ex_ctx_[coro_id];
    }

    void explain(const PatronusConfig &);

    constexpr static id_t key_hash(id_t key)
    {
        // TODO(patronus): should use real hash to distribute the keys.
        return key;
    }

    inline RetCode admin_request_impl(size_t node_id,
                                      size_t dir_id,
                                      uint64_t data,
                                      flag_t flag,
                                      bool need_response,
                                      CoroContext *ctx);

    ibv_mw *get_mw(size_t dirID)
    {
        DCHECK(!mw_pool_[dirID].empty());
        auto *ret = mw_pool_[dirID].front();
        mw_pool_[dirID].pop();
        return DCHECK_NOTNULL(ret);
    }
    void put_mw(size_t dirID, ibv_mw *mw)
    {
        if (mw != nullptr)
        {
            mw_pool_[dirID].push(mw);
        }
    }

public:
    char *get_rdma_message_buffer()
    {
        return (char *) rdma_message_buffer_pool_->get();
    }
    void debug_valid_rdma_buffer(const void *buf)
    {
        if constexpr (debug())
        {
            CHECK_GE((uint64_t) buf, (uint64_t) client_rdma_buffer_);
            CHECK_LT((uint64_t) buf,
                     (uint64_t) client_rdma_buffer_ + client_rdma_buffer_size_);
        }
    }
    void put_rdma_message_buffer(char *buf)
    {
        if (buf != nullptr)
        {
            rdma_message_buffer_pool_->put(buf);
        }
    }

private:
    RpcContext *get_rpc_context()
    {
        return rpc_context_.get();
    }
    void put_rpc_context(RpcContext *ctx)
    {
        if (ctx != nullptr)
        {
            rpc_context_.put(ctx);
        }
    }
    uint16_t get_rpc_context_id(RpcContext *ctx)
    {
        auto ret = rpc_context_.obj_to_id(ctx);
        DCHECK_LT(ret, std::numeric_limits<uint16_t>::max());
        return ret;
    }
    RWContext *get_rw_context()
    {
        return DCHECK_NOTNULL(rw_context_.get());
    }
    void put_rw_context(RWContext *ctx)
    {
        if (ctx != nullptr)
        {
            rw_context_.put(ctx);
        }
    }
    uint16_t get_rw_context_id(RWContext *ctx)
    {
        auto ret = rw_context_.obj_to_id(ctx);
        DCHECK_LT(ret, std::numeric_limits<uint16_t>::max());
        return ret;
    }

    LeaseContext *get_lease_context()
    {
        auto *ret = DCHECK_NOTNULL(lease_context_.get());
        DCHECK(!ret->valid)
            << "** A newly allocated context should not be valid.";
        // set protection_region_id to an invalid value.
        ret->protection_region_id =
            std::numeric_limits<decltype(ret->protection_region_id)>::max();
        return ret;
    }
    LeaseContext *get_lease_context(uint16_t id)
    {
        auto *ret = DCHECK_NOTNULL(lease_context_.id_to_obj(id));
        if (unlikely(!ret->valid))
        {
            DVLOG(4) << "[Patronus] get_lease_context(id) related to invalid "
                        "contexts";
            return nullptr;
        }
        return ret;
    }
    void put_lease_context(LeaseContext *ctx)
    {
        if (ctx != nullptr)
        {
            ctx->protection_region_id =
                std::numeric_limits<decltype(ctx->protection_region_id)>::max();
            ctx->valid = false;
            lease_context_.put(ctx);
        }
    }
    ProtectionRegion *get_protection_region()
    {
        auto *ret =
            (ProtectionRegion *) DCHECK_NOTNULL(protection_region_pool_->get());
        return ret;
    }
    void put_protection_region(ProtectionRegion *p)
    {
        if (p != nullptr)
        {
            auto aba_unit_nr_to_ddl =
                p->aba_unit_nr_to_ddl.load(std::memory_order_acq_rel);
            // to avoid ABA problem, add the 32 bits by one each time.
            aba_unit_nr_to_ddl.u32_1++;
            p->aba_unit_nr_to_ddl.store(aba_unit_nr_to_ddl,
                                        std::memory_order_acq_rel);
            p->valid = false;
            protection_region_pool_->put(p);
        }
    }
    ProtectionRegion *get_protection_region(size_t id)
    {
        auto *ret = protection_region_pool_->id_to_buf(id);
        return (ProtectionRegion *) ret;
    }

    bool fast_switch_backup_qp(TraceView = util::nulltrace);

    // clang-format off
    /**
     * The layout of dsm_->get_server_reserve_buffer():
     * [required_time_sync_size()] [required_protection_region_size()]
     * 
     * ^-- get_time_sync_buffer()
     *                             ^-- get_protection_region_buffer()
     *                                                                
     * Total of them: reserved_buffer_size();
     */
    // clang-format on

    size_t reserved_buffer_size() const
    {
        return required_dsm_reserve_size();
    }
    size_t required_dsm_reserve_size() const
    {
        return required_protection_region_size() + required_time_sync_size();
    }
    static size_t required_protection_region_size()
    {
        size_t ret = sizeof(ProtectionRegion) * kTotalProtectionRegionNr;
        return ROUND_UP(ret, 4096);
    }
    Buffer get_protection_region_buffer()
    {
        auto reserve_buffer = dsm_->get_server_reserved_buffer();
        auto *buf_addr = reserve_buffer.buffer + required_time_sync_size();
        auto buf_size = required_protection_region_size();
        return Buffer(buf_addr, buf_size);
    }
    static size_t required_time_sync_size()
    {
        size_t ret = sizeof(time::ClockInfo);
        return ROUND_UP(ret, 4096);
    }
    Buffer get_time_sync_buffer()
    {
        auto reserve_buffer = dsm_->get_server_reserved_buffer();
        DCHECK_GE(reserve_buffer.size, required_dsm_reserve_size());
        auto *buf_addr = reserve_buffer.buffer;
        auto buf_size = required_time_sync_size();
        return Buffer(buf_addr, buf_size);
    }
    // for clients
    size_t handle_response_messages(const char *msg_buf,
                                    size_t msg_nr,
                                    coro_t *o_coro_buf);
    size_t handle_rdma_finishes(
        ibv_wc *buffer,
        size_t rdma_nr,
        coro_t *o_coro_buf,
        std::map<std::pair<size_t, size_t>, TraceView> &recov);
    void signal_server_to_recover_qp(size_t node_id,
                                     size_t dir_id,
                                     TraceView = util::nulltrace);
    void prepare_handle_request_acquire(AcquireRequest *,
                                        HandleReqContext &req_ctx,
                                        CoroContext *ctx);
    void prepare_handle_request_lease_modify(LeaseModifyRequest *,
                                             HandleReqContext &req_ctx,
                                             CoroContext *ctx);

    // for servers
    void handle_response_acquire(AcquireResponse *);
    void handle_response_lease_relinquish(LeaseModifyResponse *);
    void handle_response_lease_extend(LeaseModifyResponse *resp);
    inline void handle_response_admin_qp_modification(AdminResponse *resp,
                                                      CoroContext *ctx);
    void handle_response_memory_access(MemoryResponse *resp, CoroContext *ctx);
    void handle_admin_exit(AdminRequest *req, CoroContext *ctx);
    void handle_admin_recover(AdminRequest *req, CoroContext *ctx);
    void handle_admin_barrier(AdminRequest *req, CoroContext *ctx);
    void handle_admin_qp_access_flag(AdminRequest *req, CoroContext *ctx);
    void prepare_handle_request_lease_relinquish(LeaseModifyRequest *,
                                                 HandleReqContext &req_ctx,
                                                 CoroContext *ctx);
    void prepare_handle_request_lease_extend(LeaseModifyRequest *,
                                             CoroContext *ctx);
    void handle_request_lease_extend(LeaseModifyRequest *, CoroContext *ctx);
    void handle_request_lease_upgrade(LeaseModifyRequest *, CoroContext *ctx);

    /**
     * @param lease_id the id of LeaseContext
     * @param cid the client id. Will use to detect GC-ed lease (when cid
     * mismatch)
     * @param expect_unit_nr the expected unit numbers for this lease
     * period. If this field changed, it means the client extended the lease
     * by one-sided CAS. Server should sustain the GC till the next period.
     * @param flag LeaseModifyFlag
     * @param ctx
     */
    void task_gc_lease(uint64_t lease_id,
                       ClientID cid,
                       compound_uint64_t expect_unit_nr,
                       flag_t flag /* LeaseModifyFlag */,
                       CoroContext *ctx = nullptr);

    void prepare_gc_lease(uint64_t lease_id,
                          HandleReqContext &req_ctx,
                          ClientID cid,
                          flag_t flag,
                          CoroContext *ctx = nullptr);

    // server coroutines
    void server_coro_master(CoroYield &yield, uint64_t key);
    void server_coro_worker(coro_t coro_id, CoroYield &yield, uint64_t key);

    // helpers, actual impls
    Lease get_lease_impl(uint16_t node_id,
                         uint16_t dir_id,
                         id_t key,
                         size_t size,
                         time::ns_t ns,
                         RpcType type,
                         flag_t flag,
                         CoroContext *ctx = nullptr);

    inline bool already_passed_ddl(time::PatronusTime time) const;
    // flag should be RWFlag
    inline RetCode handle_rwcas_flag(Lease &lease,
                                     flag_t flag,
                                     CoroContext *ctx);
    inline RetCode handle_batch_op_flag(flag_t flag) const;
    inline RetCode validate_lease(const Lease &lease);
    RetCode protection_region_rw_impl(Lease &lease,
                                      char *io_buf,
                                      size_t size,
                                      size_t offset,
                                      bool is_read,
                                      CoroContext *ctx = nullptr);
    RetCode protection_region_cas_impl(Lease &lease,
                                       char *iobuf,
                                       size_t offset,
                                       uint64_t compare,
                                       uint64_t swap,
                                       CoroContext *ctx = nullptr);
    RetCode buffer_cas_impl(Lease &lease,
                            char *iobuf,
                            size_t offset,
                            uint64_t compare,
                            uint64_t swap,
                            CoroContext *ctx);
    RetCode read_write_impl(char *iobuf,
                            size_t size,
                            size_t node_id,
                            size_t dir_id,
                            uint32_t rkey,
                            size_t remote_addr,
                            bool is_read,
                            uint16_t wrid_prefix,
                            CoroContext *ctx,
                            TraceView = util::nulltrace);
    inline RetCode rpc_rwcas_impl(char *iobuf,
                                  size_t size,
                                  size_t node_id,
                                  size_t dir_id,
                                  size_t remote_addr,
                                  MemoryRequestFlag rwcas,
                                  CoroContext *ctx);
    inline RetCode extend_impl(Lease &lease,
                               size_t extend_unit_nr,
                               flag_t flag,
                               CoroContext *ctx);
    inline RetCode rpc_extend_impl(Lease &lease,
                                   uint64_t ns,
                                   flag_t flag,
                                   CoroContext *ctx);
    RetCode cas_impl(char *iobuf,
                     size_t node_id,
                     size_t dir_id,
                     uint32_t rkey,
                     uint64_t remote_addr,
                     uint64_t compare,
                     uint64_t swap,
                     uint16_t wr_prefix,
                     CoroContext *ctx,
                     TraceView = util::nulltrace);
    RetCode lease_modify_impl(Lease &lease,
                              uint64_t hint,
                              RpcType type,
                              time::ns_t ns,
                              flag_t flag /* LeaseModificationFlag */,
                              CoroContext *ctx = nullptr);
    inline void fill_bind_mw_wr(ibv_send_wr &wr,
                                ibv_mw *mw,
                                ibv_mr *mr,
                                uint64_t addr,
                                size_t length,
                                int access_flag);
    RetCode maybe_auto_extend(Lease &lease, CoroContext *ctx = nullptr);

    inline bool valid_lease_buffer_offset(size_t buffer_offset) const;
    inline bool valid_total_buffer_offset(size_t buffer_offset) const;
    void validate_buffers();

    void debug_analysis_per_qp_batch(const char *msg_buf,
                                     size_t msg_nr,
                                     OnePassBucketMonitor<double> &m);

    // owned by both
    DSM::pointer dsm_;
    time::TimeSyncer::pointer time_syncer_;
    static thread_local std::unique_ptr<ThreadUnsafeBufferPool<kMessageSize>>
        rdma_message_buffer_pool_;
    PatronusLockManager lock_manager_;

    // owned by client threads
    static thread_local ThreadUnsafePool<RpcContext, kMaxCoroNr> rpc_context_;
    static thread_local ThreadUnsafePool<RWContext, 2 * kMaxCoroNr> rw_context_;
    static thread_local std::unique_ptr<
        ThreadUnsafeBufferPool<kClientRdmaBufferSize>>
        rdma_client_buffer_;
    static thread_local std::unique_ptr<ThreadUnsafeBufferPool<8>>
        rdma_client_buffer_8B_;
    static thread_local char *client_rdma_buffer_;
    static thread_local size_t client_rdma_buffer_size_;

    static thread_local bool is_server_;
    static thread_local bool is_client_;

    static thread_local bool self_managing_client_rdma_buffer_;

    // owned by server threads
    // [NR_DIRECTORY]
    static thread_local std::queue<ibv_mw *> mw_pool_[NR_DIRECTORY];
    std::unordered_set<ibv_mw *> allocated_mws_;
    std::mutex allocated_mws_mu_;
    static thread_local ThreadUnsafePool<LeaseContext, kLeaseContextNr>
        lease_context_;
    static thread_local ServerCoroContext server_coro_ctx_;
    static thread_local std::unique_ptr<
        ThreadUnsafeBufferPool<sizeof(ProtectionRegion)>>
        protection_region_pool_;
    static thread_local DDLManager ddl_manager_;
    static thread_local bool has_registered_;
    constexpr static uint32_t magic = 0b1010101010;
    constexpr static uint16_t mask = 0b1111111111;

    // for admin management
    std::array<std::array<std::atomic<bool>, MAX_MACHINE>, kMaxSyncKey>
        finished_{};
    std::array<std::atomic<bool>, kMaxSyncKey> should_exit_{false};

    // for barrier
    std::unordered_map<uint64_t, std::set<uint64_t>> barrier_;
    std::mutex barrier_mu_;

    std::chrono::time_point<std::chrono::steady_clock> finish_time_sync_now_;

    std::queue<PatronusThreadResourceDesc> prepared_fast_backup_descs_;
    std::mutex fast_backup_descs_mu_;

    bool reuse_mw_opt_enabled_{true};
};

bool Patronus::already_passed_ddl(time::PatronusTime patronus_ddl) const
{
    auto patronus_now = time_syncer_->patronus_now();
    auto epsilon = time_syncer_->epsilon();
    time::ns_t diff_ns = patronus_ddl - patronus_now;
    bool already_pass_ddl =
        diff_ns < 0 + epsilon + time::TimeSyncer::kCommunicationLatencyNs;
    return already_pass_ddl;
}

GlobalAddress Patronus::alloc(uint16_t node_id,
                              uint16_t dir_id,
                              size_t size,
                              uint64_t hint,
                              CoroContext *ctx)
{
    auto flag = (flag_t) AcquireRequestFlag::kNoGc |
                (flag_t) AcquireRequestFlag::kOnlyAllocation |
                (flag_t) AcquireRequestFlag::kNoBindPR;
    auto lease = get_lease_impl(node_id,
                                dir_id,
                                hint /* key & hint */,
                                size,
                                0,
                                RpcType::kAcquireWLeaseReq,
                                flag,
                                ctx);
    return get_gaddr(lease);
}

// gaddr is buffer offset
Lease Patronus::get_rlease(uint16_t node_id,
                           uint16_t dir_id,
                           GlobalAddress bind_gaddr,
                           uint64_t alloc_hint,
                           size_t size,
                           std::chrono::nanoseconds chrono_ns,
                           flag_t flag,
                           CoroContext *ctx)
{
    bool only_alloc = flag & (flag_t) AcquireRequestFlag::kOnlyAllocation;
    bool with_alloc = flag & (flag_t) AcquireRequestFlag::kWithAllocation;
    bool alloc_semantics = only_alloc || with_alloc;
    if constexpr (debug())
    {
        debug_validate_acquire_request_flag(flag);
        CHECK(!only_alloc)
            << "** API integrity: please use patronus::alloc() instead";

        if (alloc_semantics)
        {
            CHECK(bind_gaddr.is_null()) << "** API integrity: set bind_gaddr "
                                           "to null when w/ allocation";
        }
        else
        {
            // sometimes bind_gaddr.is_null() is also a valid address
            CHECK_EQ(alloc_hint, 0)
                << "** API integrity: set alloc_hint to 0 when w/o allocation";
        }
    }
    uint64_t addr_or_alloc_hint = 0;
    if (alloc_semantics)
    {
        addr_or_alloc_hint = alloc_hint;
    }
    else
    {
        addr_or_alloc_hint = bind_gaddr.offset;
    }

    auto ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(chrono_ns).count();
    return get_lease_impl(node_id,
                          dir_id,
                          addr_or_alloc_hint,
                          size,
                          ns,
                          RpcType::kAcquireRLeaseReq,
                          flag,
                          ctx);
}

// gaddr is buffer offset
Lease Patronus::get_wlease(uint16_t node_id,
                           uint16_t dir_id,
                           GlobalAddress bind_gaddr,
                           uint64_t alloc_hint,
                           size_t size,
                           std::chrono::nanoseconds chrono_ns,
                           flag_t flag,
                           CoroContext *ctx)
{
    bool only_alloc = flag & (flag_t) AcquireRequestFlag::kOnlyAllocation;
    bool with_alloc = flag & (flag_t) AcquireRequestFlag::kWithAllocation;
    bool alloc_semantics = only_alloc || with_alloc;

    if constexpr (debug())
    {
        debug_validate_acquire_request_flag(flag);
        CHECK(!only_alloc)
            << "** API integrity: please use patronus::alloc() instead";
        if (alloc_semantics)
        {
            CHECK(bind_gaddr.is_null()) << "** API integrity: set bind_gaddr "
                                           "to null when w/ allocation";
        }
        else
        {
            CHECK_EQ(alloc_hint, 0)
                << "** API integrity: set alloc_hint to 0 when w/o allocation";
        }
    }
    uint64_t addr_or_alloc_hint = 0;
    if (alloc_semantics)
    {
        addr_or_alloc_hint = alloc_hint;
    }
    else
    {
        addr_or_alloc_hint = bind_gaddr.offset;
    }

    auto ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(chrono_ns).count();
    return get_lease_impl(node_id,
                          dir_id,
                          addr_or_alloc_hint,
                          size,
                          ns,
                          RpcType::kAcquireWLeaseReq,
                          flag,
                          ctx);
}

RetCode Patronus::rpc_extend_impl(Lease &lease,
                                  uint64_t ns,
                                  flag_t flag,
                                  CoroContext *ctx)
{
    DVLOG(4) << "[patronus][rpc-extend-impl] trying to extend. ns: " << ns
             << ", flag:" << LeaseModifyFlagOut(flag)
             << ", coro:" << (ctx ? *ctx : nullctx)
             << "original lease: " << lease;

    return lease_modify_impl(
        lease, 0 /* hint */, RpcType::kExtendReq, ns, flag, ctx);
}

RetCode Patronus::extend_impl(Lease &lease,
                              size_t extend_unit_nr,
                              flag_t flag,
                              CoroContext *ctx)
{
    DVLOG(4) << "[patronus][extend-impl] trying to extend. extend_unit_nr: "
             << extend_unit_nr << ", flag:" << LeaseModifyFlagOut(flag)
             << ", coro:" << (ctx ? *ctx : nullctx)
             << "original lease: " << lease;

    auto offset = offsetof(ProtectionRegion, aba_unit_nr_to_ddl);
    using ABAT = decltype(ProtectionRegion::aba_unit_nr_to_ddl);
    auto rdma_buffer = get_rdma_buffer(sizeof(ABAT));
    DCHECK_LE(sizeof(ABAT), rdma_buffer.size);

    auto aba_unit_nr_to_ddl = lease.aba_unit_nr_to_ddl_;
    auto compare = aba_unit_nr_to_ddl.val;
    // equivalent to double: plus the current value
    aba_unit_nr_to_ddl.u32_2 += extend_unit_nr;
    auto swap = aba_unit_nr_to_ddl.val;

    auto cas_ec = protection_region_cas_impl(
        lease, rdma_buffer.buffer, offset, compare, swap, ctx);

    if (likely(cas_ec == RC::kOk))
    {
        lease.aba_unit_nr_to_ddl_.u32_2 += extend_unit_nr;
        lease.update_ddl_term();
    }
    put_rdma_buffer(rdma_buffer);
    DVLOG(4) << "[patronus][extend-impl] Done extend. coro: "
             << (ctx ? *ctx : nullctx) << ". cas_ec: " << cas_ec
             << ". Now lease: " << lease;
    return cas_ec;
}

RetCode Patronus::rpc_extend(Lease &lease,
                             std::chrono::nanoseconds chrono_ns,
                             flag_t flag,
                             CoroContext *ctx)
{
    debug_validate_lease_modify_flag(flag);
    auto ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(chrono_ns).count();
    auto rc = rpc_extend_impl(lease, ns, flag, ctx);
    if (rc != RC::kOk)
    {
        return rc;
    }

    // okay, modify lease locally
    auto ns_per_unit = lease.ns_per_unit_;
    // round up divide
    auto extend_unit_nr = (ns + ns_per_unit - 1) / ns_per_unit;
    lease.aba_unit_nr_to_ddl_.u32_2 += extend_unit_nr;
    lease.update_ddl_term();

    DCHECK_EQ(rc, RC::kOk);
    return rc;
}

RetCode Patronus::extend(Lease &lease,
                         std::chrono::nanoseconds chrono_ns,
                         flag_t flag,
                         CoroContext *ctx)
{
    debug_validate_lease_modify_flag(flag);
    auto ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(chrono_ns).count();
    auto ns_per_unit = lease.ns_per_unit_;
    // round up divide
    auto extend_unit_nr = (ns + ns_per_unit - 1) / ns_per_unit;

    // maybe too slow to check for local expiration
    // auto patronus_now = time_syncer_->patronus_now();
    // auto patronus_ddl = lease.ddl_term();
    // auto epsilon = time_syncer_->epsilon();
    // time::ns_t diff_ns = patronus_ddl - patronus_now;
    // bool already_pass_ddl =
    //     diff_ns < 0 + epsilon + time::TimeSyncer::kCommunicationLatencyNs;
    // if (unlikely(already_pass_ddl))
    // {
    //     // already pass DDL, no need to extend.
    //     DVLOG(4) << "[patronus][maybe-extend] Assume DDL passed (not extend).
    //     "
    //                 "lease ddl: "
    //              << patronus_ddl << ", patronus_now: " << patronus_now
    //              << ", diff_ns: " << diff_ns << " < 0 + epsilon(" << epsilon
    //              << ") + CommunicationLatencyNs("
    //              << time::TimeSyncer::kCommunicationLatencyNs
    //              << "). coro: " << (ctx ? *ctx : nullctx)
    //              << ". Now lease: " << lease;
    //     return RC::kLeaseLocalExpiredErr;
    // }

    return extend_impl(lease, extend_unit_nr, flag, ctx);
}

// gaddr is buffer offset
void Patronus::dealloc(GlobalAddress gaddr,
                       uint16_t dir_id,
                       size_t size,
                       uint64_t hint,
                       CoroContext *ctx)
{
    // construct a lease to make lease_modify_impl happy
    auto node_id = gaddr.nodeID;

    // a little bit hacky:
    // convert from buffer offset (gaddr) to dsm offset
    auto buffer_offset = gaddr.offset;
    auto dsm_offset = dsm_->buffer_offset_to_dsm_offset(buffer_offset);
    Lease lease;
    using LeaseIDT = decltype(lease.id_);
    lease.node_id_ = node_id;
    lease.dir_id_ = dir_id;
    lease.base_addr_ = dsm_offset;
    lease.buffer_size_ = size;
    lease.id_ = std::numeric_limits<LeaseIDT>::max();

    auto flag = (flag_t) LeaseModifyFlag::kNoRelinquishUnbindAny |
                (flag_t) LeaseModifyFlag::kOnlyDeallocation;
    auto ret = lease_modify_impl(
        lease, hint, RpcType::kRelinquishReq, 0 /* term */, flag, ctx);
    DCHECK_EQ(ret, RC::kOk);
}
void Patronus::relinquish(Lease &lease,
                          uint64_t hint,
                          flag_t flag,
                          CoroContext *ctx)
{
    bool only_dealloc = flag & (flag_t) LeaseModifyFlag::kOnlyDeallocation;
    DCHECK(!only_dealloc) << "Please use Patronus::dealloc instead";

    debug_validate_lease_modify_flag(flag);
    // TODO(Patronus): the term is set to 0 here.
    auto rc = lease_modify_impl(
        lease, hint, RpcType::kRelinquishReq, 0 /* term */, flag, ctx);
    DCHECK_EQ(rc, RC::kOk);

    lease.set_invalid();
}

RetCode Patronus::validate_lease([[maybe_unused]] const Lease &lease)
{
    auto lease_patronus_ddl = lease.ddl_term();
    auto patronus_now = DCHECK_NOTNULL(time_syncer_)->patronus_now();
    auto ret = time_syncer_->definitely_lt(patronus_now, lease_patronus_ddl)
                   ? RC::kOk
                   : RC::kLeaseLocalExpiredErr;
    DVLOG(5) << "[patronus][validate_lease] patronus_now: " << patronus_now
             << ", ddl: " << lease_patronus_ddl << ", ret: " << ret;
    return ret;
}

RetCode Patronus::handle_rwcas_flag(Lease &lease, flag_t flag, CoroContext *ctx)
{
    RetCode ec = RC::kOk;

    bool no_check;
    if (lease.no_gc_)
    {
        // does not need to check, because the lease will always be valid
        no_check = true;
    }
    else
    {
        no_check = flag & (flag_t) RWFlag::kNoLocalExpireCheck;
    }

    if (likely(!no_check))
    {
        if ((ec = validate_lease(lease)) != RC::kOk)
        {
            return ec;
        }
    }
    bool with_auto_extend = flag & (flag_t) RWFlag::kWithAutoExtend;
    if (with_auto_extend)
    {
        maybe_auto_extend(lease, ctx);
    }
    bool reserved = flag & (flag_t) RWFlag::kReserved;
    DCHECK(!reserved);
    return RC::kOk;
}

RetCode Patronus::read(Lease &lease,
                       char *obuf,
                       size_t size,
                       size_t offset,
                       flag_t flag,
                       CoroContext *ctx,
                       TraceView v)
{
    RetCode ec = RC::kOk;
    if ((ec = handle_rwcas_flag(lease, flag, ctx)) != RC::kOk)
    {
        return ec;
    }

    bool with_cache = flag & (flag_t) RWFlag::kWithCache;
    if (with_cache)
    {
        if (lease.cache_query(offset, size, obuf))
        {
            // cache hit
            return RC::kOk;
        }
    }

    CHECK(lease.success());
    CHECK(lease.is_readable());

    auto node_id = lease.node_id_;
    auto dir_id = lease.dir_id_;
    uint32_t rkey = 0;
    bool use_universal_rkey = flag & (flag_t) RWFlag::kUseUniversalRkey;
    if (use_universal_rkey)
    {
        rkey = dsm_->get_rkey(node_id, dir_id);
    }
    else
    {
        rkey = lease.cur_rkey_;
    }
    uint64_t remote_addr = lease.base_addr_ + offset;

    ec = read_write_impl(obuf,
                         size,
                         node_id,
                         dir_id,
                         rkey,
                         remote_addr,
                         true /* is_read */,
                         WRID_PREFIX_PATRONUS_RW,
                         ctx,
                         v);
    return ec;
}
RetCode Patronus::write(Lease &lease,
                        const char *ibuf,
                        size_t size,
                        size_t offset,
                        flag_t flag,
                        CoroContext *ctx,
                        TraceView v)
{
    auto ec = handle_rwcas_flag(lease, flag, ctx);
    if (unlikely(ec != RC::kOk))
    {
        return ec;
    }

    auto node_id = lease.node_id_;
    auto dir_id = lease.dir_id_;
    uint32_t rkey = 0;
    bool use_universal_rkey = flag & (flag_t) RWFlag::kUseUniversalRkey;
    if (use_universal_rkey)
    {
        rkey = dsm_->get_rkey(node_id, dir_id);
    }
    else
    {
        rkey = lease.cur_rkey_;
    }
    uint64_t remote_addr = lease.base_addr_ + offset;
    ec = read_write_impl((char *) ibuf,
                         size,
                         node_id,
                         dir_id,
                         rkey,
                         remote_addr,
                         false /* is_read */,
                         WRID_PREFIX_PATRONUS_RW,
                         ctx,
                         v);

    bool with_cache = flag & (flag_t) RWFlag::kWithCache;
    if (ec == RC::kOk && with_cache)
    {
        lease.cache_insert(offset, size, ibuf);
    }

    return ec;
}

RetCode Patronus::cas(Lease &lease,
                      char *iobuf,
                      size_t offset,
                      uint64_t compare,
                      uint64_t swap,
                      flag_t flag,
                      CoroContext *ctx,
                      TraceView v)
{
    auto ec = handle_rwcas_flag(lease, flag, ctx);
    if (unlikely(ec != RC::kOk))
    {
        return ec;
    }

    auto node_id = lease.node_id_;
    auto dir_id = lease.dir_id_;
    uint32_t rkey = 0;
    bool use_universal_rkey = flag & (flag_t) RWFlag::kUseUniversalRkey;
    if (use_universal_rkey)
    {
        rkey = dsm_->get_rkey(node_id, dir_id);
    }
    else
    {
        rkey = lease.cur_rkey_;
    }
    uint64_t remote_addr = lease.base_addr_ + offset;
    ec = cas_impl(iobuf,
                  node_id,
                  dir_id,
                  rkey,
                  remote_addr,
                  compare,
                  swap,
                  WRID_PREFIX_PATRONUS_CAS,
                  ctx,
                  v);

    bool with_cache = flag & (flag_t) RWFlag::kWithCache;
    if (ec == RC::kOk && with_cache)
    {
        DCHECK_GE(sizeof(swap), 8);
        lease.cache_insert(offset, 8 /* size */, (const char *) swap);
    }
    return ec;
}

void Patronus::fill_bind_mw_wr(ibv_send_wr &wr,
                               ibv_mw *mw,
                               ibv_mr *mr,
                               uint64_t addr,
                               size_t length,
                               int access_flag)
{
    wr.sg_list = nullptr;
    wr.num_sge = 0;
    wr.opcode = IBV_WR_BIND_MW;
    wr.imm_data = 0;
    wr.wr_id = 0;
    wr.send_flags = 0;
    wr.bind_mw.mw = mw;
    wr.bind_mw.rkey = mw->rkey;
    wr.bind_mw.bind_info.mr = mr;
    wr.bind_mw.bind_info.addr = addr;
    wr.bind_mw.bind_info.length = length;
    wr.bind_mw.bind_info.mw_access_flags = access_flag;
}

void *Patronus::patronus_alloc(size_t size, uint64_t hint)
{
    if (hint == kDefaultHint)
    {
        return default_allocator_->alloc(size);
    }
    auto it = reg_allocators_.find(hint);
    if (it == reg_allocators_.end())
    {
        DCHECK(false) << "** Unknown hint " << hint
                      << ". server thread tid: " << get_thread_id();
        return nullptr;
    }
    return it->second->alloc(size);
}
void Patronus::patronus_free(void *addr, size_t size, uint64_t hint)
{
    if (addr == nullptr)
    {
        return;
    }
    if (hint == 0)
    {
        return default_allocator_->free(addr, size);
    }

    auto it = reg_allocators_.find(hint);
    if (it == reg_allocators_.end())
    {
        CHECK(false) << "** Failed to free with hint " << hint
                     << ": not found. addr: " << addr << ", size: " << size;
    }
    return it->second->free(addr, size);
}

bool Patronus::valid_lease_buffer_offset(size_t buffer_offset) const
{
    // normally, conf valid implies dsm valid, but just test it.
    return (buffer_offset < conf_.lease_buffer_size) &&
           valid_total_buffer_offset(buffer_offset);
}
bool Patronus::valid_total_buffer_offset(size_t offset) const
{
    return dsm_->valid_buffer_offset(offset);
}

// convert to the buffer offset (which client should hold)
// from the dsm offset (which the lease holds)
GlobalAddress Patronus::get_gaddr(const Lease &lease) const
{
    if (unlikely(!lease.success()))
    {
        DCHECK_EQ(lease.ec(), AcquireRequestStatus::kNoMem)
            << "** Unexpected error type";
        return nullgaddr;
    }
    auto dsm_offset = lease.base_addr();
    auto buffer_offset = dsm_->dsm_offset_to_buffer_offset(dsm_offset);
    return GlobalAddress(lease.node_id_, buffer_offset);
}

class pre_patronus_explain
{
public:
    pre_patronus_explain(const Patronus &p) : p_(p)
    {
    }
    friend std::ostream &operator<<(std::ostream &os,
                                    const pre_patronus_explain &);

private:
    const Patronus &p_;
};

inline std::ostream &operator<<(std::ostream &os,
                                const pre_patronus_explain &pre)
{
    const auto &p = pre.p_;
    CHECK(p.is_client_ || p.is_server_)
        << "Unable to explain patronus: from un-registered thread. ";

    os << "{Patronus explain: ";
    if (p.rdma_message_buffer_pool_)
    {
        os << "rdma_message_buffer(thread): "
           << p.rdma_message_buffer_pool_->size() << " nr with size "
           << Patronus::kMessageSize << " B. at "
           << (void *) p.rdma_message_buffer_pool_.get() << ". ";
    }
    if (p.rdma_client_buffer_)
    {
        os << "rdma_client_buffer(thread): " << p.rdma_client_buffer_->size()
           << " nr with size " << Patronus::kClientRdmaBufferSize << " B. at "
           << (void *) p.rdma_client_buffer_.get() << ". ";
    }
    if (p.rdma_client_buffer_8B_)
    {
        os << "rdma_client_buffer_8B(thread): "
           << p.rdma_client_buffer_8B_->size() << " nr with size 8 B. at "
           << (void *) p.rdma_client_buffer_8B_.get() << ". ";
    }

    os << "rpc_context(thread): " << p.rpc_context_.size() << " with "
       << sizeof(RpcContext) << " B. at " << (void *) &p.rpc_context_ << ". ";
    os << "rw_context(thread): " << p.rw_context_.size() << " with "
       << sizeof(RWContext) << " B. at " << (void *) &p.rw_context_ << ". ";
    os << "lease_context(thread): " << p.lease_context_.size() << " with "
       << sizeof(LeaseContext) << " B. at " << (void *) &p.lease_context_
       << ". ";

    if (p.protection_region_pool_)
    {
        os << "pr_pool(thread): " << p.protection_region_pool_->size()
           << " with " << sizeof(ProtectionRegion) << " B. at "
           << (void *) p.protection_region_pool_.get() << ". ";
    }
    os << "}";

    return os;
}

inline void Patronus::debug_analysis_per_qp_batch(
    const char *msg_buf, size_t msg_nr, OnePassBucketMonitor<double> &batch_m)
{
    size_t batch_nr[MAX_MACHINE][kMaxAppThread]{};

    for (size_t i = 0; i < msg_nr; ++i)
    {
        auto *base = (BaseMessage *) (msg_buf + i * kMessageSize);
        auto request_type = base->type;
        switch (request_type)
        {
        case RpcType::kAcquireNoLeaseReq:
        case RpcType::kAcquireRLeaseReq:
        case RpcType::kAcquireWLeaseReq:
        {
            auto *msg = (AcquireRequest *) base;
            batch_nr[msg->cid.node_id][msg->cid.thread_id]++;
            break;
        }
        case RpcType::kRelinquishReq:
        {
            auto *msg = (LeaseModifyRequest *) base;
            batch_nr[msg->cid.node_id][msg->cid.thread_id]++;
            break;
        }
        case RpcType::kAdmin:
        {
            auto *msg = (AdminRequest *) base;
            batch_nr[msg->cid.node_id][msg->cid.thread_id]++;
            // do nothing
            break;
        }
        default:
        {
            LOG(FATAL) << "Unknown or invalid request type " << request_type
                       << ". Possible corrupted message";
        }
        }
    }
    for (size_t m = 0; m < MAX_MACHINE; ++m)
    {
        for (size_t t = 0; t < kMaxAppThread; ++t)
        {
            if (batch_nr[m][t])
            {
                batch_m.collect(batch_nr[m][t]);
            }
        }
    }
}

RetCode Patronus::admin_request_impl(size_t node_id,
                                     size_t dir_id,
                                     uint64_t data,
                                     flag_t flag,
                                     bool need_response,
                                     CoroContext *ctx)
{
    DCHECK_LT(get_thread_id(), kMaxAppThread);
    DCHECK_LT(dir_id, NR_DIRECTORY);

    RetCode ret = RC::kOk;
    char *rdma_buf = get_rdma_message_buffer();
    RpcContext *rpc_context = nullptr;

    auto *msg = (AdminRequest *) rdma_buf;
    msg->type = RpcType::kAdminReq;
    msg->cid.node_id = get_node_id();
    msg->cid.thread_id = get_thread_id();
    msg->cid.coro_id = ctx ? ctx->coro_id() : kNotACoro;
    msg->cid.rpc_ctx_id = 0;
    msg->dir_id = dir_id;
    msg->flag = flag;
    msg->data = data;
    msg->need_response = need_response;

    if (need_response)
    {
        rpc_context = get_rpc_context();
        auto rpc_ctx_id = get_rpc_context_id(rpc_context);
        msg->cid.rpc_ctx_id = rpc_ctx_id;

        rpc_context->ready = false;
        rpc_context->request = (BaseMessage *) msg;
        rpc_context->ret_code = RC::kOk;
    }

    if constexpr (debug())
    {
        msg->digest = 0;
        msg->digest = djb2_digest(msg, sizeof(AdminRequest));
    }

    dsm_->unreliable_send(rdma_buf, sizeof(AdminRequest), node_id, dir_id);

    if (need_response)
    {
        DCHECK_NOTNULL(ctx)->yield_to_master();

        DCHECK(rpc_context->ready) << "** Should have been ready when switch "
                                      "back to worker thread. coro: "
                                   << pre_coro_ctx(ctx);

        ret = rpc_context->ret_code;
        put_rpc_context(rpc_context);
    }

    // TODO(patronus): this may have problem if message not inlined and buffer
    // is re-used and NIC is DMA-ing
    put_rdma_message_buffer(rdma_buf);
    return ret;
}
RetCode Patronus::signal_modify_qp_flag(size_t node_id,
                                        size_t dir_id,
                                        bool to_ro,
                                        CoroContext *ctx)
{
    flag_t flag = 0;
    if (to_ro)
    {
        flag = (flag_t) AdminFlag::kAdminQPtoRO;
    }
    else
    {
        flag = (flag_t) AdminFlag::kAdminQPtoRW;
    }
    return admin_request_impl(node_id,
                              dir_id,
                              0 /* data */,
                              flag,
                              true /* need response */,
                              DCHECK_NOTNULL(ctx));
}

RetCode Patronus::signal_reinit_qp(size_t node_id,
                                   size_t dir_id,
                                   CoroContext *ctx)
{
    auto flag = (flag_t) AdminFlag::kAdminReqRecovery;
    auto ec = admin_request_impl(node_id,
                                 dir_id,
                                 0 /* data */,
                                 flag,
                                 true /* need response */,
                                 DCHECK_NOTNULL(ctx));
    CHECK_EQ(ec, RC::kOk);

    CHECK(dsm_->recoverThreadQP(node_id, dir_id));
    return kOk;
}

void Patronus::handle_response_admin_qp_modification(AdminResponse *resp,
                                                     CoroContext *ctx)
{
    std::ignore = ctx;
    auto rpc_ctx_id = resp->cid.rpc_ctx_id;
    auto *rpc_context = rpc_context_.id_to_obj(rpc_ctx_id);

    if (resp->success)
    {
        rpc_context->ret_code = kOk;
    }
    else
    {
        rpc_context->ret_code = RC::kRdmaExecutionErr;
    }
    rpc_context->ready.store(true, std::memory_order_release);
}

RetCode Patronus::rpc_cas(Lease &lease,
                          char *iobuf,
                          size_t offset,
                          uint64_t compare,
                          uint64_t swap,
                          CoroContext *ctx)
{
    uint64_t *data = (uint64_t *) iobuf;
    *data = compare;
    *(data + 1) = swap;
    uint64_t remote_addr = lease.base_addr_ + offset;
    return rpc_rwcas_impl(iobuf,
                          2 * sizeof(uint64_t),
                          lease.node_id_,
                          lease.dir_id_,
                          remote_addr,
                          MemoryRequestFlag::kCAS,
                          ctx);
}

RetCode Patronus::rpc_read(
    Lease &lease, char *obuf, size_t size, size_t offset, CoroContext *ctx)
{
    uint64_t remote_addr = lease.base_addr_ + offset;
    return rpc_rwcas_impl(obuf,
                          size,
                          lease.node_id_,
                          lease.dir_id_,
                          remote_addr,
                          MemoryRequestFlag::kRead,
                          ctx);
}

RetCode Patronus::rpc_write(Lease &lease,
                            const char *ibuf,
                            size_t size,
                            size_t offset,
                            CoroContext *ctx)
{
    uint64_t remote_addr = lease.base_addr_ + offset;
    return rpc_rwcas_impl((char *) ibuf,
                          size,
                          lease.node_id_,
                          lease.dir_id_,
                          remote_addr,
                          MemoryRequestFlag::kWrite,
                          ctx);
}

RetCode Patronus::rpc_rwcas_impl(char *iobuf,
                                 size_t size,
                                 size_t node_id,
                                 size_t dir_id,
                                 size_t remote_addr,
                                 MemoryRequestFlag rwcas,
                                 CoroContext *ctx)
{
    char *rdma_buf = get_rdma_message_buffer();
    auto *rpc_context = get_rpc_context();
    auto rpc_ctx_id = get_rpc_context_id(rpc_context);

    auto *msg = (MemoryRequest *) rdma_buf;
    msg->type = RpcType::kMemoryReq;
    msg->cid.node_id = get_node_id();
    msg->cid.thread_id = get_thread_id();
    msg->cid.coro_id = ctx ? ctx->coro_id() : kNotACoro;
    msg->cid.rpc_ctx_id = rpc_ctx_id;
    msg->remote_addr = remote_addr;
    msg->size = size;
    msg->flag = (flag_t) rwcas;

    rpc_context->ready = false;
    rpc_context->request = (BaseMessage *) msg;
    rpc_context->ret_code = RC::kOk;
    rpc_context->buffer_addr = iobuf;

    if (unlikely(!msg->validate()))
    {
        CHECK(false) << "** msg invalid. msg: " << msg
                     << ". possible size too large.";
        return RC::kInvalid;
    }
    if (rwcas == MemoryRequestFlag::kWrite)
    {
        memcpy(msg->buffer, iobuf, size);
    }
    if (rwcas == MemoryRequestFlag::kCAS)
    {
        DCHECK_EQ(size, 2 * sizeof(uint64_t));
        memcpy(msg->buffer, iobuf, size);
    }

    if constexpr (debug())
    {
        msg->digest = 0;
        msg->digest = djb2_digest(msg, msg->msg_size());
    }

    dsm_->unreliable_send(rdma_buf, msg->msg_size(), node_id, dir_id);

    DCHECK_NOTNULL(ctx)->yield_to_master();

    DCHECK(rpc_context->ready);

    auto ret = rpc_context->ret_code;
    put_rpc_context(rpc_context);
    put_rdma_message_buffer(rdma_buf);

    return ret;
}

}  // namespace patronus

#endif