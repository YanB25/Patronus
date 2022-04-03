#pragma once
#ifndef PATRONUS_H_
#define PATRONUS_H_

#include <set>
#include <unordered_set>

#include "Common.h"
#include "DSM.h"
#include "Result.h"
#include "patronus/Coro.h"
#include "patronus/DDLManager.h"
#include "patronus/Lease.h"
#include "patronus/LockManager.h"
#include "patronus/ProtectionRegion.h"
#include "patronus/TimeSyncer.h"
#include "patronus/Type.h"
#include "patronus/memory/allocator.h"
#include "patronus/memory/slab_allocator.h"
#include "util/Debug.h"
#include "util/RetCode.h"

namespace patronus
{
using namespace define::literals;
class Patronus;
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

struct RpcContext
{
    Lease *ret_lease{nullptr};
    Lease *origin_lease{nullptr};
    BaseMessage *request{nullptr};
    std::atomic<bool> ready{false};
    size_t dir_id{0};
};

struct RWContext
{
    bool *success{nullptr};
    std::atomic<bool> ready{false};
    coro_t coro_id;
    size_t target_node;
    size_t dir_id;
};

struct LeaseContext
{
    // correctness related fields:
    // will refuse relinquish requests from unrelated clients.
    ClientID client_cid;
    bool valid;
    // end
    ibv_mw *buffer_mw;
    ibv_mw *header_mw;
    size_t dir_id{size_t(-1)};
    uint64_t addr_to_bind{0};
    size_t buffer_size{0};
    size_t protection_region_id;
    // about with_conflict_detect
    bool with_conflict_detect{false};
    bool with_pr{true};
    bool with_buf{true};
    uint64_t key_bucket_id{0};
    uint64_t key_slot_id{0};
    uint64_t hint{0};
};

class pre_patronus_explain;

class Patronus
{
public:
    using pointer = std::shared_ptr<Patronus>;

    constexpr static size_t kMaxCoroNr = define::kMaxCoroNr;
    constexpr static size_t kMessageSize = ReliableConnection::kMessageSize;
    // TODO(patronus): try to tune this parameter up.
    constexpr static size_t kMwPoolSizePerThread = 50 * define::K;
    constexpr static uint64_t kDefaultHint = 0;
    constexpr static size_t kMaxSyncKey = 16;

    static pointer ins(const PatronusConfig &conf)
    {
        return std::make_shared<Patronus>(conf);
    }
    Patronus &operator=(const Patronus &) = delete;
    Patronus(const Patronus &) = delete;
    Patronus(const PatronusConfig &conf);
    ~Patronus();

    void barrier(uint64_t key);

    /**
     * @brief Get the rlease object
     *
     * @param buffer_gaddr
     * - If with_alloc OR only_alloc is ON, buffer_gaddr.offset is a hint for
     * the allocator.
     * - Otherwise, buffer_gaddr.offset is the address (offset, actually) you
     * want to bind
     * @param dir_id  the dir_id.
     * @param size the length of the object / address
     * @param ns the length of term requesting for protection
     * @param flag @see AcquireRequestFlag
     * @param ctx sync call if ctx is nullptr. Otherwise coroutine context.
     * @return Read Lease
     */
    inline Lease get_rlease(GlobalAddress buffer_gaddr,
                            uint16_t dir_id,
                            size_t size,
                            std::chrono::nanoseconds ns,
                            uint8_t flag /* AcquireRequestFlag */,
                            CoroContext *ctx = nullptr);
    /**
     * @brief Alloc a piece of remote memory, without any permission binded
     *
     * @param node_id the target node id
     * @param dir_id the directory id
     * @param size the requested size
     * @param hint the hint to the allocator
     * @param ctx
     * @return Lease
     */
    inline Lease alloc(uint16_t node_id,
                       uint16_t dir_id,
                       size_t size,
                       uint64_t hint,
                       CoroContext *ctx = nullptr);
    inline Lease get_wlease(GlobalAddress buffer_gaddr,
                            uint16_t dir_id,
                            size_t size,
                            std::chrono::nanoseconds ns,
                            uint8_t flag /* AcquireRequestFlag */,
                            CoroContext *ctx = nullptr);
    inline Lease upgrade(Lease &lease,
                         uint8_t flag /*LeaseModifyFlag */,
                         CoroContext *ctx = nullptr);
    /**
     * @brief extend the lifecycle of lease for another more ns.
     */
    inline RetCode extend(Lease &lease,
                          std::chrono::nanoseconds ns,
                          uint8_t flag /* LeaseModifyFlag */,
                          CoroContext *ctx = nullptr);
    /**
     * when allocation is ON, hint is sent to the server
     */
    inline void relinquish(Lease &lease,
                           uint64_t hint,
                           uint8_t flag /* LeaseModifyFlag */,
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
    inline void relinquish_write(Lease &lease, CoroContext *ctx = nullptr);
    inline RetCode read(Lease &lease,
                        char *obuf,
                        size_t size,
                        size_t offset,
                        uint8_t flag /* RWFlag */,
                        CoroContext *ctx = nullptr);
    inline RetCode write(Lease &lease,
                         const char *ibuf,
                         size_t size,
                         size_t offset,
                         uint8_t flag /* RWFlag */,
                         CoroContext *ctx = nullptr);
    inline RetCode cas(Lease &lease,
                       char *iobuf,
                       size_t offset,
                       uint64_t compare,
                       uint64_t swap,
                       uint8_t flag /* RWFlag */,
                       CoroContext *ctx = nullptr);
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
    void server_serve(size_t mid, uint64_t key);

    size_t try_get_client_continue_coros(size_t mid,
                                         coro_t *coro_buf,
                                         size_t limit);

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
    Buffer get_server_internal_buffer()
    {
        return dsm_->get_server_buffer();
    }

    void handle_request_messages(const char *msg_buf,
                                 size_t msg_nr,
                                 CoroContext *ctx = nullptr);

    size_t reliable_try_recv(size_t from_mid, char *ibuf, size_t limit = 1)
    {
        return dsm_->reliable_try_recv(from_mid, ibuf, limit);
    }
    Buffer get_rdma_buffer_8B()
    {
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

    Buffer get_rdma_buffer(size_t size)
    {
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
    constexpr size_t admin_mid() const
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
    constexpr static size_t kGuessActiveLeasePerCoro = 64;
    constexpr static size_t kClientRdmaBufferSize = 8 * define::KB;
    constexpr static size_t kLeaseContextNr =
        kMaxCoroNr * kGuessActiveLeasePerCoro;
    static_assert(
        kLeaseContextNr <
        std::numeric_limits<decltype(AcquireResponse::lease_id)>::max());
    constexpr static size_t kServerCoroNr = kMaxCoroNr;
    constexpr static size_t kProtectionRegionPerThreadNr =
        NR_DIRECTORY * kMaxCoroNr * kGuessActiveLeasePerCoro;
    constexpr static size_t kTotalProtectionRegionNr =
        kProtectionRegionPerThreadNr * kMaxAppThread;

    void explain(const PatronusConfig &);

    constexpr static id_t key_hash(id_t key)
    {
        // TODO(patronus): should use real hash to distribute the keys.
        return key;
    }

    ibv_mw *get_mw(size_t dirID)
    {
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
    size_t handle_rdma_finishes(ibv_wc *buffer,
                                size_t rdma_nr,
                                coro_t *o_coro_buf,
                                std::set<std::pair<size_t, size_t>> &recov);
    void signal_server_to_recover_qp(size_t node_id, size_t dir_id);
    void handle_request_acquire(AcquireRequest *, CoroContext *ctx);
    void handle_request_lease_modify(LeaseModifyRequest *, CoroContext *ctx);
    void handle_response_lease_extend(LeaseModifyResponse *);
    void handle_response_lease_upgrade(LeaseModifyResponse *);

    // for servers
    void handle_response_acquire(AcquireResponse *);
    void handle_response_lease_modify(LeaseModifyResponse *);
    void handle_admin_exit(AdminRequest *req, CoroContext *ctx);
    void handle_admin_recover(AdminRequest *req, CoroContext *ctx);
    void handle_admin_barrier(AdminRequest *req, CoroContext *ctx);
    void handle_request_lease_relinquish(LeaseModifyRequest *,
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
                       uint8_t flag,
                       CoroContext *ctx = nullptr);

    // server coroutines
    void server_coro_master(CoroYield &yield, size_t mid, uint64_t key);
    void server_coro_worker(coro_t coro_id, CoroYield &yield, uint64_t key);

    // helpers, actual impls
    Lease get_lease_impl(uint16_t node_id,
                         uint16_t dir_id,
                         id_t key,
                         size_t size,
                         time::ns_t ns,
                         RequestType type,
                         uint8_t flag,
                         CoroContext *ctx = nullptr);
    RetCode buffer_rw_impl(Lease &lease,
                           char *iobuf,
                           size_t size,
                           size_t offset,
                           bool is_read,
                           CoroContext *ctx = nullptr);
    inline bool already_passed_ddl(time::PatronusTime time) const;
    // flag should be RWFlag
    inline RetCode handle_rwcas_flag(Lease &lease,
                                     uint8_t flag,
                                     CoroContext *ctx);
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
                            CoroContext *ctx = nullptr);
    inline RetCode extend_impl(Lease &lease,
                               size_t extend_unit_nr,
                               uint8_t flag,
                               CoroContext *ctx);
    RetCode cas_impl(char *iobuf,
                     size_t node_id,
                     size_t dir_id,
                     uint32_t rkey,
                     uint64_t remote_addr,
                     uint64_t compare,
                     uint64_t swap,
                     uint16_t wr_prefix,
                     CoroContext *ctx = nullptr);
    Lease lease_modify_impl(Lease &lease,
                            uint64_t hint,
                            RequestType type,
                            time::ns_t ns,
                            uint8_t flag /* LeaseModificationFlag */,
                            CoroContext *ctx = nullptr);
    inline void fill_bind_mw_wr(ibv_send_wr &wr,
                                ibv_send_wr *next_wr,
                                ibv_mw *mw,
                                ibv_mr *mr,
                                uint64_t addr,
                                size_t length,
                                int access_flag);
    RetCode maybe_auto_extend(Lease &lease, CoroContext *ctx = nullptr);

    inline bool valid_lease_buffer_offset(size_t buffer_offset) const;
    inline bool valid_total_buffer_offset(size_t buffer_offset) const;

    void validate_buffers();

    /**
     * @brief call me only if u know what u are doing.
     * Do not call out of ctor of Patronus
     */
    void internal_barrier();

    // owned by both
    DSM::pointer dsm_;
    time::TimeSyncer::pointer time_syncer_;
    static thread_local std::unique_ptr<ThreadUnsafeBufferPool<kMessageSize>>
        rdma_message_buffer_pool_;
    PatronusLockManager lock_manager_;

    // owned by client threads
    static thread_local ThreadUnsafePool<RpcContext, kMaxCoroNr> rpc_context_;
    static thread_local ThreadUnsafePool<RWContext, kMaxCoroNr> rw_context_;
    static thread_local std::unique_ptr<
        ThreadUnsafeBufferPool<kClientRdmaBufferSize>>
        rdma_client_buffer_;
    static thread_local std::unique_ptr<ThreadUnsafeBufferPool<8>>
        rdma_client_buffer_8B_;
    static thread_local char *client_rdma_buffer_;
    static thread_local size_t client_rdma_buffer_size_;

    static thread_local bool is_server_;
    static thread_local bool is_client_;

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
    static thread_local size_t allocated_mw_nr_;
    constexpr static uint32_t magic = 0b1010101010;
    constexpr static uint16_t mask = 0b1111111111;

    // for admin management
    std::array<std::array<std::atomic<bool>, MAX_MACHINE>, kMaxSyncKey>
        finished_;
    std::array<std::atomic<bool>, kMaxSyncKey> should_exit_{false};

    // for barrier
    std::unordered_map<uint64_t, std::set<uint64_t>> barrier_;
    std::mutex barrier_mu_;

    std::chrono::time_point<std::chrono::steady_clock> finish_time_sync_now_;
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

Lease Patronus::alloc(uint16_t node_id,
                      uint16_t dir_id,
                      size_t size,
                      uint64_t hint,
                      CoroContext *ctx)
{
    auto flag = (uint8_t) AcquireRequestFlag::kNoGc |
                (uint8_t) AcquireRequestFlag::kOnlyAllocation;
    return get_lease_impl(node_id,
                          dir_id,
                          hint /* key & hint */,
                          size,
                          0,
                          RequestType::kAcquireWLease,
                          flag,
                          ctx);
}

// gaddr is buffer offset
Lease Patronus::get_rlease(GlobalAddress gaddr,
                           uint16_t dir_id,
                           size_t size,
                           std::chrono::nanoseconds chrono_ns,
                           uint8_t flag,
                           CoroContext *ctx)
{
    debug_validate_acquire_request_flag(flag);
    bool only_alloc = flag & (uint8_t) AcquireRequestFlag::kOnlyAllocation;
    DCHECK(!only_alloc) << "Please use Patronus::alloc";

    auto node_id = gaddr.nodeID;

    auto ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(chrono_ns).count();
    return get_lease_impl(node_id,
                          dir_id,
                          gaddr.offset,
                          size,
                          ns,
                          RequestType::kAcquireRLease,
                          flag,
                          ctx);
}

// gaddr is buffer offset
Lease Patronus::get_wlease(GlobalAddress gaddr,
                           uint16_t dir_id,
                           size_t size,
                           std::chrono::nanoseconds chrono_ns,
                           uint8_t flag,
                           CoroContext *ctx)
{
    debug_validate_acquire_request_flag(flag);
    auto node_id = gaddr.nodeID;

    auto ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(chrono_ns).count();
    return get_lease_impl(node_id,
                          dir_id,
                          gaddr.offset,
                          size,
                          ns,
                          RequestType::kAcquireWLease,
                          flag,
                          ctx);
}

Lease Patronus::upgrade(Lease &lease, uint8_t flag, CoroContext *ctx)
{
    debug_validate_lease_modify_flag(flag);
    // return lease_modify_impl(
    //     lease, RequestType::kUpgrade, 0 /* term */, flag, ctx);
    CHECK(false) << "deprecated " << lease << flag << pre_coro_ctx(ctx);
}
RetCode Patronus::extend_impl(Lease &lease,
                              size_t extend_unit_nr,
                              uint8_t flag,
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

    if (likely(cas_ec == RetCode::kOk))
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
RetCode Patronus::extend(Lease &lease,
                         std::chrono::nanoseconds chrono_ns,
                         uint8_t flag,
                         CoroContext *ctx)
{
    debug_validate_lease_modify_flag(flag);
    auto ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(chrono_ns).count();
    auto ns_per_unit = lease.ns_per_unit_;
    // round up divide
    auto extend_unit_nr = (ns + ns_per_unit - 1) / ns_per_unit;

    auto patronus_now = time_syncer_->patronus_now();
    auto patronus_ddl = lease.ddl_term();
    auto epsilon = time_syncer_->epsilon();
    time::ns_t diff_ns = patronus_ddl - patronus_now;
    bool already_pass_ddl =
        diff_ns < 0 + epsilon + time::TimeSyncer::kCommunicationLatencyNs;
    if (unlikely(already_pass_ddl))
    {
        // already pass DDL, no need to extend.
        DVLOG(4) << "[patronus][maybe-extend] Assume DDL passed (not extend). "
                    "lease ddl: "
                 << patronus_ddl << ", patronus_now: " << patronus_now
                 << ", diff_ns: " << diff_ns << " < 0 + epsilon(" << epsilon
                 << ") + CommunicationLatencyNs("
                 << time::TimeSyncer::kCommunicationLatencyNs
                 << "). coro: " << (ctx ? *ctx : nullctx)
                 << ". Now lease: " << lease;
        return RetCode::kLeaseLocalExpiredErr;
    }

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

    auto flag = (uint8_t) LeaseModifyFlag::kNoRelinquishUnbind |
                (uint8_t) LeaseModifyFlag::kOnlyDeallocation;
    lease_modify_impl(
        lease, hint, RequestType::kRelinquish, 0 /* term */, flag, ctx);
}
void Patronus::relinquish(Lease &lease,
                          uint64_t hint,
                          uint8_t flag,
                          CoroContext *ctx)
{
    bool only_dealloc = flag & (uint8_t) LeaseModifyFlag::kOnlyDeallocation;
    DCHECK(!only_dealloc) << "Please use Patronus::dealloc instead";

    debug_validate_lease_modify_flag(flag);
    // TODO(Patronus): the term is set to 0 here.
    lease_modify_impl(
        lease, hint, RequestType::kRelinquish, 0 /* term */, flag, ctx);
}

RetCode Patronus::validate_lease([[maybe_unused]] const Lease &lease)
{
    auto lease_patronus_ddl = lease.ddl_term();
    auto patronus_now = DCHECK_NOTNULL(time_syncer_)->patronus_now();
    auto ret = time_syncer_->definitely_lt(patronus_now, lease_patronus_ddl)
                   ? RetCode::kOk
                   : RetCode::kLeaseLocalExpiredErr;
    DVLOG(5) << "[patronus][validate_lease] patronus_now: " << patronus_now
             << ", ddl: " << lease_patronus_ddl << ", ret: " << ret;
    return ret;
}

RetCode Patronus::handle_rwcas_flag(Lease &lease,
                                    uint8_t flag,
                                    CoroContext *ctx)
{
    RetCode ec = RetCode::kOk;

    bool no_check;
    if (lease.no_gc_)
    {
        // does not need to check, because the lease will always be valid
        no_check = true;
    }
    else
    {
        no_check = flag & (uint8_t) RWFlag::kNoLocalExpireCheck;
    }

    if (likely(!no_check))
    {
        if ((ec = validate_lease(lease)) != RetCode::kOk)
        {
            return ec;
        }
    }
    bool with_auto_extend = flag & (uint8_t) RWFlag::kWithAutoExtend;
    if (with_auto_extend)
    {
        maybe_auto_extend(lease, ctx);
    }
    bool reserved = flag & (uint8_t) RWFlag::kReserved;
    DCHECK(!reserved);
    return RetCode::kOk;
}

RetCode Patronus::read(Lease &lease,
                       char *obuf,
                       size_t size,
                       size_t offset,
                       uint8_t flag,
                       CoroContext *ctx)
{
    RetCode ec = RetCode::kOk;
    if ((ec = handle_rwcas_flag(lease, flag, ctx)) != RetCode::kOk)
    {
        return ec;
    }

    bool with_cache = flag & (uint8_t) RWFlag::kWithCache;
    if (with_cache)
    {
        if (lease.cache_query(offset, size, obuf))
        {
            // cache hit
            return RetCode::kOk;
        }
    }

    return buffer_rw_impl(lease, obuf, size, offset, true /* is_read */, ctx);
}
RetCode Patronus::write(Lease &lease,
                        const char *ibuf,
                        size_t size,
                        size_t offset,
                        uint8_t flag,
                        CoroContext *ctx)
{
    auto ec = handle_rwcas_flag(lease, flag, ctx);
    if (unlikely(ec != RetCode::kOk))
    {
        return ec;
    }
    ec = buffer_rw_impl(
        lease, (char *) ibuf, size, offset, false /* is_read */, ctx);

    bool with_cache = flag & (uint8_t) RWFlag::kWithCache;
    if (ec == RetCode::kOk && with_cache)
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
                      uint8_t flag,
                      CoroContext *ctx)
{
    auto ec = handle_rwcas_flag(lease, flag, ctx);
    if (unlikely(ec != RetCode::kOk))
    {
        return ec;
    }
    ec = buffer_cas_impl(lease, iobuf, offset, compare, swap, ctx);
    bool with_cache = flag & (uint8_t) RWFlag::kWithCache;
    if (ec == RetCode::kOk && with_cache)
    {
        DCHECK_GE(sizeof(swap), 8);
        lease.cache_insert(offset, 8 /* size */, (const char *) swap);
    }
    return ec;
}

void Patronus::fill_bind_mw_wr(ibv_send_wr &wr,
                               ibv_send_wr *next_wr,
                               ibv_mw *mw,
                               ibv_mr *mr,
                               uint64_t addr,
                               size_t length,
                               int access_flag)
{
    wr.next = next_wr;
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

void Patronus::relinquish_write(Lease &lease, CoroContext *ctx)
{
    auto offset = offsetof(ProtectionRegion, meta) +
                  offsetof(ProtectionRegionMeta, relinquished);
    auto rdma_buffer = get_rdma_buffer(sizeof(small_bit_t));
    DCHECK_LE(sizeof(small_bit_t), rdma_buffer.size);
    *(small_bit_t *) rdma_buffer.buffer = 1;

    DVLOG(4) << "[patronus] relinquish write. write to "
                "ProtectionRegionMeta::relinquished at offset "
             << offset;

    protection_region_rw_impl(lease,
                              rdma_buffer.buffer,
                              sizeof(ProtectionRegionMeta::relinquished),
                              offset,
                              false /* is_read */,
                              ctx);

    put_rdma_buffer(rdma_buffer);
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
           << Patronus::kMessageSize << " B. ";
    }
    if (p.rdma_client_buffer_)
    {
        os << "rdma_client_buffer(thread): " << p.rdma_client_buffer_->size()
           << " nr with size " << Patronus::kClientRdmaBufferSize << " B. ";
    }
    if (p.rdma_client_buffer_8B_)
    {
        os << "rdma_client_buffer_8B(thread): "
           << p.rdma_client_buffer_8B_->size() << " nr with size 8 B. ";
    }

    os << "rpc_context(thread): " << p.rpc_context_.size() << " with "
       << sizeof(RpcContext) << " B. ";
    os << "rw_context(thread): " << p.rw_context_.size() << " with "
       << sizeof(RWContext) << " B. ";
    os << "lease_context(thread): " << p.lease_context_.size() << " with "
       << sizeof(LeaseContext) << " B. ";
    if (p.protection_region_pool_)
    {
        os << "pr_pool(thread): " << p.protection_region_pool_->size()
           << " with " << sizeof(ProtectionRegion) << " B. ";
    }
    os << "}";

    return os;
}

}  // namespace patronus

#endif