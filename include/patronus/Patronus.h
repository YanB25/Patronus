#pragma once
#ifndef PATRONUS_H_
#define PATRONUS_H_

#include <set>
#include <unordered_set>

#include "DSM.h"
#include "Result.h"
#include "patronus/Coro.h"
#include "patronus/DDLManager.h"
#include "patronus/Lease.h"
#include "patronus/ProtectionRegion.h"
#include "patronus/TimeSyncer.h"
#include "patronus/Type.h"
#include "util/Debug.h"

namespace patronus
{
class Patronus;
using KeyLocator = std::function<uint64_t(key_t)>;
static KeyLocator identity_locator = [](key_t key) -> uint64_t {
    return (uint64_t) key;
};
struct PatronusConfig
{
    size_t machine_nr{0};
    size_t buffer_size{kDSMCacheSize};
    KeyLocator key_locator{identity_locator};
    // for sync time
    bool skip_sync_time{false};
    size_t time_parent_node_id{0};
};
inline std::ostream &operator<<(std::ostream &os, const PatronusConfig &conf)
{
    os << "{PatronusConfig machine_nr: " << conf.machine_nr
       << ", buffer_size: " << conf.buffer_size
       << ", skip_sync_time: " << conf.skip_sync_time << "}";
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
};

enum class RWFlag : uint8_t
{
    kDisableLocalExpireCheck = 1 << 0,
    kReserved = 1 << 1,
};
class Patronus
{
public:
    using pointer = std::shared_ptr<Patronus>;

    constexpr static size_t kMaxCoroNr = define::kMaxCoroNr;
    constexpr static size_t kMessageSize = ReliableConnection::kMessageSize;
    // TODO(patronus): try to tune this parameter up.
    constexpr static size_t kMwPoolSizePerThread = 50 * define::K;

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
     * @param node_id the id of target node
     * @param dir_id  the dir_id.
     * @param key the unique key to identify the object / address you want to
     * access. Call @reg_locator to tell Patronus how to map the key to the
     * address
     * @param size the length of the object / address
     * @param term the length of term requesting for protection
     * @param ctx sync call if ctx is nullptr. Otherwise coroutine context.
     * @return Read Lease
     */
    inline Lease get_rlease(uint16_t node_id,
                            uint16_t dir_id,
                            id_t key,
                            size_t size,
                            time::ns_t ns,
                            uint8_t flag /* AcquireRequestFlag */,
                            CoroContext *ctx = nullptr);
    inline Lease get_wlease(uint16_t node_id,
                            uint16_t dir_id,
                            id_t key,
                            size_t size,
                            time::ns_t ns,
                            uint8_t flag /* AcquireRequestFlag */,
                            CoroContext *ctx = nullptr);
    inline Lease upgrade(Lease &lease, CoroContext *ctx = nullptr);
    inline Lease extend(Lease &lease,
                        time::ns_t ns,
                        CoroContext *ctx = nullptr);
    inline void relinquish(Lease &lease, CoroContext *ctx = nullptr);
    inline void relinquish_write(Lease &lease, CoroContext *ctx = nullptr);
    inline bool read(Lease &lease,
                     char *obuf,
                     size_t size,
                     size_t offset,
                     uint8_t flag /* RWFlag */,
                     CoroContext *ctx = nullptr);
    inline bool write(Lease &lease,
                      const char *ibuf,
                      size_t size,
                      size_t offset,
                      uint8_t flag /* RWFlag */,
                      CoroContext *ctx = nullptr);
    inline bool pingpong(uint16_t node_id,
                         uint16_t dir_id,
                         id_t key,
                         size_t size,
                         time::term_t term,
                         CoroContext *ctx = nullptr);
    /**
     * @brief After all the node call this function, @should_exit() will return
     * true
     *
     * @param ctx
     */
    void finished([[maybe_unused]] CoroContext *ctx = nullptr);
    bool should_exit() const
    {
        return should_exit_.load(std::memory_order_relaxed);
    }

    /**
     * @brief give the server thread to Patronus.
     *
     */
    void server_serve();

    /**
     * @brief handling admin messages while joining the threads.
     *
     * @param threads
     */
    void wait_join(std::vector<std::thread> &threads);

    size_t try_get_client_continue_coros(size_t mid,
                                         coro_t *coro_buf,
                                         size_t limit);

    /**
     * @brief Any thread should call this function before calling any function
     * of Patronus
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

    Buffer get_rdma_buffer()
    {
        return Buffer((char *) rdma_client_buffer_->get(),
                      kClientRdmaBufferSize);
    }
    void put_rdma_buffer(void *buf)
    {
        rdma_client_buffer_->put(buf);
    }

    DSM::pointer get_dsm()
    {
        return dsm_;
    }

    const time::TimeSyncer &time_syncer() const
    {
        return *time_syncer_;
    }

private:
    // How many leases on average may a tenant hold?
    // It determines how much resources we should reserve
    constexpr static size_t kGuessActiveLeasePerCoro = 16;
    constexpr static size_t kClientRdmaBufferSize = 4 * define::KB;
    constexpr static size_t kLeaseContextNr =
        kMaxCoroNr * kGuessActiveLeasePerCoro;
    static_assert(
        kLeaseContextNr <
        std::numeric_limits<decltype(AcquireResponse::lease_id)>::max());
    constexpr static size_t kServerCoroNr = kMaxCoroNr;
    constexpr static size_t kProtectionRegionPerThreadNr =
        NR_DIRECTORY * kMaxCoroNr * kGuessActiveLeasePerCoro;
    constexpr static size_t kTotalProtectionRegionNr =
        kProtectionRegionPerThreadNr * MAX_APP_THREAD;

    void explain(const PatronusConfig &);
    void reg_locator(const KeyLocator &locator = identity_locator)
    {
        locator_ = locator;
    }

    ibv_mw *get_mw(size_t dirID)
    {
        auto *ret = mw_pool_[dirID].front();
        mw_pool_[dirID].pop();
        return DCHECK_NOTNULL(ret);
    }
    void put_mw(size_t dirID, ibv_mw *mw)
    {
        mw_pool_[dirID].push(mw);
    }
    char *get_rdma_message_buffer()
    {
        return (char *) rdma_message_buffer_pool_->get();
    }
    void debug_valid_rdma_buffer(const void *buf)
    {
        rdma_client_buffer_->debug_validity_check(buf);
    }
    void put_rdma_message_buffer(char *buf)
    {
        rdma_message_buffer_pool_->put(buf);
    }
    RpcContext *get_rpc_context()
    {
        return rpc_context_.get();
    }
    void put_rpc_context(RpcContext *ctx)
    {
        rpc_context_.put(ctx);
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
        rw_context_.put(ctx);
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
        ctx->valid = false;
        lease_context_.put(ctx);
    }
    ProtectionRegion *get_protection_region()
    {
        auto *ret = DCHECK_NOTNULL(protection_region_pool_->get());
        return (ProtectionRegion *) ret;
    }
    void put_protection_region(ProtectionRegion *p)
    {
        protection_region_pool_->put(p);
    }
    ProtectionRegion *get_protection_region(size_t id)
    {
        auto *ret = protection_region_pool_->id_to_buf(id);
        return (ProtectionRegion *) ret;
    }

    // clang-format off
    /**
     * The layout of dsm_->uget_server_reserve_buffer():
     * [required_time_sync_size()] [required_protection_region_size()]
     * 
     * ^-- get_time_sync_buffer()
     *                             ^-- get_protection_region_buffer()
     */
    // clang-format on
    static size_t required_dsm_reserve_size()
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
    void handle_request_lease_relinquish(LeaseModifyRequest *,
                                         CoroContext *ctx);
    void handle_request_lease_extend(LeaseModifyRequest *, CoroContext *ctx);
    void handle_request_lease_upgrade(LeaseModifyRequest *, CoroContext *ctx);

    void task_gc_lease(uint64_t lease_id, ClientID cid, CoroContext *ctx = nullptr);

    // server coroutines
    void server_coro_master(CoroYield &yield);
    void server_coro_worker(coro_t coro_id, CoroYield &yield);

    // helpers, actual impls
    Lease get_lease_impl(uint16_t node_id,
                         uint16_t dir_id,
                         id_t key,
                         size_t size,
                         time::ns_t ns,
                         RequestType type,
                         uint8_t flag,
                         CoroContext *ctx = nullptr);
    bool buffer_rw_impl(Lease &lease,
                        char *iobuf,
                        size_t size,
                        size_t offset,
                        bool is_read,
                        CoroContext *ctx = nullptr);
    inline bool validate_lease(const Lease &lease);
    bool protection_region_rw_impl(Lease &lease,
                                   char *io_buf,
                                   size_t size,
                                   size_t offset,
                                   bool is_read,
                                   CoroContext *ctx = nullptr);
    bool read_write_impl(char *iobuf,
                         size_t size,
                         size_t node_id,
                         size_t dir_id,
                         uint32_t rkey,
                         size_t remote_addr,
                         bool is_read,
                         uint16_t wrid_prefix,
                         CoroContext *ctx = nullptr);
    Lease lease_modify_impl(Lease &lease,
                            RequestType type,
                            time::ns_t ns,
                            CoroContext *ctx = nullptr);
    inline void fill_bind_mw_wr(ibv_send_wr &wr,
                                ibv_send_wr *next_wr,
                                ibv_mw *mw,
                                ibv_mr *mr,
                                uint64_t addr,
                                size_t length,
                                int access_flag);

    // owned by both
    DSM::pointer dsm_;
    time::TimeSyncer::pointer time_syncer_;
    static thread_local std::unique_ptr<ThreadUnsafeBufferPool<kMessageSize>>
        rdma_message_buffer_pool_;

    // owned by client threads
    static thread_local ThreadUnsafePool<RpcContext, kMaxCoroNr> rpc_context_;
    static thread_local ThreadUnsafePool<RWContext, kMaxCoroNr> rw_context_;
    static thread_local std::unique_ptr<
        ThreadUnsafeBufferPool<kClientRdmaBufferSize>>
        rdma_client_buffer_;

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
    std::array<std::atomic<bool>, MAX_MACHINE> exits_;
    std::atomic<bool> should_exit_{false};

    // for user interfaces
    KeyLocator locator_;
};

Lease Patronus::get_rlease(uint16_t node_id,
                           uint16_t dir_id,
                           id_t key,
                           size_t size,
                           time::ns_t ns,
                           uint8_t flag,
                           CoroContext *ctx)
{
    return get_lease_impl(
        node_id, dir_id, key, size, ns, RequestType::kAcquireRLease, flag, ctx);
}
Lease Patronus::get_wlease(uint16_t node_id,
                           uint16_t dir_id,
                           id_t key,
                           size_t size,
                           time::ns_t ns,
                           uint8_t flag,
                           CoroContext *ctx)
{
    return get_lease_impl(
        node_id, dir_id, key, size, ns, RequestType::kAcquireWLease, flag, ctx);
}

Lease Patronus::upgrade(Lease &lease, CoroContext *ctx)
{
    return lease_modify_impl(lease, RequestType::kUpgrade, 0 /* term */, ctx);
}
Lease Patronus::extend(Lease &lease, time::ns_t ns, CoroContext *ctx)
{
    return lease_modify_impl(lease, RequestType::kExtend, ns, ctx);
}
void Patronus::relinquish(Lease &lease, CoroContext *ctx)
{
    lease_modify_impl(lease, RequestType::kRelinquish, 0 /* term */, ctx);
}

bool Patronus::validate_lease([[maybe_unused]] const Lease &lease)
{
    auto lease_patronus_ddl = lease.ddl_term();
    auto patronus_now = time_syncer_->patronus_now();
    return time_syncer_->definitely_lt(patronus_now, lease_patronus_ddl);
}

bool Patronus::read(Lease &lease,
                    char *obuf,
                    size_t size,
                    size_t offset,
                    uint8_t flag,
                    CoroContext *ctx)
{
    bool disable_check = flag & (uint8_t) RWFlag::kDisableLocalExpireCheck;
    if (likely(!disable_check))
    {
        if (!validate_lease(lease))
        {
            return false;
        }
    }
    bool reserved = flag & (uint8_t) RWFlag::kReserved;
    DCHECK(!reserved);
    return buffer_rw_impl(lease, obuf, size, offset, true /* is_read */, ctx);
}
bool Patronus::write(Lease &lease,
                     const char *ibuf,
                     size_t size,
                     size_t offset,
                     uint8_t flag,
                     CoroContext *ctx)
{
    bool disable_check = flag & (uint8_t) RWFlag::kDisableLocalExpireCheck;
    if (likely(!disable_check))
    {
        if (!validate_lease(lease))
        {
            return false;
        }
    }
    bool reserved = flag & (uint8_t) RWFlag::kReserved;
    DCHECK(!reserved);
    return buffer_rw_impl(
        lease, (char *) ibuf, size, offset, false /* is_read */, ctx);
}

bool Patronus::pingpong(uint16_t node_id,
                        uint16_t dir_id,
                        id_t key,
                        size_t size,
                        time::term_t term,
                        CoroContext *ctx)
{
    auto lease = get_lease_impl(node_id,
                                dir_id,
                                key,
                                size,
                                term,
                                RequestType::kAcquireNoLease,
                                (uint8_t) AcquireRequestFlag::kNoGc,
                                ctx);
    return lease.success();
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
    auto rdma_buffer = get_rdma_buffer();
    DCHECK_LT(sizeof(small_bit_t), rdma_buffer.size);
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

    put_rdma_buffer(rdma_buffer.buffer);
}

}  // namespace patronus

#endif