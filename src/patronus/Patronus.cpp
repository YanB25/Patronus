#include "patronus/Patronus.h"

#include "Util.h"
#include "patronus/IBOut.h"
#include "patronus/Time.h"
#include "util/Debug.h"

namespace patronus
{
thread_local std::unique_ptr<ThreadUnsafeBufferPool<Patronus::kMessageSize>>
    Patronus::rdma_message_buffer_pool_;
thread_local std::unique_ptr<
    ThreadUnsafeBufferPool<Patronus::kClientRdmaBufferSize>>
    Patronus::rdma_client_buffer_;
thread_local std::unique_ptr<ThreadUnsafeBufferPool<8>>
    Patronus::rdma_client_buffer_8B_;
thread_local ThreadUnsafePool<RpcContext, Patronus::kMaxCoroNr>
    Patronus::rpc_context_;
thread_local ThreadUnsafePool<RWContext, 2 * Patronus::kMaxCoroNr>
    Patronus::rw_context_;
thread_local std::queue<ibv_mw *> Patronus::mw_pool_[NR_DIRECTORY];
thread_local ThreadUnsafePool<LeaseContext, Patronus::kLeaseContextNr>
    Patronus::lease_context_;
thread_local ServerCoroContext Patronus::server_coro_ctx_;
thread_local std::unique_ptr<ThreadUnsafeBufferPool<sizeof(ProtectionRegion)>>
    Patronus::protection_region_pool_;
thread_local DDLManager Patronus::ddl_manager_;
thread_local size_t Patronus::allocated_mw_nr_;
thread_local mem::SlabAllocator::pointer Patronus::default_allocator_;
thread_local std::unordered_map<uint64_t, mem::IAllocator::pointer>
    Patronus::reg_allocators_;
thread_local char *Patronus::client_rdma_buffer_{nullptr};
thread_local size_t Patronus::client_rdma_buffer_size_{0};
thread_local bool Patronus::is_server_{false};
thread_local bool Patronus::is_client_{false};

// clang-format off
/**
 * The layout of dsm_->get_server_buffer():
 * [conf_.lease_buffer_size] [conf_.alloc_buffer_size] [conf_.reserved_buffer_size]
 *                                                     ^-- get_user_reserved_buffer()
 * 
 * The layout of dsm_->get_server_reserved_buffer():
 * @see required_dsm_reserve_size()
 * 
 */
// clang-format on
Patronus::Patronus(const PatronusConfig &conf) : conf_(conf)
{
    DSMConfig dsm_config;
    dsm_config.machineNR = conf.machine_nr;
    dsm_config.dsmReserveSize = required_dsm_reserve_size();
    dsm_config.dsmSize = conf.total_buffer_size();
    dsm_ = DSM::getInstance(dsm_config);

    // validate dsm
    auto internal_buf = dsm_->get_server_buffer();
    auto reserve_buf = dsm_->get_server_reserved_buffer();
    CHECK_GE(internal_buf.size, conf.total_buffer_size())
        << "** dsm should allocate DSM buffer at least what Patronus requires";
    CHECK_GE(reserve_buf.size, required_dsm_reserve_size())
        << "**dsm should provide reserved buffer at least what Patronus "
           "requries";

    internal_barrier();
    DVLOG(1) << "[patronus] Barrier: ctor of DSM";

    // for initial of allocator
    // the actual initialization will be postponed to registerServerThread
    allocator_buf_addr_ = internal_buf.buffer + conf.lease_buffer_size;
    allocator_buf_size_ = conf.alloc_buffer_size;

    // for time syncer
    auto time_sync_buffer = get_time_sync_buffer();
    auto time_sync_offset = dsm_->addr_to_dsm_offset(time_sync_buffer.buffer);
    GlobalAddress gaddr;
    gaddr.nodeID = conf.time_parent_node_id;
    gaddr.offset = time_sync_offset;
    time_syncer_ = time::TimeSyncer::new_instance(
        dsm_, gaddr, time_sync_buffer.buffer, time_sync_buffer.size);

    time_syncer_->sync();
    finish_time_sync_now_ = std::chrono::steady_clock::now();

    explain(conf);
    if constexpr (debug())
    {
        validate_buffers();
    }
}
Patronus::~Patronus()
{
    auto now = std::chrono::steady_clock::now();
    auto elaps_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        now - finish_time_sync_now_)
                        .count();
    DLOG(INFO) << "[patronus] Elaps " << 1.0 * elaps_ms << " ms, or "
               << 1.0 * elaps_ms / 1000 << " s";

    for (ibv_mw *mw : allocated_mws_)
    {
        dsm_->free_mw(mw);
    }
}

void Patronus::explain(const PatronusConfig &conf)
{
    LOG(INFO) << "[patronus] config: " << conf;
}

void Patronus::barrier(uint64_t key)
{
    auto tid = get_thread_id();
    auto from_mid = tid;
    auto to_mid = admin_mid();

    char *rdma_buf = get_rdma_message_buffer();
    auto *msg = (AdminRequest *) rdma_buf;
    msg->type = RequestType::kAdmin;
    msg->flag = (uint8_t) AdminFlag::kAdminBarrier;
    msg->cid.node_id = get_node_id();
    msg->cid.thread_id = tid;
    msg->cid.mid = from_mid;
    msg->data = key;
    msg->cid.coro_id = kNotACoro;

    if constexpr (debug())
    {
        msg->digest = 0;
        msg->digest = djb2_digest(msg, sizeof(AdminRequest));
    }

    for (size_t i = 0; i < dsm_->getClusterSize(); ++i)
    {
        if (i == dsm_->get_node_id())
        {
            continue;
        }
        dsm_->reliable_send(rdma_buf, sizeof(AdminRequest), i, to_mid);
    }

    // TODO(patronus): this may have problem
    // if the buffer is reused when NIC is DMA-ing
    put_rdma_message_buffer(rdma_buf);

    {
        std::lock_guard<std::mutex> lk(barrier_mu_);
        barrier_[key].insert(get_node_id());
    }

    // wait for finish
    while (true)
    {
        {
            std::lock_guard<std::mutex> lk(barrier_mu_);
            const auto &vec = barrier_[key];
            if (vec.size() == dsm_->getClusterSize())
            {
                return;
            }
        }
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(10us);
    }
}

void Patronus::internal_barrier()
{
    auto mid = admin_mid();
    for (size_t i = 0; i < dsm_->getClusterSize(); ++i)
    {
        if (i == dsm_->get_node_id())
        {
            continue;
        }
        dsm_->reliable_send(nullptr, 0, i, mid);
    }
    for (size_t i = 0; i < dsm_->getClusterSize(); ++i)
    {
        if (i == dsm_->get_node_id())
        {
            continue;
        }
        dsm_->reliable_recv(mid, nullptr, 1);
    }
}

/**
 * @brief The actual implementation (dirty and complex but versatile) to get the
 * lease.
 *
 * If allocation is on, the @key field is used as @hint for the allocator.
 * Otherwise, the @key is provided to the locator to locate the address.
 */
Lease Patronus::get_lease_impl(uint16_t node_id,
                               uint16_t dir_id,
                               uint64_t key_or_hint,
                               size_t size,
                               time::ns_t ns,
                               RequestType type,
                               uint8_t flag,
                               CoroContext *ctx)
{
    DCHECK(type == RequestType::kAcquireNoLease ||
           type == RequestType::kAcquireWLease ||
           type == RequestType::kAcquireRLease);

    bool enable_trace = false;
    trace_t trace = 0;
    if constexpr (config::kEnableTrace)
    {
        if (likely(ctx != nullptr))
        {
            if (likely(ctx->trace() != 0))
            {
                enable_trace = true;
                trace = ctx->trace();
            }
        }
    }

    auto tid = get_thread_id();
    auto from_mid = tid;
    auto to_mid = dir_id;
    CHECK_EQ(from_mid, to_mid)
        << "** rmsg does not support cross mid communication";

    DCHECK_LT(from_mid, RMSG_MULTIPLEXING);
    DCHECK_LT(to_mid, RMSG_MULTIPLEXING);
    DCHECK_LT(dir_id, NR_DIRECTORY);
    DCHECK_LT(tid, kMaxAppThread);

    char *rdma_buf = get_rdma_message_buffer();
    auto *rpc_context = get_rpc_context();
    uint16_t rpc_ctx_id = get_rpc_context_id(rpc_context);

    Lease ret_lease;
    ret_lease.node_id_ = node_id;
    bool no_gc = flag & (uint8_t) AcquireRequestFlag::kNoGc;
    ret_lease.no_gc_ = no_gc;

    auto *msg = (AcquireRequest *) rdma_buf;
    msg->type = type;
    msg->cid.node_id = get_node_id();
    msg->cid.thread_id = tid;
    msg->cid.mid = from_mid;
    msg->cid.coro_id = ctx ? ctx->coro_id() : kNotACoro;
    msg->cid.rpc_ctx_id = rpc_ctx_id;
    msg->dir_id = dir_id;
    msg->key = key_or_hint;
    msg->size = size;
    msg->required_ns = ns;
    msg->trace = trace;
    msg->flag = flag;

    rpc_context->ret_lease = &ret_lease;
    rpc_context->origin_lease = nullptr;
    rpc_context->dir_id = dir_id;
    rpc_context->ready = false;
    rpc_context->request = (BaseMessage *) msg;

    if constexpr (debug())
    {
        msg->digest = 0;
        msg->digest = djb2_digest(msg, sizeof(AcquireRequest));
    }

    if (unlikely(enable_trace))
    {
        ctx->timer().pin("[get] Before send");
    }

    DVLOG(4) << "[patronus] get_lease for coro: " << (ctx ? *ctx : nullctx)
             << ", for key(or hint) " << key_or_hint << ", ns: " << ns
             << ", patronus_now: " << time_syncer_->patronus_now();
    dsm_->reliable_send(rdma_buf, sizeof(AcquireRequest), node_id, to_mid);

    if (unlikely(ctx == nullptr))
    {
        while (!rpc_context->ready.load(std::memory_order_acquire))
        {
            std::this_thread::yield();
        }
    }
    else
    {
        if (unlikely(enable_trace))
        {
            ctx->timer().pin("[get] Requested and yield");
        }

        ctx->yield_to_master();

        if (unlikely(enable_trace))
        {
            ctx->timer().pin("[get] Back and Lease prepared");
        }
    }
    DCHECK(rpc_context->ready) << "** Should have been ready when switch back "
                                  "to worker coro. ctx: "
                               << (ctx ? *ctx : nullctx);

    if constexpr (debug())
    {
        memset(rpc_context, 0, sizeof(RpcContext));
    }

    put_rpc_context(rpc_context);
    put_rdma_message_buffer(rdma_buf);

    if (no_gc)
    {
        ret_lease.ddl_term_ = time::PatronusTime::max();
    }

    return ret_lease;
}

RetCode Patronus::read_write_impl(char *iobuf,
                                  size_t size,
                                  size_t node_id,
                                  size_t dir_id,
                                  uint32_t rkey,
                                  uint64_t remote_addr,
                                  bool is_read,
                                  uint16_t wrid_prefix,
                                  CoroContext *ctx)
{
    bool ret = false;

    GlobalAddress gaddr;
    gaddr.nodeID = node_id;
    DCHECK_NE(gaddr.nodeID, get_node_id())
        << "make no sense to R/W local buffer with RDMA.";
    gaddr.offset = remote_addr;
    auto coro_id = ctx ? ctx->coro_id() : kNotACoro;
    if constexpr (debug())
    {
        debug_valid_rdma_buffer(iobuf);
    }

    auto *rw_context = get_rw_context();
    uint16_t rw_ctx_id = get_rw_context_id(rw_context);
    rw_context->success = &ret;
    rw_context->ready = false;
    rw_context->coro_id = coro_id;
    rw_context->target_node = gaddr.nodeID;
    rw_context->dir_id = dir_id;

    // already switch ctx if not null
    if (is_read)
    {
        dsm_->rkey_read(rkey,
                        iobuf,
                        gaddr,
                        size,
                        dir_id,
                        true,
                        ctx,
                        WRID(wrid_prefix, rw_ctx_id).val);
    }
    else
    {
        dsm_->rkey_write(rkey,
                         iobuf,
                         gaddr,
                         size,
                         dir_id,
                         true,
                         ctx,
                         WRID(wrid_prefix, rw_ctx_id).val);
    }

    if (unlikely(ctx == nullptr))
    {
        dsm_->poll_rdma_cq(1);
        rw_context->ready.store(true);
    }

    DCHECK(rw_context->ready)
        << "** Should have been ready when switching back to worker " << ctx
        << ", rw_ctx_id: " << rw_ctx_id << ", ready at "
        << (void *) &rw_context->ready;
    if constexpr (debug())
    {
        memset(rw_context, 0, sizeof(RWContext));
    }

    put_rw_context(rw_context);
    return ret ? RetCode::kOk : RetCode::kRdmaProtectionErr;
}

RetCode Patronus::buffer_rw_impl(Lease &lease,
                                 char *iobuf,
                                 size_t size,
                                 size_t offset,
                                 bool is_read,
                                 CoroContext *ctx)
{
    CHECK(lease.success());
    if (is_read)
    {
        CHECK(lease.is_readable());
    }
    else
    {
        CHECK(lease.is_writable());
    }
    uint32_t rkey = lease.cur_rkey_;
    uint64_t remote_addr = lease.base_addr_ + offset;
    DLOG_IF(INFO, config::kMonitorAddressConversion)
        << "[addr] patronus remote_addr: " << (void *) remote_addr
        << " (base: " << (void *) lease.base_addr_
        << ", offset: " << (void *) offset << ")";

    return read_write_impl(iobuf,
                           size,
                           lease.node_id_,
                           lease.dir_id_,
                           rkey,
                           remote_addr,
                           is_read,
                           WRID_PREFIX_PATRONUS_RW,
                           ctx);
}

RetCode Patronus::protection_region_rw_impl(Lease &lease,
                                            char *io_buf,
                                            size_t size,
                                            size_t offset,
                                            bool is_read,
                                            CoroContext *ctx)
{
    CHECK(lease.success());

    uint32_t rkey = lease.header_rkey_;
    uint64_t remote_addr = lease.header_addr_ + offset;
    VLOG(4) << "[patronus] protection_region_rw_impl. remote_addr: "
            << remote_addr << ", rkey: " << rkey
            << " (header_addr: " << lease.header_addr_ << ")";
    return read_write_impl(io_buf,
                           size,
                           lease.node_id_,
                           lease.dir_id_,
                           rkey,
                           remote_addr,
                           is_read,
                           WRID_PREFIX_PATRONUS_PR_RW,
                           ctx);
}
RetCode Patronus::buffer_cas_impl(Lease &lease,
                                  char *iobuf,
                                  size_t offset,
                                  uint64_t compare,
                                  uint64_t swap,
                                  CoroContext *ctx)
{
    CHECK(lease.success());
    uint32_t rkey = lease.cur_rkey();
    uint64_t remote_addr = lease.base_addr_ + offset;
    VLOG(4) << "[patronus] buffer_cas. remote_addr: " << remote_addr
            << ", rkey: " << rkey << " (base_addr: " << lease.base_addr_
            << ", remote_addr: " << remote_addr
            << "). coro: " << (ctx ? *ctx : nullctx);
    return cas_impl(iobuf,
                    lease.node_id_,
                    lease.dir_id_,
                    rkey,
                    remote_addr,
                    compare,
                    swap,
                    WRID_PREFIX_PATRONUS_CAS,
                    ctx);
}
RetCode Patronus::protection_region_cas_impl(Lease &lease,
                                             char *iobuf,
                                             size_t offset,
                                             uint64_t compare,
                                             uint64_t swap,
                                             CoroContext *ctx)
{
    CHECK(lease.success());

    uint32_t rkey = lease.header_rkey_;
    uint64_t remote_addr = lease.header_addr_ + offset;
    DVLOG(4) << "[patronus] protection_region_cas. remote_addr: " << remote_addr
             << ", rkey: " << rkey << " (header_addr: " << lease.header_addr_
             << "). coro: " << (ctx ? *ctx : nullctx);
    return cas_impl(iobuf,
                    lease.node_id_,
                    lease.dir_id_,
                    rkey,
                    remote_addr,
                    compare,
                    swap,
                    WRID_PREFIX_PATRONUS_PR_CAS,
                    ctx);
}

RetCode Patronus::cas_impl(char *iobuf /* old value fill here */,
                           size_t node_id,
                           size_t dir_id,
                           uint32_t rkey,
                           uint64_t remote_addr,
                           uint64_t compare,
                           uint64_t swap,
                           uint16_t wr_prefix,
                           CoroContext *ctx)
{
    bool ret = false;

    GlobalAddress gaddr;
    gaddr.nodeID = node_id;
    DCHECK_NE(gaddr.nodeID, get_node_id())
        << "make no sense to CAS local buffer with RDMA";
    gaddr.offset = remote_addr;
    auto coro_id = ctx ? ctx->coro_id() : kNotACoro;
    if constexpr (debug())
    {
        debug_valid_rdma_buffer(iobuf);
    }
    auto *rw_context = get_rw_context();
    uint16_t rw_ctx_id = get_rw_context_id(rw_context);
    rw_context->success = &ret;
    rw_context->ready = false;
    rw_context->coro_id = coro_id;
    rw_context->target_node = gaddr.nodeID;
    rw_context->dir_id = dir_id;

    auto wrid = WRID(wr_prefix, rw_ctx_id);
    DVLOG(4) << "[patronus] CAS node_id: " << node_id << ", dir_id " << dir_id
             << ", rkey: " << rkey << ", remote_addr: " << remote_addr
             << ", compare: " << compare << ", swap: " << swap
             << ", wr_id: " << wrid << ", coro: " << (ctx ? *ctx : nullctx);

    dsm_->rkey_cas(rkey,
                   iobuf,
                   gaddr,
                   dir_id,
                   compare,
                   swap,
                   true /* is_signal */,
                   wrid.val,
                   ctx);
    if (unlikely(ctx == nullptr))
    {
        dsm_->poll_rdma_cq(1);
        rw_context->ready.store(true);
    }

    DCHECK(rw_context->ready)
        << "** Should have been ready when switching back to worker " << ctx
        << ", rw_ctx_id: " << rw_ctx_id << ", ready at "
        << (void *) &rw_context->ready;
    if constexpr (debug())
    {
        memset(rw_context, 0, sizeof(RWContext));
    }
    put_rw_context(rw_context);

    if (unlikely(!ret))
    {
        DVLOG(4) << "[patronus] CAS failed by protection. wr_id: " << wrid
                 << "coro: " << (ctx ? *ctx : nullctx);
        return RetCode::kRdmaProtectionErr;
    }
    // see if cas can success
    auto read_remote = *(uint64_t *) iobuf;
    if (read_remote == compare)
    {
        DVLOG(4) << "[patronus] CAS success. wr_id: " << wrid
                 << ", coro: " << (ctx ? *ctx : nullctx)
                 << ", read: " << compound_uint64_t(read_remote)
                 << ", compare: " << compound_uint64_t(compare)
                 << ", new: " << compound_uint64_t(swap);
        return RetCode::kOk;
    }
    DVLOG(4) << "[patronus] CAS failed by mismatch @compare. wr_id: " << wrid
             << ", coro: " << (ctx ? *ctx : nullctx)
             << ", actual: " << compound_uint64_t(read_remote)
             << ", compare: " << compound_uint64_t(compare)
             << ", swap: " << compound_uint64_t(swap);

    return RetCode::kRdmaExecutionErr;
}
/**
 * @brief the actual implementation to modify a lease (especially relinquish
 * it).
 *
 * Depending on whether flag & (uint8_t) LeaseModifyFlag::kOnlyAllocation is ON,
 * the actual implemention will be very different.
 *
 * If flag is ON:
 * the server will skip lease_ctx and read the @hint, and @address and @size
 * from lease.
 *
 * If flag is OFF:
 * the server will find lease_ctx by @lease_id from lease, and use the info
 * there.
 */
Lease Patronus::lease_modify_impl(Lease &lease,
                                  uint64_t hint,
                                  RequestType type,
                                  time::ns_t ns,
                                  uint8_t flag,
                                  CoroContext *ctx)
{
    CHECK(type == RequestType::kExtend || type == RequestType::kRelinquish ||
          type == RequestType::kUpgrade)
        << "** invalid type " << (int) type;

    auto target_node_id = lease.node_id_;
    auto dir_id = lease.dir_id();
    auto tid = get_thread_id();
    auto from_mid = tid;
    auto to_mid = dir_id;
    CHECK_EQ(from_mid, to_mid)
        << "** rmsg does not support cross mid communication";

    char *rdma_buf = get_rdma_message_buffer();
    auto *rpc_context = get_rpc_context();
    uint16_t rpc_ctx_id = get_rpc_context_id(rpc_context);

    Lease ret_lease;
    ret_lease.node_id_ = target_node_id;

    auto *msg = (LeaseModifyRequest *) rdma_buf;
    msg->type = type;
    msg->cid.node_id = get_node_id();
    msg->cid.thread_id = tid;
    msg->cid.mid = from_mid;
    msg->cid.coro_id = ctx ? ctx->coro_id() : kNotACoro;
    msg->cid.rpc_ctx_id = rpc_ctx_id;
    msg->lease_id = lease.id();
    msg->ns = ns;
    msg->flag = flag;
    msg->hint = hint;
    // msg->addr and msg->size are only used when flag & only_alloc is true
    // In this case, the provided lease should provide the expected values.
    // Otherwise, msg->addr & msg->size is not used.
    msg->addr = lease.base_addr_;
    msg->size = lease.buffer_size_;

    rpc_context->ret_lease = &ret_lease;
    rpc_context->ready = false;
    rpc_context->request = (BaseMessage *) msg;

    if constexpr (debug())
    {
        msg->digest = 0;
        msg->digest = djb2_digest(msg, sizeof(LeaseModifyRequest));
    }

    dsm_->reliable_send(
        rdma_buf, sizeof(LeaseModifyRequest), target_node_id, to_mid);

    if (unlikely(ctx == nullptr))
    {
        while (!rpc_context->ready.load(std::memory_order_acquire))
        {
            std::this_thread::yield();
        }
    }
    else
    {
        // relinuishing a lease does not need a reply.
        if (type != RequestType::kRelinquish)
        {
            ctx->yield_to_master();
        }
    }

    // relinquishing does not need a reply
    // so rpc_context is not used.
    if (type != RequestType::kRelinquish)
    {
        DCHECK(rpc_context->ready) << "** Should have been ready when switch "
                                      "back to worker coro. ctx: "
                                   << (ctx ? *ctx : nullctx);
        if (ret_lease.success())
        {
            // if success, should disable the original lease
            lease.set_invalid();
        }
    }
    else
    {
        // actually no return. so never use me
        ret_lease.set_error(AcquireRequestStatus::kReserved);
    }

    if constexpr (debug())
    {
        memset(rpc_context, 0, sizeof(RpcContext));
    }

    put_rpc_context(rpc_context);
    // TODO(patronus): this may have bug if the buffer is re-used when NIC is
    // DMA-ing
    put_rdma_message_buffer(rdma_buf);

    return ret_lease;
}

size_t Patronus::handle_response_messages(const char *msg_buf,
                                          size_t msg_nr,
                                          coro_t *coro_buf)
{
    size_t cur_idx = 0;
    for (size_t i = 0; i < msg_nr; ++i)
    {
        auto *base = (BaseMessage *) (msg_buf + i * kMessageSize);
        auto request_type = base->type;
        auto coro_id = base->cid.coro_id;

        DCHECK_NE(coro_id, kNotACoro);
        if (likely(coro_id != kMasterCoro))
        {
            coro_buf[cur_idx++] = coro_id;
        }

        switch (request_type)
        {
        case RequestType::kAcquireNoLease:
        case RequestType::kAcquireRLease:
        case RequestType::kAcquireWLease:
        {
            auto *msg = (AcquireResponse *) base;
            DVLOG(4) << "[patronus] handling acquire response. " << *msg
                     << " at patronus_now: " << time_syncer_->patronus_now();
            handle_response_acquire(msg);
            break;
        }
        case RequestType::kUpgrade:
        case RequestType::kExtend:
        case RequestType::kRelinquish:
        {
            auto *msg = (LeaseModifyResponse *) base;
            DVLOG(4) << "[patronus] handling lease modify response " << *msg
                     << " at patronus_now: " << time_syncer_->patronus_now();
            handle_response_lease_modify(msg);
            break;
        }
        case RequestType::kAdmin:
        {
            auto *msg = (AdminRequest *) base;
            DVLOG(4) << "[patronus] handling admin request " << *msg
                     << " at patronus_now: " << time_syncer_->patronus_now();
            auto admin_type = (AdminFlag) msg->flag;
            if (admin_type == AdminFlag::kAdminReqExit)
            {
                handle_admin_exit(msg, nullptr);
            }
            else if (admin_type == AdminFlag::kAdminReqRecovery)
            {
                LOG(FATAL) << "Client should not be receiving kAdminReqReocery "
                              "request.";
            }
            else
            {
                LOG(FATAL) << "Unknown admin type " << (int) admin_type;
            }
            break;
        }
        default:
        {
            LOG(FATAL) << "Unknown response type " << (int) request_type
                       << ". Possible corrupted message";
        }
        }
    }
    return cur_idx;
}

void Patronus::handle_request_messages(const char *msg_buf,
                                       size_t msg_nr,
                                       CoroContext *ctx)
{
    for (size_t i = 0; i < msg_nr; ++i)
    {
        auto *base = (BaseMessage *) (msg_buf + i * kMessageSize);
        auto request_type = base->type;
        switch (request_type)
        {
        case RequestType::kAcquireNoLease:
        case RequestType::kAcquireRLease:
        case RequestType::kAcquireWLease:
        {
            auto *msg = (AcquireRequest *) base;
            DVLOG(4) << "[patronus] handling acquire request " << *msg
                     << " coro " << *ctx;
            handle_request_acquire(msg, ctx);
            break;
        }
        case RequestType::kUpgrade:
        case RequestType::kExtend:
        case RequestType::kRelinquish:
        {
            auto *msg = (LeaseModifyRequest *) base;
            DVLOG(4) << "[patronus] handling lease modify request " << *msg
                     << ", coro: " << *ctx;
            handle_request_lease_modify(msg, ctx);
            break;
        }
        case RequestType::kAdmin:
        {
            auto *msg = (AdminRequest *) base;
            DVLOG(4) << "[patronus] handling admin request " << *msg << " "
                     << *ctx;
            auto admin_type = (AdminFlag) msg->flag;
            if (admin_type == AdminFlag::kAdminReqExit)
            {
                handle_admin_exit(msg, nullptr);
            }
            else if (admin_type == AdminFlag::kAdminReqRecovery)
            {
                handle_admin_recover(msg, nullptr);
            }
            else if (admin_type == AdminFlag::kAdminBarrier)
            {
                handle_admin_barrier(msg, nullptr);
            }
            else
            {
                LOG(FATAL) << "unknown admin type " << (int) admin_type;
            }
            break;
        }
        default:
        {
            LOG(FATAL) << "Unknown request type " << (int) request_type
                       << ". Possible corrupted message";
        }
        }
    }
}

void Patronus::handle_response_acquire(AcquireResponse *resp)
{
    auto rpc_ctx_id = resp->cid.rpc_ctx_id;
    auto *rpc_context = rpc_context_.id_to_obj(rpc_ctx_id);

    auto *request = (AcquireRequest *) rpc_context->request;

    DCHECK(resp->type == RequestType::kAcquireRLease ||
           resp->type == RequestType::kAcquireWLease ||
           resp->type == RequestType::kAcquireNoLease)
        << "** unexpected request type received. got: " << (int) resp->type;

    auto &ret_lease = *rpc_context->ret_lease;
    if (likely(resp->status == AcquireRequestStatus::kSuccess))
    {
        ret_lease.base_addr_ = resp->buffer_base;
        ret_lease.header_addr_ = resp->header_base;
        ret_lease.node_id_ = resp->cid.node_id;
        ret_lease.buffer_size_ = request->size;
        ret_lease.header_size_ = sizeof(ProtectionRegion);
        ret_lease.header_rkey_ = resp->rkey_header;
        ret_lease.cur_rkey_ = resp->rkey_0;
        ret_lease.begin_term_ = resp->begin_term;
        ret_lease.ns_per_unit_ = resp->ns_per_unit;
        // TODO(patronus): without negotiate, @cur_unit_nr set to 1
        ret_lease.aba_unit_nr_to_ddl_ = compound_uint64_t(resp->aba_id, 1);
        ret_lease.update_ddl_term();
        ret_lease.id_ = resp->lease_id;
        ret_lease.dir_id_ = rpc_context->dir_id;
        if (resp->type == RequestType::kAcquireRLease)
        {
            ret_lease.lease_type_ = LeaseType::kReadLease;
        }
        else if (resp->type == RequestType::kAcquireWLease)
        {
            ret_lease.lease_type_ = LeaseType::kWriteLease;
        }
        else
        {
            ret_lease.lease_type_ = LeaseType::kUnknown;
        }
        ret_lease.set_finish();
    }
    else
    {
        ret_lease.set_error(resp->status);
    }

    rpc_context->ready.store(true, std::memory_order_release);
}

void Patronus::handle_request_acquire(AcquireRequest *req, CoroContext *ctx)
{
    debug_validate_acquire_request_flag(req->flag);

    DCHECK_NE(req->type, RequestType::kAcquireNoLease) << "** Deprecated";

    auto dirID = req->dir_id;

    // auto internal = dsm_->get_server_buffer();
    if constexpr (debug())
    {
        uint64_t digest = req->digest.get();
        req->digest = 0;
        DCHECK_EQ(digest, djb2_digest(req, sizeof(AcquireRequest)))
            << "** digest mismatch for req " << *req;
    }

    AcquireRequestStatus status = AcquireRequestStatus::kSuccess;

    uint64_t object_buffer_offset = 0;
    uint64_t object_dsm_offset = 0;
    void *object_addr = nullptr;
    void *header_addr = nullptr;
    uint64_t header_dsm_offset = 0;
    bool ctx_success = false;
    RWContext *rw_ctx = nullptr;
    uint64_t rw_ctx_id = 0;
    bool with_conflict_detect = false;
    uint64_t bucket_id = 0;
    uint64_t slot_id = 0;
    ProtectionRegion *protection_region = nullptr;
    uint64_t protection_region_id = 0;
    ibv_mw *buffer_mw = nullptr;
    ibv_mw *header_mw = nullptr;
    ibv_mr *buffer_mr = nullptr;
    ibv_mr *header_mr = nullptr;

    // will not actually bind memory window.
    // TODO(patronus): even with no_bind_pr, the PR is also allocated
    // We can optimize this.
    bool debug_no_bind_pr =
        req->flag & (uint8_t) AcquireRequestFlag::kDebugNoBindPR;
    bool debug_no_bind_any =
        req->flag & (uint8_t) AcquireRequestFlag::kDebugNoBindAny;
    bool with_alloc = req->flag & (uint8_t) AcquireRequestFlag::kWithAllocation;
    bool only_alloc = req->flag & (uint8_t) AcquireRequestFlag::kOnlyAllocation;

    bool use_mr = req->flag & (uint8_t) AcquireRequestFlag::kUseMR;

    bool with_lock =
        req->flag & (uint8_t) AcquireRequestFlag::kWithConflictDetect;

    bool with_buf = true;
    bool with_pr = true;
    bool with_lease_ctx = true;
    if (debug_no_bind_any)
    {
        with_buf = false;
        with_pr = false;
    }
    if (debug_no_bind_pr)
    {
        with_pr = false;
    }

    if (with_alloc)
    {
        with_pr = false;
    }
    if (only_alloc)
    {
        with_pr = false;
        with_buf = false;
        with_lease_ctx = false;
    }

    if (with_lock)
    {
        auto [b, s] = locate_key(req->key);
        with_conflict_detect = true;
        bucket_id = b;
        slot_id = s;
        if (!lock_manager_.try_lock(bucket_id, slot_id))
        {
            status = AcquireRequestStatus::kLockedErr;
            // err handling
            goto handle_response;
        }
    }

    if (with_alloc || only_alloc)
    {
        // allocation
        object_addr = patronus_alloc(req->size, req->key /* hint */);
        if (unlikely(object_addr == nullptr))
        {
            status = AcquireRequestStatus::kNoMem;
            goto handle_response;
        }
        object_dsm_offset = dsm_->addr_to_dsm_offset(object_addr);
        DCHECK_EQ(object_addr, dsm_->dsm_offset_to_addr(object_dsm_offset));
        object_buffer_offset = 0;  // not used
    }
    else
    {
        // acquire permission from existing memory
        object_buffer_offset = req->key;

        if (unlikely(!valid_total_buffer_offset(object_buffer_offset)))
        {
            status = AcquireRequestStatus::kAddressOutOfRangeErr;
            goto handle_response;
        }
        object_dsm_offset =
            dsm_->buffer_offset_to_dsm_offset(object_buffer_offset);
        object_addr = dsm_->buffer_offset_to_addr(object_buffer_offset);
        DCHECK_EQ(object_addr, dsm_->dsm_offset_to_addr(object_dsm_offset));
    }

    if (likely(with_pr))
    {
        protection_region = get_protection_region();
        protection_region_id =
            protection_region_pool_->buf_to_id(protection_region);
        header_addr = protection_region;
        header_dsm_offset = dsm_->addr_to_dsm_offset(header_addr);
    }

    ctx_success = false;

    rw_ctx = get_rw_context();
    rw_ctx_id = rw_context_.obj_to_id(rw_ctx);
    rw_ctx->coro_id = ctx ? ctx->coro_id() : kNotACoro;
    rw_ctx->dir_id = dirID;
    rw_ctx->ready = false;
    rw_ctx->success = &ctx_success;

    static thread_local ibv_send_wr wrs[8];
    switch (req->type)
    {
    case RequestType::kAcquireRLease:
    case RequestType::kAcquireWLease:
    {
        if (use_mr)
        {
            // use MR
            // no pollCQ, so directly set ready to true
            rw_ctx->ready = true;

            auto *rdma_ctx = DCHECK_NOTNULL(dsm_->get_rdma_context(dirID));
            if (with_buf)
            {
                buffer_mr = createMemoryRegion(
                    (uint64_t) object_addr, req->size, rdma_ctx);
                if (unlikely(buffer_mr == nullptr))
                {
                    (*rw_ctx->success) = false;
                    status = AcquireRequestStatus::kRegMrErr;
                    goto handle_response;
                }
            }
            if (with_pr)
            {
                header_mr = createMemoryRegion(
                    (uint64_t) header_addr, sizeof(ProtectionRegion), rdma_ctx);
                if (unlikely(header_mr == nullptr))
                {
                    (*rw_ctx->success) = false;
                    status = AcquireRequestStatus::kRegMrErr;
                    goto handle_response;
                }
            }
            (*rw_ctx->success) = true;
        }
        else
        {
            // use MW
            constexpr static uint32_t magic = 0b1010101010;
            constexpr static uint16_t mask = 0b1111111111;

            auto *qp =
                dsm_->get_dir_qp(req->cid.node_id, req->cid.thread_id, dirID);
            auto *mr = dsm_->get_dir_mr(dirID);
            int access_flag = req->type == RequestType::kAcquireRLease
                                  ? IBV_ACCESS_CUSTOM_REMOTE_RO
                                  : IBV_ACCESS_CUSTOM_REMOTE_RW;

            size_t bind_req_nr = 0;
            if (with_buf && !with_pr)
            {
                // only binding buffer. not to bind ProtectionRegion
                bind_req_nr = 1;
                buffer_mw = get_mw(dirID);
                fill_bind_mw_wr(wrs[0],
                                nullptr,
                                buffer_mw,
                                mr,
                                (uint64_t) DCHECK_NOTNULL(object_addr),
                                req->size,
                                access_flag);
                DVLOG(4) << "[patronus] Bind mw for buffer. addr "
                         << (void *) object_addr
                         << "(dsm_offset: " << object_dsm_offset
                         << "), size: " << req->size << " with access flag "
                         << (int) access_flag << ". Acquire flag: "
                         << AcquireRequestFlagOut(req->flag);
                wrs[0].send_flags = IBV_SEND_SIGNALED;
                wrs[0].wr_id =
                    WRID(WRID_PREFIX_PATRONUS_BIND_MW, rw_ctx_id).val;
            }
            else if (with_buf && with_pr)
            {
                // bind buffer & ProtectionRegion
                // for buffer
                bind_req_nr = 2;
                buffer_mw = get_mw(dirID);
                header_mw = get_mw(dirID);
                fill_bind_mw_wr(wrs[0],
                                &wrs[1],
                                buffer_mw,
                                mr,
                                (uint64_t) DCHECK_NOTNULL(object_addr),
                                req->size,
                                access_flag);
                DVLOG(4) << "[patronus] Bind mw for buffer. addr "
                         << (void *) object_addr
                         << "(dsm_offset: " << object_dsm_offset
                         << "), size: " << req->size << " with access flag "
                         << (int) access_flag << ". Acquire flag: "
                         << AcquireRequestFlagOut(req->flag);
                // for header
                fill_bind_mw_wr(wrs[1],
                                nullptr,
                                header_mw,
                                mr,
                                (uint64_t) header_addr,
                                sizeof(ProtectionRegion),
                                // header always grant R/W
                                IBV_ACCESS_CUSTOM_REMOTE_RW);
                DVLOG(4) << "[patronus] Bind mw for header. addr: "
                         << (void *) header_addr
                         << " (dsm_offset: " << header_dsm_offset << ")"
                         << ", size: " << sizeof(ProtectionRegion)
                         << " with R/W access. Acquire flag: "
                         << AcquireRequestFlagOut(req->flag);
                wrs[0].send_flags = 0;
                wrs[1].send_flags = IBV_SEND_SIGNALED;
                wrs[0].wr_id = WRID(WRID_PREFIX_RESERVED, 0).val;
                wrs[1].wr_id =
                    WRID(WRID_PREFIX_PATRONUS_BIND_MW, rw_ctx_id).val;
            }
            else
            {
                CHECK(!with_pr && !with_buf)
                    << "invalid configuration. with_pr: " << with_pr
                    << ", with_buf: " << with_buf;
            }
            if constexpr (config::kEnableSkipMagicMw)
            {
                for (size_t time = 0; time < bind_req_nr; time++)
                {
                    size_t id = allocated_mw_nr_++;
                    if (unlikely((id & mask) == magic))
                    {
                        status = AcquireRequestStatus::kMagicMwErr;
                    }
                }
            }
            if (likely(with_pr || with_buf))
            {
                ibv_send_wr *bad_wr;
                int ret = ibv_post_send(qp, wrs, &bad_wr);
                if (unlikely(ret != 0))
                {
                    PLOG(ERROR) << "[patronus] failed to ibv_post_send for "
                                   "bind_mw. failed wr: "
                                << *bad_wr;
                    status = AcquireRequestStatus::kBindErr;
                    goto handle_response;
                }
                if (likely(ctx != nullptr))
                {
                    ctx->yield_to_master();
                }
            }
            else
            {
                // skip all the actual binding, and set result to true
                *rw_ctx->success = true;
            }
        }
        break;
    }
    case RequestType::kAcquireNoLease:
    {
        *rw_ctx->success = true;
        break;
    }
    default:
    {
        LOG(FATAL) << "Unknown or unsupport request type " << (int) req->type
                   << " for req " << *req;
    }
    }

    if (likely(ctx != nullptr))
    {
        DCHECK(rw_ctx->success)
            << "** Should set to finished when switch back to worker "
               "coroutine. coro: "
            << *ctx << " ready: @" << (void *) rw_ctx->success;
    }
    else
    {
        ctx_success = true;
    }

handle_response:
    auto *resp_buf = get_rdma_message_buffer();
    auto *resp_msg = (AcquireResponse *) resp_buf;
    resp_msg->type = req->type;
    resp_msg->cid = req->cid;
    resp_msg->cid.node_id = get_node_id();
    resp_msg->cid.thread_id = get_thread_id();
    resp_msg->status = status;

    using LeaseIdT = decltype(AcquireResponse::lease_id);

    if (likely(status == AcquireRequestStatus::kSuccess))
    {
        // set to a scaring value so that we know it is invalid.
        LeaseIdT lease_id = std::numeric_limits<LeaseIdT>::max();
        if (likely(with_lease_ctx))
        {
            auto *lease_ctx = get_lease_context();
            lease_ctx->client_cid = req->cid;
            lease_ctx->buffer_mw = buffer_mw;
            lease_ctx->header_mw = header_mw;
            lease_ctx->buffer_mr = buffer_mr;
            lease_ctx->header_mr = header_mr;
            lease_ctx->dir_id = dirID;
            lease_ctx->addr_to_bind = (uint64_t) object_addr;
            lease_ctx->buffer_size = req->size;
            lease_ctx->with_pr = with_pr;
            lease_ctx->with_buf = with_buf;
            lease_ctx->hint = req->key;  // key is hint when allocation is on

            if (likely(with_pr))
            {
                lease_ctx->protection_region_id = protection_region_id;
                // set to valid until linked to lease_ctx
                protection_region->valid = true;
            }
            else
            {
                lease_ctx->protection_region_id = 0;
            }
            lease_ctx->with_conflict_detect = with_conflict_detect;
            lease_ctx->key_bucket_id = bucket_id;
            lease_ctx->key_slot_id = slot_id;
            lease_ctx->valid = true;

            lease_id = lease_context_.obj_to_id(lease_ctx);
            DCHECK_LT(lease_id, std::numeric_limits<LeaseIdT>::max());
        }

        auto ns_per_unit = req->required_ns;
        if (with_buf)
        {
            uint32_t buffer_rkey = use_mr ? buffer_mr->rkey : buffer_mw->rkey;
            resp_msg->rkey_0 = buffer_rkey;
        }
        else
        {
            resp_msg->rkey_0 = 0;
        }
        if (with_pr)
        {
            uint32_t header_rkey = use_mr ? header_mr->rkey : header_mw->rkey;
            resp_msg->rkey_header = header_rkey;
        }
        else
        {
            resp_msg->rkey_header = 0;
        }
        resp_msg->buffer_base = object_dsm_offset;
        resp_msg->header_base = header_dsm_offset;
        resp_msg->lease_id = lease_id;

        bool reserved = req->flag & (uint8_t) AcquireRequestFlag::kReserved;
        DCHECK(!reserved)
            << "reserved flag should not be set. Possible corrupted message";

        compound_uint64_t aba_unit_to_ddl(0);
        if (likely(with_pr))
        {
            aba_unit_to_ddl = protection_region->aba_unit_nr_to_ddl.load(
                std::memory_order_acq_rel);
            aba_unit_to_ddl.u32_2 = 1;
            protection_region->aba_unit_nr_to_ddl.store(
                aba_unit_to_ddl, std::memory_order_acq_rel);
        }

        // success, so register Lease expiring logic here.
        bool no_gc = req->flag & (uint8_t) AcquireRequestFlag::kNoGc;

        // can not enable gc while disable pr.
        bool invalid = !no_gc && !with_pr;
        DCHECK(!invalid) << "Invalid flag: if enable auto-gc (no_gc: " << no_gc
                         << "), could not disable pr (with_pr: " << with_pr
                         << "). debug_no_bind_pr: " << debug_no_bind_pr
                         << ", debug_no_bind_any: " << debug_no_bind_any;

        if (likely(!no_gc && with_pr))
        {
            auto patronus_now = time_syncer_->patronus_now();
            auto patronus_ddl_term = patronus_now.term() + ns_per_unit;

            ddl_manager_.push(
                patronus_ddl_term,
                [this, lease_id, ctx, cid = req->cid, aba_unit_to_ddl]() {
                    DCHECK_GT(aba_unit_to_ddl.u32_2, 0);
                    task_gc_lease(
                        lease_id, cid, aba_unit_to_ddl, 0 /* flag */, ctx);
                });

            // DVLOG(4) << "[debug] get client require ns " << req->required_ns
            //          << ", for key: " << req->key
            //          << ", ns_per_unit: " << ns_per_unit
            //          << ", at patronus_now: " << patronus_now
            //          << ", patronus_ddl_term: " << patronus_ddl_term
            //          << ", epsilon: " << time_syncer_->epsilon();

            resp_msg->begin_term = patronus_now.term();
            resp_msg->ns_per_unit = ns_per_unit;
            resp_msg->aba_id = aba_unit_to_ddl.u32_1;

            protection_region->begin_term = patronus_now.term();
            protection_region->ns_per_unit = req->required_ns;
            DCHECK_GT(aba_unit_to_ddl.u32_2, 0);
            memset(&(protection_region->meta), 0, sizeof(ProtectionRegionMeta));
        }
        else
        {
            resp_msg->begin_term = 0;
            resp_msg->ns_per_unit =
                std::numeric_limits<decltype(resp_msg->ns_per_unit)>::max();
            resp_msg->aba_id = 0;
        }
    }
    else
    {
        // gc here for all allocated resources
        resp_msg->lease_id = std::numeric_limits<LeaseIdT>::max();
        put_mw(dirID, buffer_mw);
        buffer_mw = nullptr;
        put_mw(dirID, header_mw);
        header_mw = nullptr;
        if (unlikely(buffer_mr != nullptr))
        {
            CHECK(destroyMemoryRegion(buffer_mr));
            buffer_mr = nullptr;
        }
        if (unlikely(header_mr != nullptr))
        {
            CHECK(destroyMemoryRegion(header_mr));
            header_mr = nullptr;
        }
        put_protection_region(protection_region);

        protection_region = nullptr;
        if (with_alloc || only_alloc)
        {
            // freeing nullptr is always well-defined
            patronus_free(object_addr, req->size, req->key /* hint */);
        }
    }

    auto from_mid = req->cid.mid;
    DVLOG(4) << "[patronus] Handle request acquire finished. resp: "
             << *resp_msg
             << " at patronus_now: " << time_syncer_->patronus_now();
    dsm_->reliable_send(
        (char *) resp_msg, sizeof(AcquireResponse), req->cid.node_id, from_mid);

    put_rdma_message_buffer(resp_buf);
    if (rw_ctx)
    {
        put_rw_context(rw_ctx);
        rw_ctx = nullptr;
    }
}

void Patronus::handle_request_lease_modify(LeaseModifyRequest *req,
                                           [[maybe_unused]] CoroContext *ctx)
{
    debug_validate_lease_modify_flag(req->flag);
    auto type = req->type;
    switch (type)
    {
    case RequestType::kRelinquish:
    {
        handle_request_lease_relinquish(req, ctx);
        break;
    }
    case RequestType::kExtend:
    {
        handle_request_lease_extend(req, ctx);
        break;
    }
    case RequestType::kUpgrade:
    {
        handle_request_lease_upgrade(req, ctx);
        break;
    }
    default:
    {
        LOG(FATAL) << "unknown/invalid request type " << (int) type
                   << " for coro" << (ctx ? *ctx : nullctx);
    }
    }
}

void Patronus::handle_request_lease_relinquish(LeaseModifyRequest *req,
                                               CoroContext *ctx)
{
    DCHECK_EQ(req->type, RequestType::kRelinquish);

    bool only_dealloc =
        req->flag & (uint8_t) LeaseModifyFlag::kOnlyDeallocation;

    // handle dealloc here
    if (only_dealloc)
    {
        auto dsm_offset = req->addr;
        auto *addr = dsm_->dsm_offset_to_addr(dsm_offset);
        patronus_free(addr, req->size, req->hint);
        return;
    }

    auto lease_id = req->lease_id;
    task_gc_lease(lease_id,
                  req->cid,
                  compound_uint64_t(0),
                  req->flag | (uint8_t) LeaseModifyFlag::kForceUnbind,
                  ctx);
}

void Patronus::handle_request_lease_upgrade(LeaseModifyRequest *req,
                                            CoroContext *ctx)
{
    DCHECK_EQ(req->type, RequestType::kUpgrade);
    auto lease_id = req->lease_id;
    auto *lease_ctx = CHECK_NOTNULL(get_lease_context(lease_id));

    CHECK(lease_ctx->client_cid.is_same(req->cid))
        << "if cid not match, should return false. " << lease_ctx->client_cid
        << " v.s. " << req->cid;

    CHECK(false) << "TODO:";
    auto dir_id = lease_ctx->dir_id;
    // TODO(patronus): rethink about it.
    auto *mw = lease_ctx->buffer_mw;

    if constexpr (debug())
    {
        uint64_t digest = req->digest.get();
        req->digest = 0;
        DCHECK_EQ(digest, djb2_digest(req, sizeof(LeaseModifyRequest)));
    }

    bool success = false;

    auto *rw_ctx = get_rw_context();
    auto rw_ctx_id = rw_context_.obj_to_id(rw_ctx);
    rw_ctx->coro_id = ctx ? ctx->coro_id() : kNotACoro;
    rw_ctx->dir_id = dir_id;
    rw_ctx->ready = false;
    rw_ctx->success = &success;

    LOG(WARNING) << "** TODO: [patronus] upgrade lease. lease_id "
                 << (id_t) lease_id << ", coro: " << (ctx ? *ctx : nullctx);
    dsm_->bind_memory_region_sync(
        mw,
        req->cid.node_id,
        req->cid.thread_id,
        (const char *) lease_ctx->addr_to_bind,
        lease_ctx->buffer_size,
        dir_id,
        WRID(WRID_PREFIX_PATRONUS_BIND_MW, rw_ctx_id).val,
        ctx);

    if (likely(ctx != nullptr))
    {
        DCHECK(rw_ctx->success)
            << "** Should set to finished when switch back to worker "
               "coroutine. coro: "
            << *ctx << "ready @" << (void *) rw_ctx->success;
    }

    auto *resp_buf = get_rdma_message_buffer();
    auto *resp_msg = (LeaseModifyResponse *) resp_buf;
    resp_msg->type = req->type;
    resp_msg->cid = req->cid;
    resp_msg->cid.node_id = get_node_id();
    resp_msg->cid.thread_id = get_thread_id();
    resp_msg->lease_id = lease_id;

    if (likely(success))
    {
        resp_msg->lease_id = lease_id;
        resp_msg->success = true;
    }
    else
    {
        resp_msg->lease_id = 0;
        resp_msg->success = false;
    }

    if constexpr (debug())
    {
        resp_msg->digest = 0;
        resp_msg->digest = djb2_digest(resp_msg, sizeof(LeaseModifyResponse));
    }
    auto from_mid = req->cid.mid;
    dsm_->reliable_send((char *) resp_msg,
                        sizeof(LeaseModifyResponse),
                        req->cid.node_id,
                        from_mid);
    put_rdma_message_buffer(resp_buf);
    put_rw_context(rw_ctx);
}

void Patronus::handle_request_lease_extend(LeaseModifyRequest *req,
                                           CoroContext *ctx)
{
    DCHECK_EQ(req->type, RequestType::kExtend);
    auto lease_id = req->lease_id;
    auto *lease_ctx = CHECK_NOTNULL(get_lease_context(lease_id));
    CHECK(lease_ctx->client_cid.is_same(req->cid))
        << "if cid not match, should return false. " << lease_ctx->client_cid
        << " v.s. " << req->cid;
    CHECK(false) << "TODO:";
    auto dir_id = lease_ctx->dir_id;
    // TODO(patronus): rethink about it
    auto *mw = lease_ctx->buffer_mw;

    if constexpr (debug())
    {
        uint64_t digest = req->digest.get();
        req->digest = 0;
        DCHECK_EQ(digest, djb2_digest(req, sizeof(LeaseModifyRequest)));
    }

    bool success = false;

    auto *rw_ctx = get_rw_context();
    auto rw_ctx_id = rw_context_.obj_to_id(rw_ctx);
    rw_ctx->coro_id = ctx ? ctx->coro_id() : kNotACoro;
    rw_ctx->dir_id = dir_id;
    rw_ctx->ready = false;
    rw_ctx->success = &success;

    LOG(WARNING) << "** TODO: [patronus] extend lease. lease_id "
                 << (id_t) lease_id << ", coro: " << (ctx ? *ctx : nullctx);
    dsm_->bind_memory_region_sync(
        mw,
        req->cid.node_id,
        req->cid.thread_id,
        (const char *) lease_ctx->addr_to_bind,
        lease_ctx->buffer_size,
        dir_id,
        WRID(WRID_PREFIX_PATRONUS_BIND_MW, rw_ctx_id).val,
        ctx);

    if (likely(ctx != nullptr))
    {
        DCHECK(rw_ctx->success)
            << "** Should set to finished when switch back to worker "
               "coroutine. coro: "
            << *ctx << "ready @" << (void *) rw_ctx->success;
    }

    auto *resp_buf = get_rdma_message_buffer();
    auto *resp_msg = (LeaseModifyResponse *) resp_buf;
    resp_msg->type = req->type;
    resp_msg->cid = req->cid;
    resp_msg->cid.node_id = get_node_id();
    resp_msg->cid.thread_id = get_thread_id();
    resp_msg->lease_id = lease_id;

    if (likely(success))
    {
        resp_msg->lease_id = lease_id;
        resp_msg->success = true;
    }
    else
    {
        resp_msg->lease_id = 0;
        resp_msg->success = false;
    }

    if constexpr (debug())
    {
        resp_msg->digest = 0;
        resp_msg->digest = djb2_digest(resp_msg, sizeof(LeaseModifyResponse));
    }

    auto from_mid = req->cid.mid;

    dsm_->reliable_send((char *) resp_msg,
                        sizeof(LeaseModifyResponse),
                        req->cid.node_id,
                        from_mid);
    put_rdma_message_buffer(resp_buf);
    put_rw_context(rw_ctx);
}

size_t Patronus::handle_rdma_finishes(
    ibv_wc *wc_buffer,
    size_t rdma_nr,
    coro_t *coro_buf,
    std::set<std::pair<size_t, size_t>> &recov)
{
    size_t cur_idx = 0;
    size_t fail_nr = 0;
    for (size_t i = 0; i < rdma_nr; ++i)
    {
        auto &wc = wc_buffer[i];
        // log_wc_handler(&wc);
        auto wr_id = WRID(wc.wr_id);
        auto id = wr_id.id;
        DCHECK(wr_id.prefix == WRID_PREFIX_PATRONUS_RW ||
               wr_id.prefix == WRID_PREFIX_PATRONUS_PR_RW ||
               wr_id.prefix == WRID_PREFIX_PATRONUS_CAS ||
               wr_id.prefix == WRID_PREFIX_PATRONUS_PR_CAS ||
               wr_id.prefix == WRID_PREFIX_PATRONUS_BATCH_RWCAS)
            << "** unexpected prefix " << (int) wr_id.prefix;
        auto *rw_context = rw_context_.id_to_obj(id);
        auto node_id = rw_context->target_node;
        auto dir_id = rw_context->dir_id;
        auto coro_id = rw_context->coro_id;
        CHECK_NE(coro_id, kMasterCoro)
            << "** Coro master should not issue R/W.";

        if (likely(wc.status == IBV_WC_SUCCESS))
        {
            *(rw_context->success) = true;
            DVLOG(4) << "[patronus] handle rdma finishes SUCCESS for coro "
                     << (int) coro_id << ". set "
                     << (void *) rw_context->success << " to true.";
        }
        else
        {
            DLOG(WARNING) << "[patronus] rdma R/W/CAS failed. wr_id: " << wr_id
                          << " for node " << node_id << ", dir: " << dir_id
                          << ", coro_id: " << (int) coro_id
                          << ". detail: " << wc;
            *(rw_context->success) = false;
            recov.insert({node_id, dir_id});
            fail_nr++;
        }
        rw_context->ready.store(true, std::memory_order_release);

        coro_buf[cur_idx++] = coro_id;
    }
    return fail_nr;
}

void Patronus::finished(uint64_t key)
{
    finished_[key][dsm_->get_node_id()] = true;

    auto tid = get_thread_id();
    // does not require replies
    auto from_mid = tid;
    auto to_mid = admin_mid();

    char *rdma_buf = get_rdma_message_buffer();
    auto *msg = (AdminRequest *) rdma_buf;
    msg->type = RequestType::kAdmin;
    msg->flag = (uint8_t) AdminFlag::kAdminReqExit;
    msg->cid.node_id = get_node_id();
    msg->cid.thread_id = tid;
    msg->cid.mid = from_mid;
    msg->cid.coro_id = kMasterCoro;
    msg->data = key;

    if constexpr (debug())
    {
        msg->digest = 0;
        msg->digest = djb2_digest(msg, sizeof(AdminRequest));
    }

    for (size_t i = 0; i < dsm_->getClusterSize(); ++i)
    {
        if (i == dsm_->get_node_id())
        {
            continue;
        }
        dsm_->reliable_send(rdma_buf, sizeof(AdminRequest), i, to_mid);
    }

    // TODO(patronus): this may have problem
    // if the buffer is reused when NIC is DMA-ing
    put_rdma_message_buffer(rdma_buf);
}
void Patronus::handle_admin_barrier(AdminRequest *req,
                                    [[maybe_unused]] CoroContext *ctx)
{
    if constexpr (debug())
    {
        uint64_t digest = req->digest.get();
        req->digest = 0;
        DCHECK_EQ(digest, djb2_digest(req, sizeof(AdminRequest)));
    }

    auto from_node = req->cid.node_id;

    {
        std::lock_guard<std::mutex> lk(barrier_mu_);
        barrier_[req->data].insert(from_node);
    }
}

void Patronus::handle_admin_recover(AdminRequest *req,
                                    [[maybe_unused]] CoroContext *ctx)
{
    if constexpr (debug())
    {
        uint64_t digest = req->digest.get();
        req->digest = 0;
        DCHECK_EQ(digest, djb2_digest(req, sizeof(AdminRequest)));
    }

    ContTimer<config::kMonitorFailureRecovery> timer;
    timer.init("Recover Dir QP");
    auto from_node = req->cid.node_id;
    auto tid = req->cid.thread_id;
    auto dir_id = req->dir_id;
    CHECK(dsm_->recoverDirQP(from_node, tid, dir_id));
    timer.pin("finished");

    LOG_IF(INFO, config::kMonitorFailureRecovery)
        << "[patronus] timer: " << timer;
}

void Patronus::handle_admin_exit(AdminRequest *req,
                                 [[maybe_unused]] CoroContext *ctx)
{
    if constexpr (debug())
    {
        uint64_t digest = req->digest.get();
        req->digest = 0;
        DCHECK_EQ(digest, djb2_digest(req, sizeof(AdminRequest)));
    }

    auto from_node = req->cid.node_id;
    auto key = req->data;
    DCHECK_LT(key, finished_.size());
    DCHECK_LT(from_node, finished_[key].size());
    finished_[key][from_node].store(true);

    for (size_t i = 0; i < dsm_->getClusterSize(); ++i)
    {
        if (!finished_[key][i])
        {
            DVLOG(1) << "[patronus] receive exit request by " << from_node
                     << ". but node " << i << " not finished yet. key: " << key;
            return;
        }
    }

    DVLOG(1) << "[patronus] set should_exit to true";
    DCHECK_LT(key, should_exit_.size());
    should_exit_[key].store(true, std::memory_order_release);
}
void Patronus::signal_server_to_recover_qp(size_t node_id, size_t dir_id)
{
    auto tid = get_thread_id();
    auto from_mid = tid;
    auto to_mid = from_mid;
    DCHECK_LT(to_mid, RMSG_MULTIPLEXING);
    DCHECK_LT(from_mid, RMSG_MULTIPLEXING);
    DCHECK_LT(tid, kMaxAppThread);

    char *rdma_buf = get_rdma_message_buffer();

    auto *msg = (AdminRequest *) rdma_buf;
    msg->type = RequestType::kAdmin;
    msg->cid.node_id = get_node_id();
    msg->cid.thread_id = tid;
    msg->cid.mid = from_mid;
    msg->cid.coro_id = kNotACoro;
    msg->cid.rpc_ctx_id = 0;
    msg->dir_id = dir_id;
    msg->flag = (uint8_t) AdminFlag::kAdminReqRecovery;

    if constexpr (debug())
    {
        msg->digest = 0;
        msg->digest = djb2_digest(msg, sizeof(AdminRequest));
    }

    dsm_->reliable_send(rdma_buf, sizeof(AdminRequest), node_id, to_mid);

    // TODO(patronus): this may have problem if message not inlined and buffer
    // is re-used and NIC is DMA-ing
    put_rdma_message_buffer(rdma_buf);
}
void Patronus::registerServerThread()
{
    allocated_mw_nr_ = 0;
    // for server, all the buffers are given to rdma_message_buffer_pool_
    dsm_->registerThread();

    auto rdma_buffer = dsm_->get_rdma_buffer();
    auto *dsm_rdma_buffer = rdma_buffer.buffer;
    size_t message_pool_size = rdma_buffer.size;
    rdma_message_buffer_pool_ =
        std::make_unique<ThreadUnsafeBufferPool<kMessageSize>>(
            dsm_rdma_buffer, message_pool_size);

    // TODO(patronus): still not determine the thread model of server side
    // what should be the number of pre-allocated mw?
    size_t alloc_mw_nr = kMwPoolSizePerThread / NR_DIRECTORY;
    for (size_t dirID = 0; dirID < NR_DIRECTORY; ++dirID)
    {
        for (size_t i = 0; i < alloc_mw_nr; ++i)
        {
            auto *mw = CHECK_NOTNULL(dsm_->alloc_mw(dirID));
            mw_pool_[dirID].push(mw);
            {
                std::lock_guard<std::mutex> lk(allocated_mws_mu_);
                allocated_mws_.insert(mw);
            }
        }
    }

    auto tid = get_thread_id();
    CHECK_LT(tid, NR_DIRECTORY)
        << "** make no sense to have more threads than NR_DIRECTORY. One "
           "thread can manipulate multiple DIR, but should not share the same "
           "DIR accross different threads. Especially, we need to know the "
           "MAX_SERVER_THREAD_NR to share the protection_region (which, I "
           "assume, is NR_DIRECTORY)";

    constexpr static size_t kMaxServerThreadNr = NR_DIRECTORY;

    auto protection_region_buffer = get_protection_region_buffer();
    auto pr_total_size = protection_region_buffer.size;
    auto pr_size_per_thread = pr_total_size / kMaxServerThreadNr;
    auto pr_buffer_self = protection_region_buffer.buffer +
                          (pr_size_per_thread * get_thread_id());
    protection_region_pool_ =
        std::make_unique<ThreadUnsafeBufferPool<sizeof(ProtectionRegion)>>(
            pr_buffer_self, pr_size_per_thread);

    // allocator
    mem::SlabAllocatorConfig slab_alloc_conf;
    slab_alloc_conf.block_class = conf_.block_class;
    slab_alloc_conf.block_ratio = conf_.block_ratio;
    size_t allocator_buf_size_thread = allocator_buf_size_ / NR_DIRECTORY;
    void *allocator_buf_addr_thread =
        (char *) allocator_buf_addr_ + allocator_buf_size_thread * tid;
    default_allocator_ = std::make_shared<mem::SlabAllocator>(
        allocator_buf_addr_thread, allocator_buf_size_thread, slab_alloc_conf);

    CHECK(!is_server_) << "** already registered";
    CHECK(!is_client_) << "** already registered as client thread.";
    is_server_ = true;
}

void Patronus::registerClientThread()
{
    dsm_->registerThread();
    // - reserve 4MB for message pool. total 65536 messages, far then enough
    // - reserve other 12 MB for client's usage. If coro_nr == 8, could
    // get 1.5 MB each coro.

    // dsm_->get_rdma_buffer() is per-thread
    auto rdma_buffer = dsm_->get_rdma_buffer();
    auto *dsm_rdma_buffer = rdma_buffer.buffer;

    size_t message_pool_size = 4 * define::MB;
    CHECK_GT(rdma_buffer.size, message_pool_size);
    CHECK_GE(message_pool_size / kMessageSize, 65536)
        << "Consider to tune up message pool size? Less than 64436 "
           "possible messages";

    rdma_message_buffer_pool_ =
        std::make_unique<ThreadUnsafeBufferPool<kMessageSize>>(
            dsm_rdma_buffer, message_pool_size);

    // the remaining buffer is given to client rdma buffers
    // 8B and non-8B
    auto *client_rdma_buffer = dsm_rdma_buffer + message_pool_size;
    size_t rdma_buffer_size = rdma_buffer.size - message_pool_size;
    CHECK_GE(rdma_buffer_size, kMaxCoroNr * kClientRdmaBufferSize)
        << "rdma_buffer not enough for maximum coroutine";

    // remember that, so that we could validate when debugging
    client_rdma_buffer_ = client_rdma_buffer;
    client_rdma_buffer_size_ = rdma_buffer_size;

    auto rdma_buffer_size_8B = 8 * 1024;
    CHECK_GE(rdma_buffer_size, rdma_buffer_size_8B);
    auto rdma_buffer_size_non_8B = rdma_buffer_size - rdma_buffer_size_8B;
    auto *client_rdma_buffer_8B = client_rdma_buffer;
    auto *client_rdma_buffer_non_8B = client_rdma_buffer + rdma_buffer_size_8B;

    rdma_client_buffer_8B_ = std::make_unique<ThreadUnsafeBufferPool<8>>(
        client_rdma_buffer_8B, rdma_buffer_size_8B);
    rdma_client_buffer_ =
        std::make_unique<ThreadUnsafeBufferPool<kClientRdmaBufferSize>>(
            client_rdma_buffer_non_8B, rdma_buffer_size_non_8B);

    CHECK(!is_client_) << "** already registered";
    CHECK(!is_server_) << "** already registered as server thread.";
    is_client_ = true;
}

size_t Patronus::try_get_client_continue_coros(size_t mid,
                                               coro_t *coro_buf,
                                               size_t limit)
{
    static thread_local char buf[ReliableConnection::kMaxRecvBuffer];
    auto nr = dsm_->reliable_try_recv(
        mid, buf, std::min(limit, ReliableConnection::kRecvLimit));
    size_t msg_nr = nr;

    size_t cur_idx = 0;
    cur_idx = handle_response_messages(buf, nr, coro_buf);
    DCHECK_LT(cur_idx, limit);

    size_t remain_nr = limit - nr;
    // should be enough
    constexpr static size_t kBufferSize = kMaxCoroNr + 1;
    static thread_local ibv_wc wc_buffer[kBufferSize];

    auto rdma_nr =
        dsm_->try_poll_rdma_cq(wc_buffer, std::min(remain_nr, kBufferSize));
    DCHECK_LT(rdma_nr, kBufferSize)
        << "** performance issue: buffer size not enough";

    std::set<std::pair<size_t, size_t>> recovery;
    size_t fail_nr =
        handle_rdma_finishes(wc_buffer, rdma_nr, coro_buf + cur_idx, recovery);

    if constexpr (debug())
    {
        if (unlikely(fail_nr > 0))
        {
            DVLOG(1) << "[patronus] handle rdma finishes got failure nr: "
                     << fail_nr << ". expect " << rw_context_.ongoing_size();
        }
    }
    cur_idx += rdma_nr;
    DCHECK_LT(cur_idx, limit);

    if (unlikely(fail_nr))
    {
        ContTimer<config::kMonitorFailureRecovery> timer;
        timer.init("Failure recovery");

        if constexpr (debug())
        {
            DVLOG(1) << "[patronus] failed. expect nr: "
                     << rw_context_.ongoing_size();
        }
        while (fail_nr < rw_context_.ongoing_size())
        {
            auto another_nr = dsm_->try_poll_rdma_cq(wc_buffer, kBufferSize);
            auto another_rdma_nr = handle_rdma_finishes(
                wc_buffer, another_nr, coro_buf + cur_idx, recovery);
            DCHECK_EQ(another_rdma_nr, another_nr);
            cur_idx += another_nr;
            DCHECK_LT(cur_idx, limit)
                << "** Provided buffer not enough to handle error message.";
            fail_nr += another_nr;
        }
        timer.pin("Wait R/W");
        // need to wait for server realizes QP errors, and response error to
        // memory bind call otherwise, the server will recovery QP and drop the
        // failed window_bind calls then the client corotine will be waiting
        // forever.
        size_t got = msg_nr;
        size_t expect_rpc_nr = rpc_context_.ongoing_size();
        while (got < rpc_context_.ongoing_size())
        {
            nr = dsm_->reliable_try_recv(
                mid, buf, std::min(limit, ReliableConnection::kRecvLimit));
            got += nr;
            LOG_IF(INFO, nr > 0) << "got another " << nr << ", cur: " << got
                                 << ", expect " << expect_rpc_nr;
            nr = handle_response_messages(buf, nr, coro_buf + cur_idx);
            cur_idx += nr;
            CHECK_LT(cur_idx, limit);
        }
        timer.pin("Wait RPC");

        for (const auto &[node_id, dir_id] : recovery)
        {
            signal_server_to_recover_qp(node_id, dir_id);
        }
        timer.pin("signal server");
        for (const auto &[node_id, dir_id] : recovery)
        {
            CHECK(dsm_->recoverThreadQP(node_id, dir_id));
        }
        timer.pin("recover QP");
        LOG_IF(INFO, config::kMonitorFailureRecovery)
            << "[patronus] client recovery: " << timer;
    }
    else
    {
        DCHECK(recovery.empty());
    }

    return cur_idx;
}

void Patronus::handle_response_lease_extend(LeaseModifyResponse *resp)
{
    CHECK(false) << "** Deprecated";
    auto rpc_ctx_id = resp->cid.rpc_ctx_id;
    auto *rpc_context = rpc_context_.id_to_obj(rpc_ctx_id);
    auto *request = (LeaseModifyRequest *) rpc_context->request;

    if constexpr (debug())
    {
        uint64_t request_digest = request->digest.get();
        request->digest = 0;
        uint64_t resp_digest = resp->digest.get();
        resp->digest = 0;
        DCHECK_EQ(request_digest,
                  djb2_digest(request, sizeof(LeaseModifyRequest)));
        DCHECK_EQ(resp_digest, djb2_digest(resp, sizeof(LeaseModifyResponse)));
    }

    DCHECK(resp->type != RequestType::kAcquireRLease &&
           resp->type != RequestType::kAcquireWLease &&
           resp->type != RequestType::kAcquireNoLease)
        << "** unexpected request type received. got: " << (int) resp->type;

    LOG(WARNING) << "TODO: please double check me";

    auto &lease = *rpc_context->ret_lease;
    const auto &origin_lease = *rpc_context->origin_lease;

    lease.copy_from(origin_lease);
    lease.id_ = resp->lease_id;

    if (resp->success)
    {
        lease.set_finish();
    }
    else
    {
        // TODO(patronus): deprecated, so don't care about the err code
        lease.set_error(AcquireRequestStatus::kReserved);
    }

    rpc_context->ready.store(true, std::memory_order_release);
}

void Patronus::handle_response_lease_modify(LeaseModifyResponse *resp)
{
    auto type = resp->type;
    switch (type)
    {
    case RequestType::kRelinquish:
    {
        LOG(FATAL) << "** invalid response type kReqinuish";
        break;
    }
    case RequestType::kExtend:
    {
        CHECK(false) << "** Deprecated.";
        handle_response_lease_extend(resp);
        break;
    }
    case RequestType::kUpgrade:
    {
        handle_response_lease_extend(resp);
        break;
    }
    default:
    {
        LOG(FATAL) << "** Unknow/invalid type " << (int) type;
        break;
    }
    }
}

void Patronus::server_serve(size_t mid, uint64_t key)
{
    auto &server_workers = server_coro_ctx_.server_workers;
    auto &server_master = server_coro_ctx_.server_master;

    for (size_t i = 0; i < kServerCoroNr; ++i)
    {
        server_workers[i] = CoroCall([this, i, key](CoroYield &yield) {
            server_coro_worker(i, yield, key);
        });
    }
    server_master = CoroCall([this, mid, key](CoroYield &yield) {
        server_coro_master(yield, mid, key);
    });

    server_master();
}

void Patronus::server_coro_master(CoroYield &yield,
                                  size_t mid,
                                  uint64_t wait_key)
{
    auto tid = get_thread_id();
    auto dir_id = mid;

    auto &server_workers = server_coro_ctx_.server_workers;
    CoroContext mctx(tid, &yield, server_workers);
    auto &comm = server_coro_ctx_.comm;
    auto &task_pool = server_coro_ctx_.task_pool;
    auto &task_queue = comm.task_queue;

    CHECK(mctx.is_master());

    LOG(INFO) << "[patronus] server thread " << tid
              << " handling mid = dir_id = " << mid;

    for (size_t i = 0; i < kMaxCoroNr; ++i)
    {
        comm.finished[i] = true;
    }

    constexpr static size_t kServerBufferNr = kMaxCoroNr * MAX_MACHINE;
    std::vector<char> __buffer;
    __buffer.resize(ReliableConnection::kMaxRecvBuffer * kServerBufferNr);

    server_coro_ctx_.buffer_pool = std::make_unique<
        ThreadUnsafeBufferPool<ReliableConnection::kMaxRecvBuffer>>(
        __buffer.data(), ReliableConnection::kMaxRecvBuffer * kServerBufferNr);
    auto &buffer_pool = *server_coro_ctx_.buffer_pool;

    while (likely(!should_exit(wait_key) || !task_queue.empty()))
    {
        // handle received messages
        char *buffer = (char *) CHECK_NOTNULL(buffer_pool.get());
        size_t nr =
            reliable_try_recv(mid, buffer, ReliableConnection::kRecvLimit);
        if (likely(nr > 0))
        {
            DVLOG(4) << "[patronus] server recv messages " << nr;
            auto *task = task_pool.get();
            task->buf = DCHECK_NOTNULL(buffer);
            task->msg_nr = nr;
            task->fetched_nr = 0;
            task->finished_nr = 0;
            comm.task_queue.push(task);
        }
        else
        {
            buffer_pool.put(buffer);
        }

        if (!comm.task_queue.empty())
        {
            for (size_t i = 0; i < kMaxCoroNr; ++i)
            {
                if (!comm.task_queue.empty())
                {
                    // it finished its last reqeust.
                    if (comm.finished[i])
                    {
                        comm.finished[i] = false;
                        DVLOG(4) << "[patronus] yield to " << (int) i
                                 << " because has task";
                        mctx.yield_to_worker(i);
                    }
                }
            }
        }

        constexpr static size_t kBufferSize = kMaxCoroNr * 2;
        static thread_local ibv_wc wc_buffer[kBufferSize];

        // handle finished CQEs
        nr = dsm_->try_poll_dir_cq(wc_buffer, dir_id, kBufferSize);

        // TODO(patronus): server can recovery before client telling it to do.
        // bool need_recovery = false;
        for (size_t i = 0; i < nr; ++i)
        {
            auto &wc = wc_buffer[i];
            auto wrid = WRID(wc.wr_id);
            DCHECK(wrid.prefix == WRID_PREFIX_PATRONUS_BIND_MW ||
                   wrid.prefix == WRID_PREFIX_PATRONUS_UNBIND_MW)
                << "** unexpexted prefix from wrid: " << wrid;
            auto rw_ctx_id = wrid.id;
            auto *rw_ctx = rw_context_.id_to_obj(rw_ctx_id);
            auto coro_id = rw_ctx->coro_id;
            CHECK_NE(coro_id, kNotACoro);
            CHECK_NE(coro_id, kMasterCoro)
                << "not sure. I this assert fail, rethink about me.";
            if (unlikely(wc.status != IBV_WC_SUCCESS))
            {
                log_wc_handler(&wc);
                LOG(WARNING) << "[patronus] server got failed wc: " << wrid
                             << " for coro " << (int) coro_id;
                // need_recovery = true;
                *(rw_ctx->success) = false;
            }
            else
            {
                DVLOG(4) << "[patronus] server got dir CQE for coro "
                         << (int) coro_id << ". wr_id: " << wrid;
                *(rw_ctx->success) = true;
            }
            rw_ctx->ready.store(true, std::memory_order_release);

            DVLOG(4) << "[patronus] server yield to coro " << (int) coro_id
                     << " for Dir CQE.";
            mctx.yield_to_worker(coro_id);
        }

        // handle any DDL Tasks
        // TODO(patronus): should worker also test the ddl_manager
        ddl_manager_.do_task(time_syncer_->patronus_now().term());
    }
}

void Patronus::task_gc_lease(uint64_t lease_id,
                             ClientID cid,
                             compound_uint64_t expect_aba_unit_nr_to_ddl,
                             uint8_t flag,
                             CoroContext *ctx)
{
    debug_validate_lease_modify_flag(flag);

    DVLOG(4) << "[patronus][gc_lease] task_gc_lease for lease_id " << lease_id
             << ", expect cid " << cid << ", coro: " << (ctx ? *ctx : nullctx)
             << ", at patronus time: " << time_syncer_->patronus_now();

    bool wait_success = flag & (uint8_t) LeaseModifyFlag::kWaitUntilSuccess;
    bool with_dealloc = flag & (uint8_t) LeaseModifyFlag::kWithDeallocation;
    bool only_dealloc = flag & (uint8_t) LeaseModifyFlag::kOnlyDeallocation;
    DCHECK(!only_dealloc) << "This case should have been handled. "
                             "task_gc_lease never handle this";
    bool use_mr = flag & (uint8_t) LeaseModifyFlag::kUseMR;

    auto *lease_ctx = get_lease_context(lease_id);
    if (unlikely(lease_ctx == nullptr))
    {
        DVLOG(4) << "[patronus][gc_lease] skip relinquish. lease_id "
                 << lease_id << " no valid lease context.";
        return;
    }
    DCHECK(lease_ctx->valid);
    if (unlikely(!lease_ctx->client_cid.is_same(cid)))
    {
        DVLOG(4) << "[patronus][gc_lease] skip relinquish. cid mismatch: "
                    "expect: "
                 << lease_ctx->client_cid << ", got: " << cid
                 << ". lease_ctx at " << (void *) lease_ctx;
        return;
    }

    bool with_pr = lease_ctx->with_pr;
    bool with_buf = lease_ctx->with_buf;

    uint64_t protection_region_id = 0;
    ProtectionRegion *protection_region = nullptr;

    if (likely(with_pr))
    {
        // test if client issued extends
        protection_region_id = lease_ctx->protection_region_id;
        protection_region = get_protection_region(protection_region_id);
        DCHECK(protection_region->valid)
            << "** If lease_ctx is valid, the protection region must also be "
               "valid. protection_region_id: "
            << protection_region_id << ", PR: " << *protection_region
            << ", lease_id: " << lease_id << ", cid: " << cid
            << ", expect_aba_unit_nr_to_ddl: " << expect_aba_unit_nr_to_ddl
            << ", flag: " << LeaseModifyFlagOut(flag)
            << ", coro: " << (ctx ? *ctx : nullctx) << ", protection_region at "
            << (void *) protection_region << ", lease_ctx at "
            << (void *) lease_ctx << ", handled by server tid "
            << get_thread_id();
        auto aba_unit_nr_to_ddl = protection_region->aba_unit_nr_to_ddl.load(
            std::memory_order_relaxed);
        auto next_expect_aba_unit_nr_to_ddl = aba_unit_nr_to_ddl;
        DCHECK_GT(aba_unit_nr_to_ddl.u32_2, 0);
        auto aba = aba_unit_nr_to_ddl.u32_1;
        auto expect_aba = expect_aba_unit_nr_to_ddl.u32_1;
        auto unit_nr_to_ddl = aba_unit_nr_to_ddl.u32_2;
        auto expect_unit_nr_to_ddl = expect_aba_unit_nr_to_ddl.u32_2;

        DCHECK_GE(unit_nr_to_ddl, expect_unit_nr_to_ddl)
            << "** The period will not go back: client only extend them";
        // indicate that server will by no means GC the lease
        bool force_gc = flag & (uint8_t) LeaseModifyFlag::kForceUnbind;
        bool client_already_exteded = unit_nr_to_ddl != expect_unit_nr_to_ddl;
        DVLOG(4) << "[patronus][gc_lease] determine the behaviour: force_gc: "
                 << force_gc
                 << ", client_already_extended: " << client_already_exteded
                 << " (pr->aba_unit_nr_to_ddl: " << aba_unit_nr_to_ddl
                 << " v.s. expect_aba_unit_nr_to_ddl: "
                 << expect_aba_unit_nr_to_ddl;

        if constexpr (debug())
        {
            if (!force_gc)
            {
                DCHECK_EQ(aba, expect_aba)
                    << "** The aba_id should remain the same. actual: "
                    << aba_unit_nr_to_ddl
                    << ", expect: " << expect_aba_unit_nr_to_ddl;
            }
        }

        if (unlikely(client_already_exteded && !force_gc))
        {
            DCHECK_GT(unit_nr_to_ddl, 0);
            auto next_ns = unit_nr_to_ddl * protection_region->ns_per_unit;
            auto next_ddl = protection_region->begin_term + next_ns;
            if constexpr (debug())
            {
                auto patronus_ddl = time::PatronusTime(next_ddl);
                auto patronus_now = time_syncer_->patronus_now();
                LOG_IF(WARNING, patronus_ddl < patronus_now)
                    << "[patronus] tasks' next DDL < now. patronus_ddl: "
                    << patronus_ddl << ", patronus_now: " << patronus_now;
            }
            ddl_manager_.push(next_ddl,
                              [this,
                               lease_id,
                               cid,
                               flag,
                               next_expect_aba_unit_nr_to_ddl,
                               ctx]() {
                                  task_gc_lease(lease_id,
                                                cid,
                                                next_expect_aba_unit_nr_to_ddl,
                                                flag,
                                                ctx);
                              });
            DVLOG(3) << "[patronus][gc_lease] skip relinquish because client's "
                        "extend. lease_id: "
                     << lease_id << "ProtectionRegion: " << (*protection_region)
                     << ", next_ddl: " << next_ddl << ", next_ns: " << next_ns
                     << ", patronus_now: " << time_syncer_->patronus_now()
                     << ", next_expect_aba_unit_nr_to_ddl: "
                     << next_expect_aba_unit_nr_to_ddl;
            return;
        }
        else
        {
            DVLOG(3) << "[patronus][gc_lease] relinquish from "
                        "decision(client_already_extended: "
                     << client_already_exteded << ", force_gc: " << force_gc
                     << "). lease_id: " << lease_id
                     << "ProtectionRegion: " << (*protection_region)
                     << ", patronus_now: " << time_syncer_->patronus_now();
        }

        // add @aba by 1, so that client cas will fail
        auto next_aba_unit_nr_to_ddl =
            protection_region->aba_unit_nr_to_ddl.load(
                std::memory_order_relaxed);
        next_aba_unit_nr_to_ddl.u32_1++;
        protection_region->aba_unit_nr_to_ddl.store(next_aba_unit_nr_to_ddl);
    }

    RWContext *rw_ctx = nullptr;
    uint64_t rw_ctx_id = 0;
    bool ctx_success = false;
    if (unlikely(wait_success))
    {
        rw_ctx = DCHECK_NOTNULL(get_rw_context());
        rw_ctx_id = rw_context_.obj_to_id(rw_ctx);
        rw_ctx->coro_id = ctx ? ctx->coro_id() : kNotACoro;
        rw_ctx->dir_id = lease_ctx->dir_id;
        rw_ctx->ready = false;
        rw_ctx->success = &ctx_success;
    }

    bool no_unbind = flag & (uint8_t) LeaseModifyFlag::kNoRelinquishUnbind;
    if (only_dealloc)
    {
        no_unbind = true;
    }
    if (likely(!no_unbind))
    {
        if (use_mr)
        {
            auto *buffer_mr = DCHECK_NOTNULL(lease_ctx)->buffer_mr;
            auto *header_mr = DCHECK_NOTNULL(lease_ctx)->header_mr;
            if (buffer_mr != nullptr)
            {
                CHECK(destroyMemoryRegion(buffer_mr));
                CHECK(destroyMemoryRegion(header_mr));
            }
            if (rw_ctx)
            {
                (*rw_ctx->success) = true;
                rw_ctx->ready = true;
            }
        }
        else
        {
            // should issue unbind MW here.
            static thread_local ibv_send_wr wrs[8];
            auto dir_id = lease_ctx->dir_id;
            auto *qp = dsm_->get_dir_qp(lease_ctx->client_cid.node_id,
                                        lease_ctx->client_cid.thread_id,
                                        dir_id);
            auto *mr = dsm_->get_dir_mr(dir_id);
            void *bind_nulladdr = dsm_->dsm_offset_to_addr(0);

            // NOTE: if size == 0, no matter access set to RO, RW or NORW
            // and no matter allocated_mw_nr +2/-2/0, it generates corrupted
            // mws. But if set size to 1 and allocated_mw_nr + 2, it works well.
            size_t bind_nr = 0;
            if (with_buf && !with_pr)
            {
                // only buffer
                bind_nr = 1;
                fill_bind_mw_wr(wrs[0],
                                nullptr,
                                lease_ctx->buffer_mw,
                                mr,
                                (uint64_t) bind_nulladdr,
                                1,
                                IBV_ACCESS_CUSTOM_REMOTE_NORW);

                wrs[0].send_flags = 0;
                if (unlikely(wait_success))
                {
                    wrs[0].send_flags |= IBV_SEND_SIGNALED;
                }
                // but want to detect failure
                wrs[0].wr_id =
                    WRID(WRID_PREFIX_PATRONUS_UNBIND_MW, rw_ctx_id).val;
            }
            else if (with_buf && with_pr)
            {
                // pr and buffer
                bind_nr = 2;
                fill_bind_mw_wr(wrs[0],
                                &wrs[1],
                                lease_ctx->header_mw,
                                mr,
                                (uint64_t) bind_nulladdr,
                                1,
                                IBV_ACCESS_CUSTOM_REMOTE_NORW);
                fill_bind_mw_wr(wrs[1],
                                nullptr,
                                lease_ctx->buffer_mw,
                                mr,
                                (uint64_t) bind_nulladdr,
                                1,
                                IBV_ACCESS_CUSTOM_REMOTE_NORW);

                wrs[0].send_flags = 0;
                wrs[1].send_flags = 0;
                if (unlikely(wait_success))
                {
                    wrs[1].send_flags = IBV_SEND_SIGNALED;
                }
                // but want to detect failure
                wrs[0].wr_id = wrs[1].wr_id =
                    WRID(WRID_PREFIX_PATRONUS_UNBIND_MW, rw_ctx_id).val;
            }
            else
            {
                CHECK(!with_buf && !with_pr)
                    << "invalid configuration. with_buf: " << with_buf
                    << ", with_pr: " << with_pr;
            }
            if (with_buf || with_pr)
            {
                // these are unsignaled requests
                // if going to fast, will overwhelm the QP
                // so limit the rate.
                ibv_send_wr *bad_wr;
                int ret = ibv_post_send(qp, wrs, &bad_wr);
                PLOG_IF(ERROR, ret != 0)
                    << "[patronus][gc_lease] failed to "
                       "ibv_post_send to unbind memory window. "
                    << *bad_wr;
                DCHECK(ctx != nullptr);
                if (unlikely(wait_success))
                {
                    ctx->yield_to_master();
                }
            }
            if constexpr (config::kEnableSkipMagicMw)
            {
                allocated_mw_nr_ += bind_nr;
            }
        }
    }
    bool with_conflict_detect = lease_ctx->with_conflict_detect;
    if (with_conflict_detect)
    {
        auto bucket_id = lease_ctx->key_bucket_id;
        auto slot_id = lease_ctx->key_slot_id;
        lock_manager_.unlock(bucket_id, slot_id);
    }

    if (with_dealloc)
    {
        auto *addr = (void *) lease_ctx->addr_to_bind;
        auto size = lease_ctx->buffer_size;
        auto hint = lease_ctx->hint;
        patronus_free(addr, size, hint);
    }

    put_mw(lease_ctx->dir_id, lease_ctx->header_mw);
    put_mw(lease_ctx->dir_id, lease_ctx->buffer_mw);
    // TODO(patronus): not considering any checks here
    // for example, the pr->meta.relinquished bits.
    if (with_pr)
    {
        put_protection_region(protection_region);
    }

    if (unlikely(wait_success))
    {
        DCHECK(rw_ctx->ready) << "** go back to coro " << pre_coro_ctx(ctx)
                              << ", but rw_ctx not ready";
        DCHECK(ctx_success) << "Don't know why this op failed";
    }
    if (rw_ctx != nullptr)
    {
        put_rw_context(rw_ctx);
    }

    put_lease_context(lease_ctx);
}

void Patronus::server_coro_worker(coro_t coro_id,
                                  CoroYield &yield,
                                  uint64_t wait_key)
{
    auto tid = get_thread_id();
    CoroContext ctx(tid, &yield, &server_coro_ctx_.server_master, coro_id);
    auto &comm = server_coro_ctx_.comm;
    auto &task_queue = comm.task_queue;
    auto &task_pool = server_coro_ctx_.task_pool;
    auto &buffer_pool = *server_coro_ctx_.buffer_pool;

    while (likely(!should_exit(wait_key) || !task_queue.empty()))
    {
        DCHECK(!task_queue.empty());
        auto task = task_queue.front();
        DCHECK_NE(task->msg_nr, 0);
        DCHECK_LT(task->fetched_nr, task->msg_nr);
        auto cur_nr = task->fetched_nr;
        const char *cur_msg =
            task->buf + ReliableConnection::kMessageSize * cur_nr;
        task->fetched_nr++;
        if (task->fetched_nr == task->msg_nr)
        {
            comm.task_queue.pop();
        }
        DVLOG(4) << "[patronus] server handling task @" << (void *) task
                 << ", message_nr " << task->msg_nr << ", cur_fetched "
                 << cur_nr << ", cur_finished: " << task->finished_nr << " "
                 << ctx;
        handle_request_messages(cur_msg, 1, &ctx);
        task->finished_nr++;
        if (task->finished_nr == task->msg_nr)
        {
            DVLOG(4) << "[patronus] server handling callback of task @"
                     << (void *) task;
            buffer_pool.put((void *) task->buf);
            task_pool.put(task);
        }

        DVLOG(4) << "[patronus] server " << ctx
                 << " finished current task. yield to master.";
        comm.finished[coro_id] = true;
        ctx.yield_to_master();
    }

    DVLOG(3) << "[bench] server coro: " << ctx << " exit.";
    ctx.yield_to_master();
}

RetCode Patronus::maybe_auto_extend(Lease &lease, CoroContext *ctx)
{
    using namespace std::chrono_literals;

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
    // assume one-sided write will not take longer than this
    constexpr static auto kMinMarginDuration = 100us;
    constexpr static time::ns_t kMinMarginDurationNs =
        std::chrono::duration_cast<std::chrono::nanoseconds>(kMinMarginDuration)
            .count();
    // two @ns_per_unit should be enough
    time::ns_t ideal_margin_duration_ns = lease.ns_per_unit_ * 2;
    auto margin_duration_ns =
        std::max(ideal_margin_duration_ns, kMinMarginDurationNs);
    if (diff_ns <= margin_duration_ns)
    {
        return extend_impl(
            lease, lease.next_extend_unit_nr(), 0 /* flag */, ctx);
    }
    DVLOG(4) << "[patronus][maybe-extend] Not reach lease DDL (not extend). "
                "lease ddl:"
             << patronus_ddl << ", patronus_now: " << patronus_now
             << ", diff_ns: " << diff_ns << ", > margin_duration_ns("
             << margin_duration_ns << "). coro: " << (ctx ? *ctx : nullctx)
             << ". Now lease: " << lease;
    return RetCode::kOk;
}

void Patronus::thread_explain() const
{
    LOG(INFO) << "[patronus] tid: " << get_thread_id()
              << ", rdma_message_buffer_pool: "
              << rdma_message_buffer_pool_.get()
              << ", rdma_client_buffer: " << rdma_client_buffer_.get()
              << ", rdma_client_buffer_8B: " << rdma_client_buffer_8B_.get()
              << ", rpc_context: " << (void *) &rpc_context_
              << ", rw_context: " << (void *) &rw_context_
              << ", lease_context: " << (void *) &lease_context_
              << ", protection_region_pool: " << protection_region_pool_.get();
}

void Patronus::validate_buffers()
{
    std::vector<Buffer> buffers;
    buffers.push_back(get_time_sync_buffer());
    buffers.push_back(get_protection_region_buffer());
    buffers.push_back(get_alloc_buffer());
    buffers.push_back(get_lease_buffer());
    buffers.push_back(get_user_reserved_buffer());

    validate_buffer_not_overlapped(buffers);
}

void Patronus::reg_allocator(uint64_t hint, mem::IAllocator::pointer allocator)
{
    DCHECK(is_server_)
        << "** not registered thread, or registered client thread.";
    reg_allocators_[hint] = allocator;
}
mem::IAllocator::pointer Patronus::get_allocator(uint64_t hint)
{
    if (hint == kDefaultHint)
    {
        return default_allocator_;
    }
    auto it = reg_allocators_.find(hint);
    if (unlikely(it == reg_allocators_.end()))
    {
        return nullptr;
    }
    return it->second;
}

RetCode Patronus::prepare_write(PatronusBatchContext &batch,
                                Lease &lease,
                                const char *ibuf,
                                size_t size,
                                size_t offset,
                                uint8_t flag,
                                CoroContext *ctx)
{
    auto ec = handle_batch_op_flag(flag);
    if (unlikely(ec != RetCode::kOk))
    {
        return ec;
    }
    CHECK(lease.success());
    CHECK(lease.is_writable());

    uint32_t rkey = lease.cur_rkey_;
    uint64_t remote_addr = lease.base_addr_ + offset;
    auto node_id = lease.node_id_;
    auto dir_id = lease.dir_id_;

    GlobalAddress gaddr;
    gaddr.nodeID = node_id;
    gaddr.offset = remote_addr;
    uint64_t source = (uint64_t) ibuf;
    uint64_t dest = dsm_->gaddr_to_addr(gaddr);
    uint32_t lkey = dsm_->get_icon_lkey();
    auto *qp = DCHECK_NOTNULL(dsm_->get_th_qp(node_id, dir_id));
    return batch.prepare_write(
        qp, node_id, dir_id, dest, source, size, lkey, rkey, ctx);
}

RetCode Patronus::prepare_read(PatronusBatchContext &batch,
                               Lease &lease,
                               char *obuf,
                               size_t size,
                               size_t offset,
                               uint8_t flag,
                               CoroContext *ctx)
{
    auto ec = handle_batch_op_flag(flag);
    if (unlikely(ec != RetCode::kOk))
    {
        return ec;
    }
    CHECK(lease.success());
    CHECK(lease.is_readable());

    uint32_t rkey = lease.cur_rkey_;
    uint64_t remote_addr = lease.base_addr_ + offset;
    auto node_id = lease.node_id_;
    auto dir_id = lease.dir_id_;

    GlobalAddress gaddr;
    gaddr.nodeID = node_id;
    gaddr.offset = remote_addr;
    uint64_t source = (uint64_t) obuf;
    uint64_t dest = dsm_->gaddr_to_addr(gaddr);
    uint32_t lkey = dsm_->get_icon_lkey();
    auto *qp = DCHECK_NOTNULL(dsm_->get_th_qp(node_id, dir_id));
    return batch.prepare_read(
        qp, node_id, dir_id, source, dest, size, lkey, rkey, ctx);
}

RetCode Patronus::prepare_cas(PatronusBatchContext &batch,
                              Lease &lease,
                              char *iobuf,
                              size_t offset,
                              uint64_t compare,
                              uint64_t swap,
                              uint8_t flag,
                              CoroContext *ctx)
{
    auto ec = handle_batch_op_flag(flag);
    if (unlikely(ec != RetCode::kOk))
    {
        return ec;
    }
    CHECK(lease.success());
    CHECK(lease.is_writable());

    uint32_t rkey = lease.cur_rkey_;
    uint64_t remote_addr = lease.base_addr_ + offset;
    auto node_id = lease.node_id_;
    auto dir_id = lease.dir_id_;

    GlobalAddress gaddr;
    gaddr.nodeID = node_id;
    gaddr.offset = remote_addr;
    uint64_t source = (uint64_t) iobuf;
    uint64_t dest = dsm_->gaddr_to_addr(gaddr);
    uint32_t lkey = dsm_->get_icon_lkey();
    auto *qp = DCHECK_NOTNULL(dsm_->get_th_qp(node_id, dir_id));
    return batch.prepare_cas(
        qp, node_id, dir_id, dest, source, compare, swap, lkey, rkey, ctx);
}

RetCode Patronus::handle_batch_op_flag(uint8_t flag) const
{
    if constexpr (debug())
    {
        bool no_local_check = flag & (uint8_t) RWFlag::kNoLocalExpireCheck;
        CHECK(no_local_check) << "** Batch op not support local expire checks";
        bool with_auto_expend = flag & (uint8_t) RWFlag::kWithAutoExtend;
        CHECK(!with_auto_expend)
            << "** Batch op does not support auto lease extend";
        bool with_cache = flag & (uint8_t) RWFlag::kWithCache;
        CHECK(!with_cache) << "** Batch op does not support local caching";
        bool reserved = flag & (uint8_t) RWFlag::kReserved;
        CHECK(!reserved);
    }
    return kOk;
}

RetCode Patronus::commit(PatronusBatchContext &batch, CoroContext *ctx)
{
    RetCode rc = kOk;
    RWContext *rw_context = nullptr;
    uint16_t rw_ctx_id = 0;

    if (unlikely(batch.empty()))
    {
        return kOk;
    }
    bool ret_success = false;
    rw_context = DCHECK_NOTNULL(get_rw_context());
    rw_ctx_id = get_rw_context_id(rw_context);
    rw_context->success = &ret_success;
    rw_context->ready = false;
    rw_context->coro_id = ctx ? ctx->coro_id() : kNotACoro;
    // We have to fill the below two fields
    // so we retrieve from the batch
    rw_context->target_node = batch.node_id();
    rw_context->dir_id = batch.dir_id();

    WRID wr_id(WRID_PREFIX_PATRONUS_BATCH_RWCAS, rw_ctx_id);
    rc = batch.commit(wr_id.val, ctx);
    if (unlikely(rc != kOk))
    {
        goto ret;
    }

    DCHECK(rw_context->ready)
        << "** When commit finished, should have been ready.";

    if (likely(ret_success))
    {
        rc = kOk;
    }
    else
    {
        // TODO: I believe it is protection error
        rc = kRdmaProtectionErr;
    }

ret:
    put_rw_context(rw_context);
    return rc;
}

}  // namespace patronus