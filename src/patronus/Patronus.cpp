#include "patronus/Patronus.h"

#include "Util.h"
#include "patronus/IBOut.h"
#include "patronus/Time.h"
#include "umsg/Config.h"
#include "util/Debug.h"
#include "util/PerformanceReporter.h"
#include "util/Pre.h"
#include "util/Rand.h"

namespace patronus
{
// rdma_message_buffer_pool_
// client_rdma_buffer_
// client_rdma_buffer_size_
// rdma_client_buffer_
// rdma_client_buffer_8B_
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
thread_local mem::SlabAllocator::pointer Patronus::default_allocator_;
thread_local std::unordered_map<uint64_t, mem::IAllocator::pointer>
    Patronus::reg_allocators_;
thread_local char *Patronus::client_rdma_buffer_{nullptr};
thread_local size_t Patronus::client_rdma_buffer_size_{0};
thread_local bool Patronus::is_server_{false};
thread_local bool Patronus::is_client_{false};
thread_local bool Patronus::self_managing_client_rdma_buffer_{false};
thread_local bool Patronus::has_registered_{false};
thread_local std::vector<ServerCoroBatchExecutionContext>
    Patronus::coro_batch_ex_ctx_;

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

    // we have to registered dsm threads
    // because the following time syncer needs it.
    dsm_->registerThread();

    // validate dsm
    auto internal_buf = dsm_->get_server_buffer();
    auto reserve_buf = dsm_->get_server_reserved_buffer();
    CHECK_GE(internal_buf.size, conf.total_buffer_size())
        << "** dsm should allocate DSM buffer at least what Patronus requires";
    CHECK_GE(reserve_buf.size, required_dsm_reserve_size())
        << "**dsm should provide reserved buffer at least what Patronus "
           "requries";

    // DVLOG(1) << "[patronus] Barrier: ctor of DSM";
    dsm_->keeper_barrier("__patronus::internal_barrier", 10ms);

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
                               RpcType type,
                               flag_t flag,
                               CoroContext *ctx)
{
    DCHECK(type == RpcType::kAcquireNoLeaseReq ||
           type == RpcType::kAcquireWLeaseReq ||
           type == RpcType::kAcquireRLeaseReq);

    bool no_rpc = flag & (flag_t) AcquireRequestFlag::kNoRpc;
    bool no_gc = flag & (flag_t) AcquireRequestFlag::kNoGc;
    if (unlikely(no_rpc))
    {
        Lease ret;
        ret.node_id_ = node_id;
        ret.no_gc_ = no_gc;
        if (no_gc)
        {
            ret.ddl_term_ = time::PatronusTime::max();
        }

        auto object_buffer_offset = key_or_hint;  // must be key in this case
        auto object_dsm_offset =
            dsm_->buffer_offset_to_dsm_offset(object_buffer_offset);
        ret.base_addr_ = object_dsm_offset;

        ret.buffer_size_ = size;
        ret.header_addr_ = 0;
        ret.header_size_ = 0;
        ret.header_rkey_ = 0;
        ret.cur_rkey_ = 0;
        ret.begin_term_ = 0;
        ret.ns_per_unit_ = 0;
        ret.aba_unit_nr_to_ddl_ = compound_uint64_t(0, 1);
        ret.update_ddl_term();
        ret.id_ = 0;
        ret.dir_id_ = dir_id;
        ret.lease_type_ = type == RpcType::kAcquireWLeaseReq
                              ? LeaseType::kWriteLease
                              : LeaseType::kReadLease;

        ret.set_finish();
        return ret;
    }

    bool enable_trace = false;
    trace_t trace = 0;
    if constexpr (::config::kEnableTrace)
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

    DCHECK_LT(tid, kMaxAppThread);
    DCHECK_LT(dir_id, NR_DIRECTORY);
    DCHECK_LT(tid, kMaxAppThread);

    char *rdma_buf = get_rdma_message_buffer();
    auto *rpc_context = get_rpc_context();
    uint16_t rpc_ctx_id = get_rpc_context_id(rpc_context);

    Lease ret_lease;
    ret_lease.node_id_ = node_id;
    ret_lease.no_gc_ = no_gc;

    auto *msg = (AcquireRequest *) rdma_buf;
    msg->type = type;
    msg->cid.node_id = get_node_id();
    msg->cid.thread_id = tid;
    msg->cid.coro_id = ctx ? ctx->coro_id() : kNotACoro;
    msg->cid.rpc_ctx_id = rpc_ctx_id;
    msg->dir_id = dir_id;
    msg->key = key_or_hint;
    msg->size = size;
    msg->required_ns = ns;
    msg->trace = trace;
    msg->flag = flag;

    rpc_context->ret_lease = &ret_lease;
    rpc_context->dir_id = dir_id;
    rpc_context->ready = false;
    rpc_context->request = (BaseMessage *) msg;
    rpc_context->ret_code = RetCode::kOk;

    if constexpr (debug())
    {
        msg->digest = 0;
        msg->digest = djb2_digest(msg, sizeof(AcquireRequest));
    }

    if (unlikely(enable_trace))
    {
        ctx->timer().pin("[get] Before send");
    }
    DVLOG(4) << "[patronus] get_lease from node_id: " << node_id
             << ", dir_id: " << dir_id << ", for key_or_hint: " << key_or_hint
             << ", size: " << size << ", ns: " << ns << ", type: " << type
             << ", flag: " << AcquireRequestFlagOut(flag)
             << ", at patronus_now: " << time_syncer_->patronus_now()
             << ". coro: " << pre_coro_ctx(ctx);

    dsm_->unreliable_send(rdma_buf, sizeof(AcquireRequest), node_id, dir_id);

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
    CHECK(rpc_context->ready) << "** Should have been ready when switch back "
                                 "to worker coro. ctx: "
                              << (ctx ? *ctx : nullctx);
    if (!ret_lease.success())
    {
        CHECK_NE(ret_lease.status_, AcquireRequestStatus::kReserved)
            << "** getting ret_lease is not succeeded, but the status is "
               "reserved. Lease: "
            << ret_lease;
        DCHECK_NE(rpc_context->ret_code, RetCode::kOk);
    }

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
                                  CoroContext *ctx,
                                  TraceView v)
{
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
    rw_context->wc_status = IBV_WC_SUCCESS;
    rw_context->ready = false;
    rw_context->coro_id = coro_id;
    rw_context->target_node = gaddr.nodeID;
    rw_context->dir_id = dir_id;
    rw_context->trace_view = v;
    v.pin("issue");

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

    auto wc_status = rw_context->wc_status;

    DCHECK(wc_status == IBV_WC_SUCCESS || wc_status == IBV_WC_WR_FLUSH_ERR ||
           wc_status == IBV_WC_REM_ACCESS_ERR || wc_status == IBV_WC_REM_OP_ERR)
        << "** unexpected status " << ibv_wc_status_str(wc_status) << " ["
        << (int) wc_status << "]";

    if constexpr (debug())
    {
        memset(rw_context, 0, sizeof(RWContext));
    }

    put_rw_context(rw_context);

    if (wc_status == IBV_WC_SUCCESS)
    {
        return kOk;
    }
    else
    {
        return kRdmaProtectionErr;
    }
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
    DCHECK(lease.success());

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
                           CoroContext *ctx,
                           TraceView v)
{
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
    rw_context->wc_status = IBV_WC_SUCCESS;
    rw_context->ready = false;
    rw_context->coro_id = coro_id;
    rw_context->target_node = gaddr.nodeID;
    rw_context->dir_id = dir_id;
    rw_context->trace_view = v;
    v.pin("issue");

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
    auto wc_status = rw_context->wc_status;
    bool success = wc_status == IBV_WC_SUCCESS;
    if constexpr (debug())
    {
        memset(rw_context, 0, sizeof(RWContext));
    }
    put_rw_context(rw_context);

    if (unlikely(!success))
    {
        DVLOG(4) << "[patronus] CAS failed by protection. wr_id: " << wrid
                 << ", wc_status: " << ibv_wc_status_str(wc_status)
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
 * Depending on whether flag & (flag_t) LeaseModifyFlag::kOnlyAllocation is ON,
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
RetCode Patronus::lease_modify_impl(Lease &lease,
                                    uint64_t hint,
                                    RpcType type,
                                    time::ns_t ns,
                                    flag_t flag,
                                    CoroContext *ctx)
{
    DCHECK(type == RpcType::kRelinquishReq || type == RpcType::kExtendReq)
        << "** invalid type " << (int) type;

    bool no_rpc = flag & (flag_t) LeaseModifyFlag::kNoRpc;

    if (unlikely(no_rpc))
    {
        return RetCode::kOk;
    }

    auto target_node_id = lease.node_id_;
    auto dir_id = lease.dir_id();
    auto tid = get_thread_id();

    char *rdma_buf = get_rdma_message_buffer();
    auto *rpc_context = get_rpc_context();
    uint16_t rpc_ctx_id = get_rpc_context_id(rpc_context);

    auto *msg = (LeaseModifyRequest *) rdma_buf;
    msg->type = type;
    msg->cid.node_id = get_node_id();
    msg->cid.thread_id = tid;
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

    rpc_context->ready = false;
    rpc_context->request = (BaseMessage *) msg;
    rpc_context->ret_code = RetCode::kOk;

    if constexpr (debug())
    {
        msg->digest = 0;
        msg->digest = djb2_digest(msg, sizeof(LeaseModifyRequest));
    }

    dsm_->unreliable_send(
        rdma_buf, sizeof(LeaseModifyRequest), target_node_id, dir_id);

    if (unlikely(ctx == nullptr))
    {
        while (!rpc_context->ready.load(std::memory_order_acquire))
        {
            std::this_thread::yield();
        }
    }
    else
    {
        ctx->yield_to_master();
    }

    DCHECK(rpc_context->ready) << "** Should have been ready when switch "
                                  "back to worker coro. ctx: "
                               << (ctx ? *ctx : nullctx);

    auto ret = rpc_context->ret_code;

    put_rpc_context(rpc_context);
    // TODO(patronus): this may have bug if the buffer is re-used when NIC is
    // DMA-ing
    put_rdma_message_buffer(rdma_buf);

    return ret;
}

size_t Patronus::handle_response_messages(const char *msg_buf,
                                          size_t msg_nr,
                                          coro_t *coro_buf)
{
    size_t cur_idx = 0;
    for (size_t i = 0; i < msg_nr; ++i)
    {
        auto *base = (BaseMessage *) (msg_buf + i * kMessageSize);
        auto response_type = base->type;
        auto coro_id = base->cid.coro_id;

        DCHECK_NE(coro_id, kNotACoro);
        if (likely(coro_id != kMasterCoro))
        {
            coro_buf[cur_idx++] = coro_id;
        }

        switch (response_type)
        {
        case RpcType::kAcquireLeaseResp:
        {
            auto *msg = (AcquireResponse *) base;
            DVLOG(4) << "[patronus] handling acquire response. " << *msg
                     << " at patronus_now: " << time_syncer_->patronus_now();
            if (unlikely(msg->status == AcquireRequestStatus::kReserved))
            {
                CHECK(false) << "** Unexpected status: " << msg->status
                             << ". Msg: " << *msg;
            }
            DCHECK(is_client_)
                << "** only server can handle request_acquire. msg: " << *msg;
            handle_response_acquire(msg);
            break;
        }
        case RpcType::kRelinquishResp:
        {
            auto *msg = (LeaseModifyResponse *) base;
            DVLOG(4) << "[patronus] handling lease modify response " << *msg
                     << " at patronus_now: " << time_syncer_->patronus_now();
            DCHECK(is_client_)
                << "** only server can handle request_acquire. msg: " << *msg;
            if constexpr (debug())
            {
                uint64_t digest = msg->digest.get();
                msg->digest = 0;
                DCHECK_EQ(digest,
                          djb2_digest(msg, sizeof(LeaseModifyResponse)));
            }
            handle_response_lease_relinquish(msg);
            break;
        }
        case RpcType::kExtendResp:
        {
            auto *msg = (LeaseModifyResponse *) base;
            DVLOG(4) << "[patronus] handling lease modify response " << *msg
                     << " at patronus_now: " << time_syncer_->patronus_now();
            DCHECK(is_client_)
                << "** only server can handle request_acquire. msg: " << *msg;
            if constexpr (debug())
            {
                uint64_t digest = msg->digest.get();
                msg->digest = 0;
                DCHECK_EQ(digest,
                          djb2_digest(msg, sizeof(LeaseModifyResponse)));
            }
            handle_response_lease_extend(msg);
            break;
        }
        case RpcType::kAdmin:
        {
            auto *msg = (AdminRequest *) base;
            DVLOG(4) << "[patronus] handling admin request " << *msg
                     << " at patronus_now: " << time_syncer_->patronus_now();

            if constexpr (debug())
            {
                uint64_t digest = msg->digest.get();
                msg->digest = 0;
                DCHECK_EQ(digest, djb2_digest(msg, sizeof(AdminRequest)));
            }

            auto admin_type = (AdminFlag) msg->flag;
            if (admin_type == AdminFlag::kAdminReqExit)
            {
                handle_admin_exit(msg, nullptr);
            }
            else
            {
                LOG(FATAL) << "Unknown admin type " << (int) admin_type;
            }
            break;
        }
        case RpcType::kMemoryResp:
        {
            auto *msg = (MemoryResponse *) base;

            if constexpr (debug())
            {
                uint64_t digest = msg->digest.get();
                msg->digest = 0;
                DCHECK_EQ(digest, djb2_digest(msg, msg->msg_size()))
                    << "** digest mismatch for message: " << *msg
                    << ". msg_size: " << msg->msg_size();
            }

            handle_response_memory_access(msg, nullptr);
            break;
        }
        case RpcType::kAdminResp:
        {
            auto *msg = (AdminResponse *) base;
            auto admin_type = (AdminFlag) msg->flag;

            if constexpr (debug())
            {
                uint64_t digest = msg->digest.get();
                msg->digest = 0;
                DCHECK_EQ(digest, djb2_digest(msg, sizeof(AdminResponse)))
                    << "** digest mismatch for message " << *msg;
            }

            CHECK(admin_type == AdminFlag::kAdminReqRecovery ||
                  admin_type == AdminFlag::kAdminQPtoRO ||
                  admin_type == AdminFlag::kAdminQPtoRW);

            handle_response_admin_qp_modification(msg, nullptr);
            break;
        }
        default:
        {
            LOG(FATAL) << "Unknown or invalid response type " << response_type
                       << ". Possible corrupted message";
        }
        }
    }
    return cur_idx;
}

void Patronus::prepare_handle_request_messages(
    const char *msg_buf,
    size_t msg_nr,
    ServerCoroBatchExecutionContext &ex_ctx,
    CoroContext *ctx)
{
    DCHECK_EQ(ex_ctx.prepared_size(), 0)
        << "** ex_ctx " << (void *) &ex_ctx << " for coro " << pre_coro_ctx(ctx)
        << " wr not empty";
    DCHECK_EQ(ex_ctx.req_size(), 0)
        << "** ex_ctx " << (void *) &ex_ctx << " for coro " << pre_coro_ctx(ctx)
        << " req not empty";

    for (size_t i = 0; i < msg_nr; ++i)
    {
        auto *base = (BaseMessage *) (msg_buf + i * kMessageSize);
        auto request_type = base->type;
        auto req_id = ex_ctx.fetch_req_idx();
        auto &req_ctx = ex_ctx.req_ctx(req_id);
        DCHECK_EQ(req_id, i);
        switch (request_type)
        {
        case RpcType::kAcquireNoLeaseReq:
        case RpcType::kAcquireRLeaseReq:
        case RpcType::kAcquireWLeaseReq:
        {
            auto *msg = (AcquireRequest *) base;
            DVLOG(4) << "[patronus] prepare handling acquire request " << *msg
                     << " coro " << *ctx;
            DCHECK(is_server_)
                << "** only server can handle request_acquire. msg: " << *msg;
            if constexpr (debug())
            {
                uint64_t digest = msg->digest.get();
                msg->digest = 0;
                DCHECK_EQ(digest, djb2_digest(msg, sizeof(AcquireRequest)))
                    << "** digest mismatch for req " << *msg;
            }
            prepare_handle_request_acquire(msg, req_ctx, ctx);
            break;
        }
        case RpcType::kExtendReq:
        case RpcType::kRelinquishReq:
        {
            auto *msg = (LeaseModifyRequest *) base;
            DVLOG(4) << "[patronus] prepare handling lease modify request "
                     << *msg << ", coro: " << *ctx;
            DCHECK(is_server_)
                << "** only server can handle request_acquire. msg: " << *msg;
            if constexpr (debug())
            {
                uint64_t digest = msg->digest.get();
                msg->digest = 0;
                DCHECK_EQ(digest, djb2_digest(msg, sizeof(LeaseModifyRequest)));
            }
            prepare_handle_request_lease_modify(msg, req_ctx, ctx);
            break;
        }
        case RpcType::kMemoryReq:
        {
            auto *msg = (MemoryRequest *) base;
            DVLOG(4) << "[patronus] prepare handling memory request " << *msg
                     << ", coro: " << *ctx;
            DCHECK(is_server_)
                << "** only server can handle request_acquire. msg: " << *msg;
            if constexpr (debug())
            {
                uint64_t digest = msg->digest.get();
                msg->digest = 0;
                DCHECK_EQ(digest, djb2_digest(msg, msg->msg_size()));
            }
            DCHECK(msg->validate());
            // do nothing
            break;
        }
        case RpcType::kAdmin:
        case RpcType::kAdminReq:
        {
            auto *msg = (AdminRequest *) base;
            DVLOG(4) << "[patronus] handling admin request " << *msg << *ctx;
            auto admin_type = (AdminFlag) msg->flag;

            if constexpr (debug())
            {
                uint64_t digest = msg->digest.get();
                msg->digest = 0;
                DCHECK_EQ(digest, djb2_digest(msg, sizeof(AdminRequest)));
            }

            if (admin_type == AdminFlag::kAdminReqExit)
            {
                handle_admin_exit(msg, nullptr);
            }
            else if (admin_type == AdminFlag::kAdminReqRecovery)
            {
                CHECK_EQ(request_type, RpcType::kAdminReq);
                handle_admin_recover(msg, nullptr);
            }
            else if (admin_type == AdminFlag::kAdminBarrier)
            {
                handle_admin_barrier(msg, nullptr);
            }
            else if (admin_type == AdminFlag::kAdminQPtoRO ||
                     admin_type == AdminFlag::kAdminQPtoRW)
            {
                CHECK_EQ(request_type, RpcType::kAdminReq);
                handle_admin_qp_access_flag(msg, nullptr);
            }
            else
            {
                LOG(FATAL) << "unknown admin type " << (int) admin_type;
            }
            break;
        }
        default:
        {
            LOG(FATAL) << "Unknown or invalid request type " << request_type
                       << ". Possible corrupted message";
        }
        }
    }
    DCHECK_EQ(ex_ctx.req_size(), msg_nr)
        << "** ex_ctx " << (void *) &ex_ctx << " for coro " << pre_coro_ctx(ctx)
        << " req size mismatch";
}

void Patronus::handle_response_acquire(AcquireResponse *resp)
{
    DCHECK_EQ(resp->cid.node_id, ::config::get_server_nids().front())
        << "** Should hold for the single-server case";

    auto rpc_ctx_id = resp->cid.rpc_ctx_id;
    auto *rpc_context = rpc_context_.id_to_obj(rpc_ctx_id);

    auto *request = (AcquireRequest *) rpc_context->request;

    DCHECK_EQ(resp->type, RpcType::kAcquireLeaseResp);

    switch (resp->status)
    {
    case AcquireRequestStatus::kSuccess:
        rpc_context->ret_code = RetCode::kOk;
        break;
    case AcquireRequestStatus::kLockedErr:
    case AcquireRequestStatus::kMagicMwErr:
        rpc_context->ret_code = RetCode::kRetry;
        break;
    case AcquireRequestStatus::kBindErr:
    case AcquireRequestStatus::kRegMrErr:
        rpc_context->ret_code = RetCode::kRdmaExecutionErr;
        break;
    case AcquireRequestStatus::kAddressOutOfRangeErr:
        rpc_context->ret_code = RetCode::kInvalid;
    case AcquireRequestStatus::kNoMw:
    case AcquireRequestStatus::kNoMem:
        rpc_context->ret_code = RetCode::kNoMem;
        break;
    case AcquireRequestStatus::kReserved:
    case AcquireRequestStatus::kReservedNoReturn:
    default:
        LOG(FATAL) << "** Invalid resp->status: " << (int) resp->status
                   << ". possibly reserved one.";
    }

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
        ret_lease.post_qp_idx_ = resp->post_qp_id;
        if (request->type == RpcType::kAcquireRLeaseReq)
        {
            ret_lease.lease_type_ = LeaseType::kReadLease;
        }
        else if (request->type == RpcType::kAcquireWLeaseReq)
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
        CHECK_NE(resp->status, AcquireRequestStatus::kReserved)
            << "** Server response with a reserved status. msg:" << *resp;
        ret_lease.set_error(resp->status);
    }

    if (!ret_lease.success())
    {
        CHECK_NE(resp->status, AcquireRequestStatus::kReserved)
            << "** Server response with a reserved status. msg:" << *resp;
    }

    rpc_context->ready.store(true, std::memory_order_release);
}

void Patronus::prepare_handle_request_acquire(AcquireRequest *req,
                                              HandleReqContext &req_ctx,
                                              CoroContext *ctx)
{
    debug_validate_acquire_request_flag(req->flag);

    DCHECK_NE(req->type, RpcType::kAcquireNoLeaseReq) << "** Deprecated";

    auto dirID = req->dir_id;
    DCHECK_EQ(dirID, get_thread_id())
        << "** Currently we limit that server tid handles dir_id";

    auto &ex_ctx = coro_ex_ctx(ctx->coro_id());

    AcquireRequestStatus status = AcquireRequestStatus::kSuccess;

    uint64_t object_buffer_offset = 0;
    uint64_t object_dsm_offset = 0;
    void *object_addr = nullptr;
    void *header_addr = nullptr;
    uint64_t header_dsm_offset = 0;
    bool with_conflict_detect = false;
    uint64_t bucket_id = 0;
    uint64_t slot_id = 0;
    ProtectionRegion *protection_region = nullptr;
    uint64_t protection_region_id = 0;
    ibv_mr *buffer_mr = nullptr;
    ibv_mr *header_mr = nullptr;

    bool no_bind_pr = req->flag & (flag_t) AcquireRequestFlag::kNoBindPR;
    bool no_bind_any = req->flag & (flag_t) AcquireRequestFlag::kNoBindAny;
    bool with_alloc = req->flag & (flag_t) AcquireRequestFlag::kWithAllocation;
    bool only_alloc = req->flag & (flag_t) AcquireRequestFlag::kOnlyAllocation;
    bool debug_srv_do_nothing =
        req->flag & (flag_t) AcquireRequestFlag::kDebugServerDoNothing;
    bool dbg_flg_1 = req->flag & (flag_t) AcquireRequestFlag::kDebugFlag_1;

    bool use_mr = req->flag & (flag_t) AcquireRequestFlag::kUseMR;

    bool with_lock =
        req->flag & (flag_t) AcquireRequestFlag::kWithConflictDetect;
    bool i_acquire_the_lock = false;

    // below are default settings
    bool alloc_buf = false;
    bool alloc_pr = true;
    bool bind_buf = true;
    bool bind_pr = true;
    bool alloc_lease_ctx = true;

    if (debug_srv_do_nothing)
    {
        alloc_buf = false;
        alloc_pr = false;
        bind_buf = false;
        bind_pr = false;
        alloc_lease_ctx = false;
    }
    if (dbg_flg_1)
    {
        // do what you want
    }

    // if not binding pr, we will even skip allocating it.
    if (no_bind_pr)
    {
        alloc_pr = false;
        bind_pr = false;
    }
    if (no_bind_any)
    {
        // same as no_bind_pr
        alloc_pr = false;
        bind_pr = false;
        bind_buf = false;
    }

    // NOTE:
    // even if with_alloc is true, does not imply that pr is disabled.
    // e.g. alloc + require(lease semantics)
    if (with_alloc)
    {
        alloc_buf = true;
    }
    if (only_alloc)
    {
        alloc_buf = true;
        alloc_pr = false;
        bind_buf = false;
        bind_pr = false;
        alloc_lease_ctx = false;
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
            goto out;
        }
        else
        {
            i_acquire_the_lock = true;
        }
    }

    if (alloc_buf)
    {
        // allocation
        object_addr = patronus_alloc(req->size, req->key /* hint */);
        if (unlikely(object_addr == nullptr))
        {
            status = AcquireRequestStatus::kNoMem;
            goto out;
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
            goto out;
        }
        object_dsm_offset =
            dsm_->buffer_offset_to_dsm_offset(object_buffer_offset);
        object_addr = dsm_->buffer_offset_to_addr(object_buffer_offset);
        DCHECK_EQ(object_addr, dsm_->dsm_offset_to_addr(object_dsm_offset));
    }

    if (likely(alloc_pr))
    {
        protection_region = DCHECK_NOTNULL(get_protection_region());
        protection_region_id =
            protection_region_pool_->buf_to_id(protection_region);
        header_addr = protection_region;
        header_dsm_offset = dsm_->addr_to_dsm_offset(header_addr);
    }

    if (req->type == RpcType::kAcquireRLeaseReq ||
        req->type == RpcType::kAcquireWLeaseReq)
    {
        if (use_mr)
        {
            auto *rdma_ctx = DCHECK_NOTNULL(dsm_->get_rdma_context(dirID));
            if (bind_buf)
            {
                buffer_mr = createMemoryRegion(
                    (uint64_t) object_addr, req->size, rdma_ctx);
                if (unlikely(buffer_mr == nullptr))
                {
                    status = AcquireRequestStatus::kRegMrErr;
                    goto out;
                }
            }
            if (bind_pr)
            {
                header_mr = createMemoryRegion(
                    (uint64_t) header_addr, sizeof(ProtectionRegion), rdma_ctx);
                if (unlikely(header_mr == nullptr))
                {
                    status = AcquireRequestStatus::kRegMrErr;
                    goto out;
                }
            }
        }
        else
        {
            auto *mr = dsm_->get_dir_mr(dirID);
            int access_flag = req->type == RpcType::kAcquireRLeaseReq
                                  ? IBV_ACCESS_CUSTOM_REMOTE_RO
                                  : IBV_ACCESS_CUSTOM_REMOTE_RW;

            size_t bind_req_nr = 0;

            if (bind_buf && !bind_pr)
            {
                bind_req_nr = 1;
                ex_ctx.prepare_bind_mw(mr,
                                       (uint64_t) DCHECK_NOTNULL(object_addr),
                                       req->size,
                                       access_flag,
                                       &req_ctx.status,
                                       &req_ctx.acquire.buffer_mw);

                DVLOG(4) << "[patronus] Bind mw for buffer. addr "
                         << (void *) object_addr
                         << "(dsm_offset: " << object_dsm_offset
                         << "), size: " << req->size << " with access flag "
                         << (int) access_flag << ". Acquire flag: "
                         << AcquireRequestFlagOut(req->flag);
            }
            else if (bind_buf && bind_pr)
            {
                ex_ctx.prepare_bind_mw(mr,
                                       (uint64_t) DCHECK_NOTNULL(object_addr),
                                       req->size,
                                       access_flag,
                                       &req_ctx.status,
                                       &req_ctx.acquire.buffer_mw);
                DVLOG(4) << "[patronus] Bind mw for buffer. addr "
                         << (void *) object_addr
                         << "(dsm_offset: " << object_dsm_offset
                         << "), size: " << req->size << " with access flag "
                         << (int) access_flag << ". Acquire flag: "
                         << AcquireRequestFlagOut(req->flag);
                // for header
                ex_ctx.prepare_bind_mw(mr,
                                       (uint64_t) header_addr,
                                       sizeof(ProtectionRegion),
                                       IBV_ACCESS_CUSTOM_REMOTE_RW,
                                       &req_ctx.status,
                                       &req_ctx.acquire.header_mw);
                DVLOG(4) << "[patronus] Bind mw for header. addr: "
                         << (void *) header_addr
                         << " (dsm_offset: " << header_dsm_offset << ")"
                         << ", size: " << sizeof(ProtectionRegion)
                         << " with R/W access. Acquire flag: "
                         << AcquireRequestFlagOut(req->flag);
            }
            else
            {
                CHECK(!bind_pr && !bind_buf)
                    << "invalid configuration. bind_pr: " << bind_pr
                    << ", bind_buf: " << bind_buf;
            }
        }
    }
    else if (req->type == RpcType::kAcquireNoLeaseReq)
    {
        // do nothing
    }
    else
    {
        LOG(FATAL) << "Unknown or unsupport request type " << (int) req->type
                   << " for req " << *req;
    }

out:
    req_ctx.status = status;
    req_ctx.acquire.buffer_mr = buffer_mr;
    req_ctx.acquire.header_mr = header_mr;
    req_ctx.acquire.object_addr = (uint64_t) object_addr;
    req_ctx.acquire.protection_region_id = protection_region_id;
    req_ctx.acquire.bucket_id = bucket_id;
    req_ctx.acquire.slot_id = slot_id;
    req_ctx.acquire.object_dsm_offset = object_dsm_offset;
    req_ctx.acquire.header_dsm_offset = header_dsm_offset;
    req_ctx.acquire.protection_region = protection_region;
    req_ctx.acquire.with_conflict_detect = with_conflict_detect;
    req_ctx.acquire.alloc_lease_ctx = alloc_lease_ctx;
    req_ctx.acquire.bind_pr = bind_pr;
    req_ctx.acquire.bind_buf = bind_buf;
    req_ctx.acquire.alloc_pr = alloc_pr;
    req_ctx.acquire.use_mr = use_mr;
    req_ctx.acquire.with_alloc = with_alloc;
    req_ctx.acquire.only_alloc = only_alloc;
    req_ctx.acquire.with_lock = with_lock;
    req_ctx.acquire.i_acquire_the_lock = i_acquire_the_lock;
}

void Patronus::prepare_handle_request_lease_modify(
    LeaseModifyRequest *req,
    HandleReqContext &req_ctx,
    [[maybe_unused]] CoroContext *ctx)
{
    debug_validate_lease_modify_flag(req->flag);
    auto type = req->type;
    switch (type)
    {
    case RpcType::kRelinquishReq:
    {
        prepare_handle_request_lease_relinquish(req, req_ctx, ctx);
        break;
    }
    case RpcType::kExtendReq:
    {
        // do nothing
        // the task is postponed to post_handle_request_lease_extend
        // because we directly get whether extension succeeds
        // and response to the client
        break;
    }
    default:
    {
        LOG(FATAL) << "unknown/invalid request type " << (int) type
                   << " for coro" << (ctx ? *ctx : nullctx);
    }
    }
}

void Patronus::prepare_handle_request_lease_relinquish(
    LeaseModifyRequest *req, HandleReqContext &req_ctx, CoroContext *ctx)
{
    DCHECK_EQ(req->type, RpcType::kRelinquishReq);

    bool only_dealloc = req->flag & (flag_t) LeaseModifyFlag::kOnlyDeallocation;
    if (only_dealloc)
    {
        auto dsm_offset = req->addr;
        auto *addr = dsm_->dsm_offset_to_addr(dsm_offset);
        patronus_free(addr, req->size, req->hint);
        return;
    }

    auto lease_id = req->lease_id;
    prepare_gc_lease(lease_id,
                     req_ctx,
                     req->cid,
                     req->flag | (flag_t) LeaseModifyFlag::kForceUnbind,
                     ctx);

    req_ctx.lease_id = req->lease_id;
}

void Patronus::post_handle_request_messages(
    const char *msg_buf,
    size_t msg_nr,
    ServerCoroBatchExecutionContext &ex_ctx,
    CoroContext *ctx)
{
    DCHECK_EQ(ex_ctx.req_size(), msg_nr)
        << "** req size not matched. ex_ctx: " << &ex_ctx
        << ", coro: " << pre_coro_ctx(ctx);

    for (size_t i = 0; i < msg_nr; ++i)
    {
        auto *base = (BaseMessage *) (msg_buf + i * kMessageSize);
        auto request_type = base->type;
        auto &req_ctx = ex_ctx.req_ctx(i);
        switch (request_type)
        {
        case RpcType::kAcquireNoLeaseReq:
        case RpcType::kAcquireRLeaseReq:
        case RpcType::kAcquireWLeaseReq:
        {
            auto *msg = (AcquireRequest *) base;
            DVLOG(4) << "[patronus] post handling acquire request " << *msg
                     << " coro " << *ctx;
            DCHECK(is_server_)
                << "** only server can handle request_acquire. msg: " << *msg;
            post_handle_request_acquire(msg, req_ctx, ctx);
            break;
        }
        case RpcType::kExtendReq:
        {
            auto *msg = (LeaseModifyRequest *) base;
            DVLOG(4) << "[patronus] post handling lease modify request " << *msg
                     << ", coro: " << *ctx;
            DCHECK(is_server_)
                << "** only server can handle request_acquire. msg: " << *msg;
            post_handle_request_lease_extend(msg, ctx);
            break;
        }
        case RpcType::kRelinquishReq:
        {
            auto *msg = (LeaseModifyRequest *) base;
            DVLOG(4) << "[patronus] post handling lease modify request " << *msg
                     << ", coro: " << *ctx;
            DCHECK(is_server_)
                << "** only server can handle request_acquire. msg: " << *msg;
            post_handle_request_lease_relinquish(msg, req_ctx, ctx);
            break;
        }
        case RpcType::kMemoryReq:
        {
            auto *msg = (MemoryRequest *) base;
            DVLOG(4) << "[patronus] post handling lease modify request " << *msg
                     << ", coro: " << *ctx;
            DCHECK(is_server_)
                << "** only server can handle request_acquire. msg: " << *msg;
            post_handle_request_memory_access(msg, ctx);
            break;
        }
        case RpcType::kAdmin:
        case RpcType::kAdminReq:
        {
            // do nothing
            auto *msg = (AdminRequest *) base;
            DVLOG(4) << "[patronus] post handling admin request " << *msg
                     << ", coro: " << *ctx;
            DCHECK(is_server_)
                << "** only server can handle request_acquire. msg: " << *msg;
            post_handle_request_admin(msg, ctx);
            break;
        }
        default:
        {
            LOG(FATAL) << "Unknown or invalid request type " << request_type
                       << ". Possible corrupted message";
        }
        }
    }
}

size_t Patronus::handle_rdma_finishes(
    ibv_wc *wc_buffer,
    size_t rdma_nr,
    coro_t *coro_buf,
    std::map<std::pair<size_t, size_t>, TraceView> &recov)
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
               wr_id.prefix == WRID_PREFIX_PATRONUS_BATCH_RWCAS ||
               wr_id.prefix == WRID_PREFIX_PATRONUS_GENERATE_FAULT)
            << "** unexpected prefix " << (int) wr_id.prefix;
        auto *rw_context = rw_context_.id_to_obj(id);
        auto node_id = rw_context->target_node;
        auto dir_id = rw_context->dir_id;
        auto coro_id = rw_context->coro_id;
        CHECK_NE(coro_id, kMasterCoro)
            << "** Coro master should not issue R/W.";

        rw_context->trace_view.pin("arrived");

        if (likely(wc.status == IBV_WC_SUCCESS))
        {
            rw_context->wc_status = IBV_WC_SUCCESS;
            DVLOG(4) << "[patronus] handle rdma finishes SUCCESS for coro "
                     << (int) coro_id << ". set "
                     << (void *) rw_context->wc_status << " to IBV_WC_SUCCESS";
        }
        else
        {
            LOG(WARNING) << "[patronus] rdma R/W/CAS failed. wr_id: " << wr_id
                         << " for node " << node_id << ", dir: " << dir_id
                         << ", coro_id: " << (int) coro_id
                         << ". detail: " << wc;
            rw_context->wc_status = wc.status;
            recov.emplace(std::make_pair(node_id, dir_id),
                          rw_context->trace_view);
            fail_nr++;
        }
        rw_context->ready.store(true, std::memory_order_release);

        coro_buf[cur_idx++] = coro_id;
    }
    return fail_nr;
}

void Patronus::finished(uint64_t key)
{
    VLOG(1) << "[patronus] finished(" << key << ")";
    finished_[key][dsm_->get_node_id()] = true;

    auto tid = get_thread_id();
    // does not require replies
    auto to_dir_id = admin_dir_id();

    char *rdma_buf = get_rdma_message_buffer();
    auto *msg = (AdminRequest *) rdma_buf;
    msg->type = RpcType::kAdmin;
    msg->flag = (flag_t) AdminFlag::kAdminReqExit;
    msg->cid.node_id = get_node_id();
    msg->cid.thread_id = tid;
    msg->cid.coro_id = kMasterCoro;
    msg->data = key;
    msg->need_response = false;

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
        dsm_->unreliable_send(rdma_buf, sizeof(AdminRequest), i, to_dir_id);
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
    LOG(WARNING) << "[patronus] QP recovering. req: " << *req;

    ContTimer<::config::kMonitorFailureRecovery> timer;
    timer.init("Recover Dir QP");
    auto from_node = req->cid.node_id;
    auto tid = req->cid.thread_id;
    auto dir_id = req->dir_id;
    CHECK(dsm_->recoverDirQP(from_node, tid, dir_id));
    timer.pin("finished");

    LOG_IF(INFO, ::config::kMonitorFailureRecovery)
        << "[patronus] timer: " << timer;
}

void Patronus::handle_admin_qp_access_flag(AdminRequest *req,
                                           [[maybe_unused]] CoroContext *ctx)
{
    LOG(WARNING) << "[patronus] QP recovering. req: " << *req;

    auto from_node = req->cid.node_id;
    auto tid = req->cid.thread_id;
    auto dir_id = req->dir_id;
    if (req->flag == (flag_t) AdminFlag::kAdminQPtoRO)
    {
        CHECK(dsm_->modify_dir_qp_access_flag(
            from_node, tid, dir_id, IBV_ACCESS_REMOTE_READ));
    }
    else
    {
        CHECK_EQ(req->flag, (flag_t) AdminFlag::kAdminQPtoRW);
        CHECK(dsm_->modify_dir_qp_access_flag(
            from_node,
            tid,
            dir_id,
            (flag_t) IBV_ACCESS_REMOTE_READ |
                (flag_t) IBV_ACCESS_REMOTE_WRITE));
    }
}

void Patronus::handle_admin_exit(AdminRequest *req,
                                 [[maybe_unused]] CoroContext *ctx)
{
    auto from_node = req->cid.node_id;
    auto key = req->data;
    DCHECK_LT(key, finished_.size());
    DCHECK_LT(from_node, finished_[key].size());
    finished_[key][from_node].store(true);

    for (size_t i = 0; i < dsm_->getClusterSize(); ++i)
    {
        if (!finished_[key][i])
        {
            VLOG(1) << "[patronus] receive exit request by " << from_node
                    << ". but (at least) node " << i
                    << " not finished yet. key: " << key;
            return;
        }
    }

    VLOG(1) << "[patronus] set should_exit to true";
    DCHECK_LT(key, should_exit_.size());
    should_exit_[key].store(true, std::memory_order_release);
}
void Patronus::signal_server_to_recover_qp(size_t node_id,
                                           size_t dir_id,
                                           TraceView v)
{
    auto rc = admin_request_impl(node_id,
                                 dir_id,
                                 0 /* data */,
                                 (flag_t) AdminFlag::kAdminReqRecovery,
                                 false /* need response */,
                                 nullptr);
    DCHECK_EQ(rc, kOk);

    v.pin("signal-server-recovery");
}
void Patronus::registerServerThread()
{
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
    auto dir_id = tid;
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

    coro_batch_ex_ctx_.resize(kMaxCoroNr);
    for (size_t i = 0; i < kMaxCoroNr; ++i)
    {
        coro_batch_ex_ctx_[i].init(this, dir_id);
    }

    CHECK(!is_server_) << "** already registered";
    CHECK(!is_client_) << "** already registered as client thread.";
    is_server_ = true;
}

void Patronus::registerClientThread()
{
    auto desc = prepare_client_thread(true /* is_registering_thread */);
    auto succ = apply_client_resource(std::move(desc), true /* bind_core */);
    CHECK(succ);
    has_registered_ = true;
}

size_t Patronus::try_get_client_continue_coros(coro_t *coro_buf, size_t limit)
{
    static thread_local char buf[config::umsg::kMaxRecvBuffer];
    auto nr = dsm_->unreliable_try_recv(
        buf, std::min(limit, config::umsg::kRecvLimit));
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

    using node_t = size_t;
    using dir_t = size_t;
    std::map<std::pair<node_t, dir_t>, TraceView> recovery;
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
        auto trace = recovery.begin()->second;
        // ignore dsm_->try_poll_rdma_cq, ignoring any on-going one-sided
        // requests (if any)
        // ignore dsm_- >reliable_try_recv, ignoring any on-going two-sided
        // requests (if any)
        bool succ = fast_switch_backup_qp(trace);
        if (succ)
        {
            DVLOG(1) << "[patronus] succeeded in QP recovery fast path.";
            return cur_idx;
        }
        else
        {
            DVLOG(1) << "[patronus] failed in QP recovery fast path.";
        }

        LOG(WARNING) << "[patronus] QP failed. Recovering is triggered. tid: "
                     << get_thread_id();
        ContTimer<::config::kMonitorFailureRecovery> timer;
        timer.init("Failure recovery");

        if constexpr (debug())
        {
            DVLOG(1) << "[patronus] failed. expect nr: "
                     << rw_context_.ongoing_size();
        }
        while (fail_nr < rw_context_.ongoing_size())
        {
            auto another_nr = dsm_->try_poll_rdma_cq(wc_buffer, kBufferSize);
            LOG(WARNING)
                << "[patronus] QP recovering: handling on-going rdma finishes";
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
        // memory bind call
        // otherwise, the server will recovery QP and drop the
        // failed window_bind calls then the client corotine will be waiting
        // forever.
        size_t got = msg_nr;
        size_t expect_rpc_nr = rpc_context_.ongoing_size();
        while (got < rpc_context_.ongoing_size())
        {
            nr = dsm_->unreliable_try_recv(
                buf, std::min(limit, config::umsg::kRecvLimit));
            got += nr;
            LOG_IF(WARNING, nr > 0)
                << "[patronus] QP recovering: handling on-going rpc. Got "
                   "another "
                << nr << " responses, cur: " << got << ", expect another "
                << expect_rpc_nr;
            nr = handle_response_messages(buf, nr, coro_buf + cur_idx);
            cur_idx += nr;
            CHECK_LT(cur_idx, limit);
        }
        timer.pin("Wait RPC");

        for (const auto &[position, trace_view] : recovery)
        {
            auto node_id = position.first;
            auto dir_id = position.second;
            signal_server_to_recover_qp(node_id, dir_id, trace_view);
        }

        timer.pin("signal server");
        // for (const auto &tp : recovery)
        for (const auto &[position, trace_view] : recovery)
        {
            auto node_id = position.first;
            auto dir_id = position.second;
            CHECK(dsm_->recoverThreadQP(node_id, dir_id, trace_view));
        }
        timer.pin("recover QP");
        LOG_IF(INFO, ::config::kMonitorFailureRecovery)
            << "[patronus] client recovery: " << timer;
    }
    else
    {
        DCHECK(recovery.empty());
    }

    return cur_idx;
}

void Patronus::handle_response_memory_access(MemoryResponse *resp,
                                             CoroContext *ctx)
{
    std::ignore = ctx;

    auto type = resp->type;
    DCHECK_EQ(type, RpcType::kMemoryResp);
    auto rpc_ctx_id = resp->cid.rpc_ctx_id;
    auto *rpc_context = rpc_context_.id_to_obj(rpc_ctx_id);

    auto mf = (MemoryRequestFlag) resp->flag;

    if (resp->success)
    {
        rpc_context->ret_code = RetCode::kOk;
        if (mf == MemoryRequestFlag::kRead)
        {
            memcpy(rpc_context->buffer_addr, resp->buffer, resp->size);
        }
    }
    else
    {
        if (mf == MemoryRequestFlag::kCAS)
        {
            rpc_context->ret_code = RetCode::kRetry;
        }
        else
        {
            rpc_context->ret_code = RetCode::kRdmaExecutionErr;
        }
    }

    if (mf == MemoryRequestFlag::kCAS)
    {
        DCHECK_EQ(resp->size, sizeof(uint64_t));
        memcpy(rpc_context->buffer_addr, resp->buffer, resp->size);
    }

    rpc_context->ready.store(true, std::memory_order_release);
}
void Patronus::handle_response_lease_relinquish(LeaseModifyResponse *resp)
{
    auto type = resp->type;
    DCHECK_EQ(type, RpcType::kRelinquishResp);

    auto rpc_ctx_id = resp->cid.rpc_ctx_id;
    auto *rpc_context = rpc_context_.id_to_obj(rpc_ctx_id);

    DCHECK(resp->success) << "** relinquish has to success";

    rpc_context->ret_code = RetCode::kOk;
    rpc_context->ready.store(true, std::memory_order_release);
}

void Patronus::handle_response_lease_extend(LeaseModifyResponse *resp)
{
    auto type = resp->type;
    DCHECK_EQ(type, RpcType::kExtendResp);

    auto rpc_ctx_id = resp->cid.rpc_ctx_id;
    auto *rpc_context = rpc_context_.id_to_obj(rpc_ctx_id);

    if (resp->success)
    {
        rpc_context->ret_code = RetCode::kOk;
    }
    else
    {
        rpc_context->ret_code = RetCode::kInvalid;
    }

    rpc_context->ready.store(true, std::memory_order_release);
}

void Patronus::server_serve(uint64_t key)
{
    DVLOG(1) << "[patronus] server_serve(" << key << ")";
    auto &server_workers = server_coro_ctx_.server_workers;
    auto &server_master = server_coro_ctx_.server_master;

    for (size_t i = 0; i < kServerCoroNr; ++i)
    {
        server_workers[i] = CoroCall([this, i, key](CoroYield &yield) {
            server_coro_worker(i, yield, key);
        });
    }
    server_master = CoroCall(
        [this, key](CoroYield &yield) { server_coro_master(yield, key); });

    server_master();
}

void Patronus::server_coro_master(CoroYield &yield, uint64_t wait_key)
{
    auto tid = get_thread_id();
    auto dir_id = tid;

    auto &server_workers = server_coro_ctx_.server_workers;
    CoroContext mctx(tid, &yield, server_workers);
    auto &comm = server_coro_ctx_.comm;
    auto &task_pool = server_coro_ctx_.task_pool;
    auto &task_queue = comm.task_queue;

    CHECK(mctx.is_master());

    LOG(INFO) << "[patronus] server thread " << tid
              << " handlin dir_id = " << dir_id;

    for (size_t i = 0; i < kMaxCoroNr; ++i)
    {
        comm.finished[i] = true;
    }

    constexpr static size_t kServerBufferNr =
        kMaxCoroNr * MAX_MACHINE * kClientThreadPerServerThread;
    std::vector<char> __buffer;
    __buffer.resize(config::umsg::kMaxRecvBuffer * kServerBufferNr);

    server_coro_ctx_.buffer_pool = std::make_unique<
        ThreadUnsafeBufferPool<::config::umsg::kMaxRecvBuffer>>(
        __buffer.data(), config::umsg::kMaxRecvBuffer * kServerBufferNr);
    auto &buffer_pool = *server_coro_ctx_.buffer_pool;

    OnePassBucketMonitor batch_m(
        (uint64_t) 0, (uint64_t) config::umsg::kRecvLimit, (uint64_t) 1);

    while (likely(!should_exit(wait_key) || likely(!task_queue.empty())) ||
           likely(!std::all_of(std::begin(comm.finished),
                               std::end(comm.finished),
                               [](bool i) { return i; })))
    {
        // handle received messages
        char *buffer = (char *) CHECK_NOTNULL(buffer_pool.get());
        size_t nr = dsm_->unreliable_try_recv(buffer, config::umsg::kRecvLimit);
        if (likely(nr > 0))
        {
            DVLOG(4) << "[patronus] server recv messages " << nr;
            auto *task = DCHECK_NOTNULL(task_pool.get());
            task->buf = DCHECK_NOTNULL(buffer);
            task->msg_nr = nr;
            task->fetched_nr = 0;
            task->active_coro_nr = 0;
            comm.task_queue.push(task);
            batch_m.collect(nr);
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
                                 << " for further new task. " << mctx;
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
            auto prefix = wrid.prefix;
            bool should_signal = wrid.u16_a;
            auto rw_ctx_id = wrid.id;
            DCHECK(prefix == WRID_PREFIX_PATRONUS_BATCH ||
                   prefix == WRID_PREFIX_PATRONUS_BIND_MW_MAGIC_ERR)
                << "** unexpected prefix " << pre_wrid_prefix(prefix) << " ("
                << (int) prefix << "). wrid: " << wrid;

            if (unlikely(!should_signal))
            {
                DLOG(WARNING)
                    << "[patronus] server_coro_master: ignoring reserved "
                       "wr_id: "
                    << wrid
                    << ". Possible from a failed unsignaled wr. status: "
                    << ibv_wc_status_str(wc.status);
                continue;
            }

            auto *rw_ctx = rw_context_.id_to_obj(rw_ctx_id);
            auto coro_id = rw_ctx->coro_id;
            CHECK_NE(coro_id, kNotACoro);
            CHECK_NE(coro_id, kMasterCoro)
                << "not sure. I this assert fail, rethink about me.";
            if (unlikely(wc.status != IBV_WC_SUCCESS))
            {
                log_wc_handler(&wc);
                LOG(WARNING) << "[patronus] server got failed wc: " << wrid
                             << " for coro " << (int) coro_id << ". " << mctx;
                // need_recovery = true;
                rw_ctx->wc_status = wc.status;
            }
            else
            {
                DVLOG(4) << "[patronus] server got dir CQE for coro "
                         << (int) coro_id << ". wr_id: " << wrid << ". "
                         << mctx;
                rw_ctx->wc_status = IBV_WC_SUCCESS;
            }
            rw_ctx->ready.store(true, std::memory_order_release);

            DVLOG(4) << "[patronus] server yield to coro " << (int) coro_id
                     << " for Dir CQE. rw_ctx_id: " << rw_ctx_id
                     << ", ready at " << (void *) &rw_ctx->ready
                     << ", rw_ctx at " << (void *) rw_ctx << ", ready is "
                     << rw_ctx->ready;
            mctx.yield_to_worker(coro_id, wrid);
        }
    }

    bool all_finished = std::all_of(std::begin(comm.finished),
                                    std::end(comm.finished),
                                    [](bool b) { return b; });
    CHECK(all_finished) << "** Master coroutine trying to exist with "
                           "unfinished worker coroutine.";
    LOG(INFO) << "[debug] batch_m: " << batch_m;
}

void Patronus::post_handle_request_acquire(AcquireRequest *req,
                                           HandleReqContext &req_ctx,
                                           CoroContext *ctx)
{
    auto &ex_ctx = coro_ex_ctx(ctx->coro_id());

    ibv_mw *buffer_mw = req_ctx.acquire.buffer_mw;
    ibv_mw *header_mw = req_ctx.acquire.header_mw;
    ibv_mr *buffer_mr = req_ctx.acquire.buffer_mr;
    ibv_mr *header_mr = req_ctx.acquire.header_mr;
    uint64_t object_addr = req_ctx.acquire.object_addr;
    uint64_t protection_region_id = req_ctx.acquire.protection_region_id;
    uint64_t bucket_id = req_ctx.acquire.bucket_id;
    uint64_t slot_id = req_ctx.acquire.slot_id;
    uint64_t object_dsm_offset = req_ctx.acquire.object_dsm_offset;
    uint64_t header_dsm_offset = req_ctx.acquire.header_dsm_offset;
    ProtectionRegion *protection_region = req_ctx.acquire.protection_region;
    bool with_conflict_detect = req_ctx.acquire.with_conflict_detect;
    bool alloc_lease_ctx = req_ctx.acquire.alloc_lease_ctx;
    bool bind_pr = req_ctx.acquire.bind_pr;
    bool bind_buf = req_ctx.acquire.bind_buf;
    bool alloc_pr = req_ctx.acquire.alloc_pr;
    bool use_mr = req_ctx.acquire.use_mr;
    bool with_alloc = req_ctx.acquire.with_alloc;
    bool only_alloc = req_ctx.acquire.only_alloc;
    bool with_lock = req_ctx.acquire.with_lock;
    bool i_acquire_the_lock = req_ctx.acquire.i_acquire_the_lock;

    auto status = req_ctx.status;
    DCHECK_NE(status, AcquireRequestStatus::kReserved);

    auto dir_id = req->dir_id;

    auto *resp_buf = get_rdma_message_buffer();
    auto *resp_msg = (AcquireResponse *) resp_buf;
    resp_msg->type = RpcType::kAcquireLeaseResp;
    resp_msg->cid = req->cid;
    resp_msg->cid.node_id = get_node_id();
    resp_msg->cid.thread_id = get_thread_id();
    resp_msg->status = status;
    resp_msg->post_qp_id = 0 /* not used. delete this field later */;

    CHECK_NE(resp_msg->status, AcquireRequestStatus::kReserved)
        << "** Server should not response a reserved status. "
        << pre_coro_ctx(ctx);

    using LeaseIdT = decltype(AcquireResponse::lease_id);

    bool no_gc = req->flag & (flag_t) AcquireRequestFlag::kNoGc;
    bool debug_srv_do_nothing =
        req->flag & (flag_t) AcquireRequestFlag::kDebugServerDoNothing;
    if (debug_srv_do_nothing)
    {
        no_gc = true;
        DCHECK(!alloc_lease_ctx);
    }

    if (likely(status == AcquireRequestStatus::kSuccess))
    {
        // set to a scaring value so that we know it is invalid.
        LeaseIdT lease_id = std::numeric_limits<LeaseIdT>::max();
        if (likely(alloc_lease_ctx))
        {
            auto *lease_ctx = get_lease_context();
            lease_ctx->client_cid = req->cid;
            lease_ctx->buffer_mw = buffer_mw;
            lease_ctx->header_mw = header_mw;
            lease_ctx->buffer_mr = buffer_mr;
            lease_ctx->header_mr = header_mr;
            lease_ctx->dir_id = dir_id;
            lease_ctx->addr_to_bind = (uint64_t) object_addr;
            lease_ctx->buffer_size = req->size;
            lease_ctx->with_pr = bind_pr;
            lease_ctx->with_buf = bind_buf;
            lease_ctx->hint = req->key;

            if (likely(alloc_pr))
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
        if (bind_buf)
        {
            uint32_t buffer_rkey = use_mr ? buffer_mr->rkey : buffer_mw->rkey;
            resp_msg->rkey_0 = buffer_rkey;
        }
        else
        {
            resp_msg->rkey_0 = 0;
        }
        if (bind_pr)
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

        bool reserved = req->flag & (flag_t) AcquireRequestFlag::kReserved;
        DCHECK(!reserved)
            << "reserved flag should not be set. Possible corrupted message "
            << pre_coro_ctx(ctx);

        compound_uint64_t aba_unit_to_ddl(0);
        if (likely(alloc_pr))
        {
            aba_unit_to_ddl = protection_region->aba_unit_nr_to_ddl.load(
                std::memory_order_acq_rel);
            aba_unit_to_ddl.u32_2 = 1;
            protection_region->aba_unit_nr_to_ddl.store(
                aba_unit_to_ddl, std::memory_order_acq_rel);
        }

        auto patronus_now = time_syncer_->patronus_now();
        resp_msg->aba_id = aba_unit_to_ddl.u32_1;
        resp_msg->begin_term = patronus_now.term();
        resp_msg->ns_per_unit = ns_per_unit;

        // NOTE: we allow enable gc while disable pr.
        if (likely(!no_gc))
        {
            CHECK(alloc_lease_ctx);
            auto patronus_ddl_term = patronus_now.term() + ns_per_unit;

            auto flag = (flag_t) 0;
            ddl_manager_.push(
                patronus_ddl_term,
                [this, lease_id, cid = req->cid, aba_unit_to_ddl, flag](
                    CoroContext *ctx) {
                    task_gc_lease(lease_id, cid, aba_unit_to_ddl, flag, ctx);
                });
        }
        if (alloc_pr)
        {
            protection_region->begin_term = patronus_now.term();
            protection_region->ns_per_unit = req->required_ns;
            DCHECK_GT(aba_unit_to_ddl.u32_2, 0);
            memset(&(protection_region->meta), 0, sizeof(ProtectionRegionMeta));
        }
    }
    else
    {
        // gc here for all allocated resources
        resp_msg->lease_id = std::numeric_limits<LeaseIdT>::max();

        ex_ctx.put_mw(buffer_mw);
        ex_ctx.put_mw(header_mw);
        buffer_mw = nullptr;
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
            patronus_free((void *) object_addr, req->size, req->key /* hint */);
        }
        if (with_lock && i_acquire_the_lock)
        {
            auto [b, s] = locate_key(req->key);
            bucket_id = b;
            slot_id = s;
            lock_manager_.unlock(bucket_id, slot_id);
        }
    }

    dsm_->unreliable_send((char *) resp_msg,
                          sizeof(AcquireResponse),
                          req->cid.node_id,
                          req->cid.thread_id);

    put_rdma_message_buffer(resp_buf);
    DCHECK_EQ(alloc_pr, bind_pr)
        << "currently stick to thisconstrain " << pre_coro_ctx(ctx);
}

void Patronus::post_handle_request_admin(AdminRequest *req, CoroContext *ctx)
{
    std::ignore = ctx;
    if (unlikely(!req->need_response))
    {
        return;
    }
    auto *resp_buf = get_rdma_message_buffer();
    auto &resp_msg = *(AdminResponse *) resp_buf;
    resp_msg.type = RpcType::kAdminResp;
    resp_msg.cid = req->cid;
    resp_msg.cid.node_id = get_node_id();
    resp_msg.cid.thread_id = get_thread_id();
    resp_msg.flag = req->flag;
    resp_msg.success = true;

    if constexpr (debug())
    {
        resp_msg.digest = 0;
        resp_msg.digest = djb2_digest(&resp_msg, sizeof(AdminResponse));
    }

    dsm_->unreliable_send((const char *) resp_buf,
                          sizeof(AdminResponse),
                          req->cid.node_id,
                          req->cid.thread_id);
    put_rdma_message_buffer(resp_buf);
}

void Patronus::post_handle_request_lease_extend(LeaseModifyRequest *req,
                                                CoroContext *ctx)
{
    std::ignore = ctx;
    DCHECK_EQ(req->type, RpcType::kExtendReq);

    bool lease_valid = true;
    bool dbg_ext_do_nothing =
        req->flag & (flag_t) LeaseModifyFlag::kDebugExtendDoNothing;

    if (likely(!dbg_ext_do_nothing))
    {
        auto lease_id = req->lease_id;
        auto *lease_ctx = get_lease_context(lease_id);
        if (unlikely(lease_ctx == nullptr))
        {
            lease_valid = false;
        }
        else
        {
            if (unlikely(!lease_ctx->client_cid.is_same(req->cid)))
            {
                lease_valid = false;
            }
        }

        if (lease_valid)
        {
            bool with_pr = lease_ctx->with_pr;
            CHECK(with_pr) << "** extend a lease without pr.";
            auto protection_region_id = lease_ctx->protection_region_id;
            auto *protection_region =
                get_protection_region(protection_region_id);
            CHECK(protection_region)
                << "** get invalid protection region from valid lease_ctx.";

            auto ns_per_unit = protection_region->ns_per_unit;
            auto extend_unit_nr = (req->ns + ns_per_unit - 1) / ns_per_unit;
            auto old_val = protection_region->aba_unit_nr_to_ddl.load(
                std::memory_order_relaxed);
            old_val.u32_2 += extend_unit_nr;
            protection_region->aba_unit_nr_to_ddl.store(
                old_val, std::memory_order_relaxed);
        }
    }

    auto *resp_buf = get_rdma_message_buffer();
    auto &resp_msg = *(LeaseModifyResponse *) resp_buf;
    resp_msg.type = RpcType::kExtendResp;
    resp_msg.cid = req->cid;
    resp_msg.cid.node_id = get_node_id();
    resp_msg.cid.thread_id = get_thread_id();
    resp_msg.success = lease_valid;

    if constexpr (debug())
    {
        resp_msg.digest = 0;
        resp_msg.digest = djb2_digest(&resp_msg, sizeof(LeaseModifyResponse));
    }

    dsm_->unreliable_send((const char *) resp_buf,
                          sizeof(LeaseModifyResponse),
                          req->cid.node_id,
                          req->cid.thread_id);
    put_rdma_message_buffer(resp_buf);
}

void Patronus::post_handle_request_memory_access(MemoryRequest *req,
                                                 CoroContext *ctx)
{
    std::ignore = ctx;

    auto *resp_buf = get_rdma_message_buffer();
    auto &resp_msg = *(MemoryResponse *) resp_buf;
    resp_msg.type = RpcType::kMemoryResp;
    resp_msg.cid = req->cid;
    resp_msg.cid.node_id = get_node_id();
    resp_msg.cid.thread_id = get_thread_id();
    resp_msg.success = true;
    resp_msg.size = req->size;
    resp_msg.flag = req->flag;

    CHECK(resp_msg.validate())
        << "** If request is valid, the response must be valid";

    auto mf = (MemoryRequestFlag) req->flag;
    // remote_address is indexed to this buffer
    auto dsm = get_dsm();
    char *addr = (char *) dsm->get_base_addr() + dsm->dsm_reserve_size() +
                 req->remote_addr;
    if (mf == MemoryRequestFlag::kRead)
    {
        // memcpy
        DCHECK_GE(resp_msg.buffer_capacity(), req->size);
        memcpy(resp_msg.buffer, addr, req->size);
        DVLOG(4) << "[patronus][rpc-mem]  handling rpc memory read at "
                 << (void *) addr << " with size " << (size_t) req->size
                 << ". dsm_base: " << (void *) dsm->get_base_addr()
                 << ", reserve_size: " << (void *) dsm->dsm_reserve_size()
                 << ", remote_addr: " << (void *) req->remote_addr << " from "
                 << req->cid;
    }
    else if (mf == MemoryRequestFlag::kWrite)
    {
        // memcpy
        DCHECK_GE(req->buffer_capacity(), req->size);
        memcpy(addr, req->buffer, req->size);
        DVLOG(4) << "[patronus][rpc-mem] handling rpc memory write at "
                 << (void *) addr << " with size " << (size_t) req->size
                 << ". dsm_base: " << (void *) dsm->get_base_addr()
                 << ", reserve_size: " << (void *) dsm->dsm_reserve_size()
                 << ", remote_addr: " << (void *) req->remote_addr << " from "
                 << req->cid;
    }
    else
    {
        CHECK_EQ(mf, MemoryRequestFlag::kCAS);
        auto *patomic = (std::atomic<uint64_t> *) addr;
        DCHECK_EQ(req->size, 2 * sizeof(uint64_t));
        uint64_t compare = *((uint64_t *) req->buffer);
        // uint64_t remember_compare = compare;
        uint64_t swap = *(((uint64_t *) req->buffer) + 1);
        bool succ = patomic->compare_exchange_strong(
            compare, swap, std::memory_order_relaxed);
        resp_msg.success = succ;
        resp_msg.size = sizeof(uint64_t);
        memcpy(resp_msg.buffer, &compare, sizeof(compare));
        DVLOG(4) << "[patronus][rpc-mem] handling rpc memory CAS at "
                 << (void *) addr << " with size " << (size_t) req->size
                 << ". dsm_base: " << (void *) dsm->get_base_addr()
                 << ", reserve_size: " << (void *) dsm->dsm_reserve_size()
                 << ", remote_addr: " << (void *) req->remote_addr << " from "
                 << req->cid << ". compare: " << compare << ", swap: " << swap;
    }

    if constexpr (debug())
    {
        resp_msg.digest = 0;
        resp_msg.digest = djb2_digest(&resp_msg, resp_msg.msg_size());
    }
    dsm_->unreliable_send((char *) resp_buf,
                          resp_msg.msg_size(),
                          req->cid.node_id,
                          req->cid.thread_id);

    put_rdma_message_buffer(resp_buf);
}

void Patronus::post_handle_request_lease_relinquish(LeaseModifyRequest *req,
                                                    HandleReqContext &req_ctx,
                                                    CoroContext *ctx)
{
    std::ignore = ctx;

    auto *resp_buf = get_rdma_message_buffer();
    auto &resp_msg = *(LeaseModifyResponse *) resp_buf;
    resp_msg.type = RpcType::kRelinquishResp;
    resp_msg.cid = req->cid;
    resp_msg.cid.node_id = get_node_id();
    resp_msg.cid.thread_id = get_thread_id();
    resp_msg.success = true;
    if constexpr (debug())
    {
        resp_msg.digest = 0;
        resp_msg.digest = djb2_digest(&resp_msg, sizeof(LeaseModifyResponse));
    }
    dsm_->unreliable_send((char *) resp_buf,
                          sizeof(LeaseModifyResponse),
                          req->cid.node_id,
                          req->cid.thread_id);
    put_rdma_message_buffer(resp_buf);

    if (req_ctx.relinquish.do_nothing)
    {
        return;
    }

    LeaseContext *lease_ctx = req_ctx.relinquish.lease_ctx;
    bool with_dealloc = req_ctx.relinquish.with_dealloc;
    bool with_pr = req_ctx.relinquish.with_pr;
    ProtectionRegion *protection_region = req_ctx.relinquish.protection_region;
    bool with_conflict_detect = req_ctx.relinquish.with_conflict_detect;
    uint64_t key_bucket_id = req_ctx.relinquish.key_bucket_id;
    uint64_t key_slot_id = req_ctx.relinquish.key_slot_id;
    uint64_t addr_to_bind = req_ctx.relinquish.addr_to_bind;
    size_t buffer_size = req_ctx.relinquish.buffer_size;
    uint64_t hint = req_ctx.relinquish.hint;

    if (with_conflict_detect)
    {
        auto bucket_id = key_bucket_id;
        auto slot_id = key_slot_id;
        lock_manager_.unlock(bucket_id, slot_id);
    }

    if (with_dealloc)
    {
        patronus_free((void *) addr_to_bind, buffer_size, hint);
    }

    // TODO(patronus): not considering any checks here
    // for example, the pr->meta.relinquished bits.
    if (with_pr)
    {
        put_protection_region(DCHECK_NOTNULL(protection_region));
    }

    put_lease_context(lease_ctx);
}

void Patronus::prepare_gc_lease(uint64_t lease_id,
                                HandleReqContext &req_ctx,
                                ClientID cid,
                                flag_t flag,
                                CoroContext *ctx)
{
    debug_validate_lease_modify_flag(flag);
    req_ctx.status = AcquireRequestStatus::kSuccess;

    auto &ex_ctx = coro_ex_ctx(ctx->coro_id());

    DVLOG(4) << "[patronus][gc_lease] task_gc_lease for lease_id " << lease_id
             << ", expect cid " << cid << ", coro: " << pre_coro_ctx(ctx)
             << ", at patronus time: " << time_syncer_->patronus_now();

    bool with_dealloc = flag & (flag_t) LeaseModifyFlag::kWithDeallocation;
    bool only_dealloc = flag & (flag_t) LeaseModifyFlag::kOnlyDeallocation;
    bool no_unbind_any =
        flag & (flag_t) LeaseModifyFlag::kNoRelinquishUnbindAny;
    bool no_unbind_pr = flag & (flag_t) LeaseModifyFlag::kNoRelinquishUnbindPr;
    DCHECK(!only_dealloc) << "This case should have been handled. "
                             "task_gc_lease never handle this";
    bool use_mr = flag & (flag_t) LeaseModifyFlag::kUseMR;

    auto *lease_ctx = get_lease_context(lease_id);
    req_ctx.relinquish.lease_ctx = lease_ctx;
    req_ctx.relinquish.with_dealloc = with_dealloc;

    req_ctx.relinquish.do_nothing = true;
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
    req_ctx.relinquish.do_nothing = false;

    bool with_pr = lease_ctx->with_pr;
    bool with_buf = lease_ctx->with_buf;
    bool unbind_pr = with_pr;
    bool unbind_buf = with_buf;

    if (no_unbind_pr)
    {
        DCHECK(!no_unbind_any);
        unbind_pr = false;
    }
    if (no_unbind_any)
    {
        DCHECK(!no_unbind_pr);
        unbind_buf = false;
        unbind_pr = false;
    }

    uint64_t protection_region_id = 0;
    ProtectionRegion *protection_region = nullptr;
    if (likely(with_pr))
    {
        protection_region_id = lease_ctx->protection_region_id;
        protection_region = get_protection_region(protection_region_id);
    }

    // okay:
    // when we reach here, we definitely should do the GC
    // nothing stops us
    // NOTE: set valid = false here
    // otherwise, concurrent relinquish will gc twice.
    DCHECK_NOTNULL(lease_ctx)->valid = false;

    // after that, getting this lease_ctx is not possible
    // so, store what we need at req_ctx.relinquish.
    if (only_dealloc)
    {
        unbind_pr = false;
        unbind_buf = false;
    }

    if (likely(unbind_pr || unbind_buf))
    {
        if (use_mr)
        {
            auto *buffer_mr = DCHECK_NOTNULL(lease_ctx)->buffer_mr;
            auto *header_mr = DCHECK_NOTNULL(lease_ctx)->header_mr;
            if (buffer_mr != nullptr && unbind_buf)
            {
                CHECK(destroyMemoryRegion(buffer_mr));
            }
            if (header_mr != nullptr && unbind_pr)
            {
                CHECK(destroyMemoryRegion(header_mr));
            }
        }
        else
        {
            // should issue unbind MW here.
            auto dir_id = lease_ctx->dir_id;
            DCHECK_EQ(dir_id, get_thread_id())
                << "Currently we limit each server thread tid handles the "
                   "same "
                   "dir_id";
            auto *mr = dsm_->get_dir_mr(dir_id);
            void *bind_nulladdr = dsm_->dsm_offset_to_addr(0);

            // NOTE: if size == 0, no matter access set to RO, RW or NORW
            // and no matter allocated_mw_nr +2/-2/0, it generates corrupted
            // mws. But if set size to 1 and allocated_mw_nr + 2, it works
            // well.
            if (unbind_buf)
            {
                ex_ctx.prepare_unbind_mw(DCHECK_NOTNULL(lease_ctx->buffer_mw),
                                         mr,
                                         (uint64_t) bind_nulladdr,
                                         1,
                                         IBV_ACCESS_CUSTOM_REMOTE_NORW);
            }
            else
            {
                if (unlikely(with_buf))
                {
                    ex_ctx.put_mw(DCHECK_NOTNULL(lease_ctx->buffer_mw));
                }
            }
            lease_ctx->buffer_mw = nullptr;
            if (unbind_pr)
            {
                ex_ctx.prepare_unbind_mw(DCHECK_NOTNULL(lease_ctx->header_mw),
                                         mr,
                                         (uint64_t) bind_nulladdr,
                                         1,
                                         IBV_ACCESS_CUSTOM_REMOTE_NORW);
            }
            else
            {
                if (unlikely(with_pr))
                {
                    ex_ctx.put_mw(DCHECK_NOTNULL(lease_ctx->header_mw));
                }
            }
            lease_ctx->header_mw = nullptr;
        }
    }

    req_ctx.relinquish.lease_ctx = lease_ctx;
    req_ctx.relinquish.with_dealloc = with_dealloc;
    req_ctx.relinquish.with_pr = with_pr;
    req_ctx.relinquish.protection_region_id = protection_region_id;
    req_ctx.relinquish.protection_region = protection_region;
    req_ctx.relinquish.with_conflict_detect =
        DCHECK_NOTNULL(lease_ctx)->with_conflict_detect;
    req_ctx.relinquish.key_bucket_id = lease_ctx->key_bucket_id;
    req_ctx.relinquish.key_slot_id = lease_ctx->key_slot_id;
    req_ctx.relinquish.addr_to_bind = lease_ctx->addr_to_bind;
    req_ctx.relinquish.buffer_size = lease_ctx->buffer_size;
    req_ctx.relinquish.hint = lease_ctx->hint;
    req_ctx.relinquish.dir_id = lease_ctx->dir_id;
}

void Patronus::task_gc_lease(uint64_t lease_id,
                             ClientID cid,
                             compound_uint64_t expect_aba_unit_nr_to_ddl,
                             flag_t flag /* LeaseModifyFlag */,
                             CoroContext *ctx)
{
    debug_validate_lease_modify_flag(flag);

    DVLOG(4) << "[patronus][gc_lease] task_gc_lease for lease_id " << lease_id
             << ", expect cid " << cid << ", coro: " << pre_coro_ctx(ctx)
             << ", at patronus time: " << time_syncer_->patronus_now();

    auto &ex_ctx = coro_ex_ctx(ctx->coro_id());

    bool with_dealloc = flag & (flag_t) LeaseModifyFlag::kWithDeallocation;
    bool only_dealloc = flag & (flag_t) LeaseModifyFlag::kOnlyDeallocation;
    bool no_unbind_any =
        flag & (flag_t) LeaseModifyFlag::kNoRelinquishUnbindAny;
    bool no_unbind_pr = flag & (flag_t) LeaseModifyFlag::kNoRelinquishUnbindPr;
    DCHECK(!only_dealloc) << "This case should have been handled. "
                             "task_gc_lease never handle this";
    bool use_mr = flag & (flag_t) LeaseModifyFlag::kUseMR;

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
    bool unbind_pr = with_pr;
    bool unbind_buf = with_buf;

    if (no_unbind_any)
    {
        unbind_pr = false;
        unbind_buf = false;
    }
    if (no_unbind_pr)
    {
        unbind_pr = false;
    }

    uint64_t protection_region_id = 0;
    ProtectionRegion *protection_region = nullptr;

    if (likely(with_pr))
    {
        // test if client issued extends
        protection_region_id = lease_ctx->protection_region_id;
        protection_region = get_protection_region(protection_region_id);
        DCHECK(protection_region->valid)
            << "** If lease_ctx is valid, the protection region must also "
               "be "
               "valid. protection_region_id: "
            << protection_region_id << ", PR: " << *protection_region
            << ", lease_id: " << lease_id << ", cid: " << cid
            << ", expect_aba_unit_nr_to_ddl: " << expect_aba_unit_nr_to_ddl
            << ", flag: " << LeaseModifyFlagOut(flag)
            << ", coro: " << pre_coro_ctx(ctx) << ", protection_region at "
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
        bool force_gc = flag & (flag_t) LeaseModifyFlag::kForceUnbind;
        bool client_already_exteded = unit_nr_to_ddl != expect_unit_nr_to_ddl;
        DVLOG(4) << "[patronus][gc_lease] determine the behaviour : force_gc : "
                 << force_gc
                 << ", client_already_extended: " << client_already_exteded
                 << " (pr->aba_unit_nr_to_ddl: " << aba_unit_nr_to_ddl
                 << " v.s. expect_aba_unit_nr_to_ddl: "
                 << expect_aba_unit_nr_to_ddl;

        if constexpr (debug())
        {
            if (!force_gc)
            {
                CHECK_EQ(aba, expect_aba)
                    << "** The aba_id should remain the same. actual: "
                    << aba_unit_nr_to_ddl
                    << ", expect: " << expect_aba_unit_nr_to_ddl
                    << ". pr: " << (void *) protection_region
                    << ", id: " << protection_region_id
                    << ", lease_id: " << lease_id;
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

            ddl_manager_.push(
                next_ddl,
                [this, lease_id, cid, flag, next_expect_aba_unit_nr_to_ddl](
                    CoroContext *ctx) {
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

        // NOTE: only add aba by one when the PR is gc-ed
        // so, add the aba at put_protection_region() call.
        // NOTE2: aba is deprecated: no need
    }

    // okay:
    // when we reach here, we definitely should do the GC
    // nothing stops us
    // NOTE: set valid = false here
    // otherwise, concurrent relinquish will gc twice.
    DCHECK_NOTNULL(lease_ctx)->valid = false;

    if (only_dealloc)
    {
        unbind_buf = false;
        unbind_pr = false;
    }
    if (likely(unbind_buf || unbind_pr))
    {
        if (use_mr)
        {
            auto *buffer_mr = DCHECK_NOTNULL(lease_ctx)->buffer_mr;
            auto *header_mr = DCHECK_NOTNULL(lease_ctx)->header_mr;
            if (buffer_mr != nullptr && unbind_buf)
            {
                CHECK(destroyMemoryRegion(buffer_mr));
            }
            if (header_mr != nullptr && unbind_pr)
            {
                CHECK(destroyMemoryRegion(header_mr));
            }
        }
        else
        {
            // should issue unbind MW here.
            // static thread_local ibv_send_wr wrs[8];
            auto dir_id = lease_ctx->dir_id;
            auto *mr = dsm_->get_dir_mr(dir_id);
            void *bind_nulladdr = dsm_->dsm_offset_to_addr(0);

            // NOTE: if size == 0, no matter access set to RO, RW or NORW
            // and no matter allocated_mw_nr +2/-2/0, it generates corrupted
            // mws. But if set size to 1 and allocated_mw_nr + 2, it works
            // well.
            if (unbind_buf)
            {
                ex_ctx.prepare_unbind_mw(DCHECK_NOTNULL(lease_ctx->buffer_mw),
                                         mr,
                                         (uint64_t) bind_nulladdr,
                                         1,
                                         IBV_ACCESS_CUSTOM_REMOTE_NORW);
            }
            else
            {
                if (unlikely(with_buf))
                {
                    ex_ctx.put_mw(DCHECK_NOTNULL(lease_ctx->buffer_mw));
                }
            }
            if (unbind_pr)
            {
                ex_ctx.prepare_unbind_mw(DCHECK_NOTNULL(lease_ctx->header_mw),
                                         mr,
                                         (uint64_t) bind_nulladdr,
                                         1,
                                         IBV_ACCESS_CUSTOM_REMOTE_NORW);
            }
            else
            {
                if (unlikely(with_pr))
                {
                    ex_ctx.put_mw(DCHECK_NOTNULL(lease_ctx->header_mw));
                }
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

    // NOTE: don't call put_mw(...) here
    // because ex_ctx manages the ownership of mw.
    if (lease_ctx->header_mw)
    {
        lease_ctx->header_mw = nullptr;
    }
    if (lease_ctx->buffer_mw)
    {
        lease_ctx->buffer_mw = nullptr;
    }

    if (with_pr)
    {
        put_protection_region(protection_region);
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

    // uint64_t min = 0;
    // uint64_t max = config::umsg::kRecvLimit;
    // uint64_t rng = 1;
    // OnePassBucketMonitor<double> per_qp_batch_m(min, max, rng);

    auto &ex_ctx = coro_ex_ctx(coro_id);
    while (likely(!should_exit(wait_key) || !task_queue.empty()))
    {
        DCHECK(!task_queue.empty());
        auto task = task_queue.front();
        task->active_coro_nr++;
        DCHECK_GT(task->msg_nr, 0);
        size_t remain_task_nr = task->msg_nr - task->fetched_nr;
        DCHECK_GE(remain_task_nr, 0);
        size_t acquire_task = std::min(
            remain_task_nr, config::patronus::kHandleRequestBatchLimit);
        auto *msg_buf = task->buf + kMessageSize * task->fetched_nr;

        task->fetched_nr += acquire_task;
        DCHECK_LE(task->fetched_nr, task->msg_nr);

        DVLOG(4) << "[patronus] server acquiring task @" << (void *) task
                 << ": " << *task << ", handle " << acquire_task
                 << ". coro: " << ctx;

        if (task->fetched_nr == task->msg_nr)
        {
            // pop the task from the queue
            // so that the following coroutine will not be able to access this
            // task however, the task is still ACTIVE, becase
            // @task->active_coro_nr coroutines are still sharing the task
            task_queue.pop();
        }

        ex_ctx.clear();

        DVLOG(4) << "[patronus] server handling task @" << (void *) task
                 << ", message_nr " << task->msg_nr << ", handle "
                 << acquire_task << ". coro: " << ctx;
        // prepare never switches
        prepare_handle_request_messages(msg_buf, acquire_task, ex_ctx, &ctx);
        // NOTE: worker coroutine poll tasks here
        // NOTE: we combine wrs, so put do_task here.
        DCHECK_GE(2 * ex_ctx.remain_size(), ex_ctx.max_wr_size())
            << "** By design, we give ex_ctx the double capacity to handle 1) "
               "requests and 2) auto-relinquish. Therefore, ex_ctx should be "
               "at most half-full";
        if (!ddl_manager_.empty() && ex_ctx.remain_size() > 0)
        {
            ddl_manager_.do_task(time_syncer_->patronus_now().term(),
                                 &ctx,
                                 // one task may have two wr at most
                                 ex_ctx.remain_size() / 2);
        }
        commit_handle_request_messages(ex_ctx, &ctx);
        post_handle_request_messages(msg_buf, acquire_task, ex_ctx, &ctx);

        // debug_analysis_per_qp_batch(msg_buf, acquire_task, per_qp_batch_m);

        task->active_coro_nr--;

        if (task->active_coro_nr == 0 && (task->fetched_nr == task->msg_nr))
        {
            DVLOG(4) << "[patronus] server handling callback of task @"
                     << (void *) task << ": " << *task << ". coro: " << ctx;
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

void Patronus::commit_handle_request_messages(
    ServerCoroBatchExecutionContext &ex_ctx, CoroContext *ctx)
{
    auto *rw_ctx = DCHECK_NOTNULL(get_rw_context());
    uint64_t rw_ctx_id = rw_context_.obj_to_id(rw_ctx);
    rw_ctx->coro_id = ctx ? ctx->coro_id() : kNotACoro;
    rw_ctx->dir_id = get_thread_id();
    rw_ctx->ready = false;
    rw_ctx->wc_status = IBV_WC_SUCCESS;

    bool has_work = ex_ctx.commit(WRID_PREFIX_PATRONUS_BATCH, rw_ctx_id);
    if (has_work)
    {
        ctx->yield_to_master();
        DCHECK(rw_ctx->ready);
    }

    put_rw_context(rw_ctx);
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
                                flag_t flag,
                                CoroContext *ctx)
{
    auto ec = handle_batch_op_flag(flag);
    if (unlikely(ec != RetCode::kOk))
    {
        return ec;
    }
    CHECK(lease.success());
    CHECK(lease.is_writable());

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
                               flag_t flag,
                               CoroContext *ctx)
{
    auto ec = handle_batch_op_flag(flag);
    if (unlikely(ec != RetCode::kOk))
    {
        return ec;
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
                              flag_t flag,
                              CoroContext *ctx)
{
    auto ec = handle_batch_op_flag(flag);
    if (unlikely(ec != RetCode::kOk))
    {
        return ec;
    }
    CHECK(lease.success());
    CHECK(lease.is_writable());

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

RetCode Patronus::handle_batch_op_flag(flag_t flag) const
{
    if constexpr (debug())
    {
        bool no_local_check = flag & (flag_t) RWFlag::kNoLocalExpireCheck;
        CHECK(no_local_check) << "** Batch op not support local expire checks";
        bool with_auto_expend = flag & (flag_t) RWFlag::kWithAutoExtend;
        CHECK(!with_auto_expend)
            << "** Batch op does not support auto lease extend";
        bool with_cache = flag & (flag_t) RWFlag::kWithCache;
        CHECK(!with_cache) << "** Batch op does not support local caching";
        bool reserved = flag & (flag_t) RWFlag::kReserved;
        CHECK(!reserved);
    }
    return kOk;
}

RetCode Patronus::commit(PatronusBatchContext &batch, CoroContext *ctx)
{
    RetCode rc = kOk;
    RWContext *rw_context = nullptr;
    uint16_t rw_ctx_id = 0;
    auto wc_status = IBV_WC_SUCCESS;
    bool ret_success = true;

    if (unlikely(batch.empty()))
    {
        return kOk;
    }
    rw_context = DCHECK_NOTNULL(get_rw_context());
    rw_ctx_id = get_rw_context_id(rw_context);
    rw_context->wc_status = IBV_WC_SUCCESS;
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
        << "** When commit finished, should have been ready. rw_ctx at "
        << (void *) rw_context << ", ready at " << (void *) &rw_context->ready
        << ", rw_ctx_id: " << rw_ctx_id << ", expect wrid: " << wr_id
        << ", coro: " << pre_coro_ctx(ctx);

    wc_status = rw_context->wc_status;
    ret_success = (wc_status == IBV_WC_SUCCESS);

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

void Patronus::hack_trigger_rdma_protection_error(size_t node_id,
                                                  size_t dir_id,
                                                  CoroContext *ctx)
{
    size_t size = 1;
    auto rdma_buf = get_rdma_buffer(size);
    auto rc = read_write_impl(rdma_buf.buffer,
                              size,
                              node_id,
                              dir_id,
                              0 /* rkey */,
                              GlobalAddress(0).val,
                              true /* is_read */,
                              WRID_PREFIX_PATRONUS_GENERATE_FAULT,
                              ctx);
    CHECK_EQ(rc, kRdmaProtectionErr);
    // NOTE:
    // do not put rdma buffer from previous thread desc.
    // put_rdma_buffer(rdma_buf);
}

void Patronus::prepare_fast_backup_recovery(size_t prepare_nr)
{
    std::lock_guard<std::mutex> lk(fast_backup_descs_mu_);
    for (size_t i = 0; i < prepare_nr; ++i)
    {
        prepared_fast_backup_descs_.push(
            prepare_client_thread(false /* is_registering */));
    }
}

PatronusThreadResourceDesc Patronus::prepare_client_thread(
    bool is_registering_thread)
{
    PatronusThreadResourceDesc desc;
    if (unlikely(is_registering_thread && dsm_->hasRegistered()))
    {
        desc.dsm_desc = dsm_->getCurrentThreadDesc();
    }
    else
    {
        desc.dsm_desc = dsm_->prepareThread();
    }

    auto *dsm_rdma_buffer = desc.dsm_desc.rdma_buffer;

    size_t message_pool_size = 4 * define::MB;
    CHECK_GT(desc.dsm_desc.rdma_buffer_size, message_pool_size);
    CHECK_GE(message_pool_size / kMessageSize, 65536)
        << "Consider to tune up message pool size? Less than 64436 "
           "possible messages";

    desc.rdma_message_buffer_pool =
        std::make_unique<ThreadUnsafeBufferPool<kMessageSize>>(
            dsm_rdma_buffer, message_pool_size);

    // the remaining buffer is given to client rdma buffers
    // 8B and non-8B
    auto *client_rdma_buffer = dsm_rdma_buffer + message_pool_size;
    size_t rdma_buffer_size =
        desc.dsm_desc.rdma_buffer_size - message_pool_size;
    CHECK_GE(rdma_buffer_size, kMaxCoroNr * kClientRdmaBufferSize)
        << "rdma_buffer not enough for maximum coroutine";

    // remember that, so that we could validate when debugging
    desc.client_rdma_buffer = client_rdma_buffer;
    desc.client_rdma_buffer_size = rdma_buffer_size;

    auto rdma_buffer_size_8B = 8 * 1024;
    CHECK_GE(rdma_buffer_size, rdma_buffer_size_8B);
    auto rdma_buffer_size_non_8B = rdma_buffer_size - rdma_buffer_size_8B;
    auto *client_rdma_buffer_8B = client_rdma_buffer;
    auto *client_rdma_buffer_non_8B = client_rdma_buffer + rdma_buffer_size_8B;

    desc.rdma_client_buffer_8B = std::make_unique<ThreadUnsafeBufferPool<8>>(
        client_rdma_buffer_8B, rdma_buffer_size_8B);
    desc.rdma_client_buffer =
        std::make_unique<ThreadUnsafeBufferPool<kClientRdmaBufferSize>>(
            client_rdma_buffer_non_8B, rdma_buffer_size_non_8B);
    return desc;
}

bool Patronus::has_registered() const
{
    return has_registered_;
}

bool Patronus::apply_client_resource(PatronusThreadResourceDesc &&desc,
                                     bool bind_core,
                                     TraceView v)
{
    auto succ = dsm_->applyResource(desc.dsm_desc, bind_core);
    CHECK(succ);
    rdma_message_buffer_pool_ = std::move(desc.rdma_message_buffer_pool);
    client_rdma_buffer_ = desc.client_rdma_buffer;
    client_rdma_buffer_size_ = desc.client_rdma_buffer_size;
    rdma_client_buffer_8B_ = std::move(desc.rdma_client_buffer_8B);
    rdma_client_buffer_ = std::move(desc.rdma_client_buffer);

    is_client_ = true;
    v.pin("switch-thread-desc");
    return true;
}

bool Patronus::fast_switch_backup_qp(TraceView v)
{
    std::lock_guard<std::mutex> lk(fast_backup_descs_mu_);
    if (unlikely(prepared_fast_backup_descs_.empty()))
    {
        return false;
    }
    auto desc = std::move(prepared_fast_backup_descs_.front());
    prepared_fast_backup_descs_.pop();
    return apply_client_resource(std::move(desc), false /* bind core */, v);
}
void Patronus::set_configure_reuse_mw_opt(bool val)
{
    for (auto &ex_ctx : coro_batch_ex_ctx_)
    {
        ex_ctx.set_configure_reuse_mw_opt(val);
    }
}
bool Patronus::get_configure_reuse_mw_opt()
{
    bool conf = coro_batch_ex_ctx_.front().get_configure_reuse_mw_opt();
    if constexpr (debug())
    {
        for (const auto &ex_ctx : coro_batch_ex_ctx_)
        {
            CHECK_EQ(conf, ex_ctx.get_configure_reuse_mw_opt());
        }
    }
    return conf;
}

}  // namespace patronus