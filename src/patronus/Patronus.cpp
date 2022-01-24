#include "patronus/Patronus.h"

#include "patronus/IBOut.h"
#include "util/Debug.h"

namespace patronus
{
thread_local std::unique_ptr<ThreadUnsafeBufferPool<Patronus::kMessageSize>>
    Patronus::rdma_message_buffer_pool_;
thread_local std::unique_ptr<
    ThreadUnsafeBufferPool<Patronus::kClientRdmaBufferSize>>
    Patronus::rdma_client_buffer_;
thread_local ThreadUnsafePool<RpcContext, Patronus::kMaxCoroNr>
    Patronus::rpc_context_;
thread_local ThreadUnsafePool<RWContext, Patronus::kMaxCoroNr>
    Patronus::rw_context_;
thread_local std::queue<ibv_mw *> Patronus::mw_pool_[NR_DIRECTORY];
thread_local ThreadUnsafePool<LeaseContext, Patronus::kLeaseContextNr>
    Patronus::lease_context_;
thread_local ServerCoroContext Patronus::server_coro_ctx_;
thread_local std::unique_ptr<ThreadUnsafeBufferPool<sizeof(ProtectionRegion)>>
    Patronus::protection_region_pool_;
thread_local DDLManager Patronus::ddl_manager_;
thread_local size_t Patronus::allocated_mw_nr_;

Patronus::Patronus(const PatronusConfig &conf)
{
    DSMConfig dsm_config;
    dsm_config.machineNR = conf.machine_nr;
    dsm_config.dsmReserveSize = required_dsm_reserve_size();
    dsm_config.dsmSize = conf.buffer_size;
    dsm_ = DSM::getInstance(dsm_config);

    // validate dsm
    auto internal_buf = dsm_->get_server_buffer();
    auto reserve_buf = dsm_->get_server_reserved_buffer();
    CHECK_GE(internal_buf.size, conf.buffer_size)
        << "** dsm should allocate DSM buffer at least what Patronus requires";
    CHECK_GE(reserve_buf.size, required_dsm_reserve_size())
        << "**dsm should provide reserved buffer at least what Patronus "
           "requries";

    // for time syncer
    auto time_sync_buffer = get_time_sync_buffer();
    auto time_sync_offset = dsm_->addr_to_dsm_offset(time_sync_buffer.buffer);
    GlobalAddress gaddr;
    gaddr.nodeID = conf.time_parent_node_id;
    gaddr.offset = time_sync_offset;
    time_syncer_ = time::TimeSyncer::new_instance(
        dsm_, gaddr, time_sync_buffer.buffer, time_sync_buffer.size);

    time_syncer_->sync();

    reg_locator(conf.key_locator);
    explain(conf);
}
Patronus::~Patronus()
{
    for (ibv_mw *mw : allocated_mws_)
    {
        dsm_->free_mw(mw);
    }
}

void Patronus::explain(const PatronusConfig &conf)
{
    LOG(INFO) << "[patronus] config: " << conf;
}

Lease Patronus::get_lease_impl(uint16_t node_id,
                               uint16_t dir_id,
                               id_t key,
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

    // TODO(patronus): see if this tid to mid binding is correct.
    auto tid = get_thread_id();
    auto mid = dir_id;

    char *rdma_buf = get_rdma_message_buffer();
    auto *rpc_context = get_rpc_context();
    uint16_t rpc_ctx_id = get_rpc_context_id(rpc_context);
    // DVLOG(3) << "[debug] allocating rpc_context " << (void *) rpc_context
    //          << " at id " << rpc_ctx_id;

    Lease ret_lease;
    ret_lease.node_id_ = node_id;

    auto *msg = (AcquireRequest *) rdma_buf;
    msg->type = type;
    msg->cid.node_id = get_node_id();
    msg->cid.thread_id = tid;
    msg->cid.mid = mid;
    msg->cid.coro_id = ctx ? ctx->coro_id() : kNotACoro;
    msg->cid.rpc_ctx_id = rpc_ctx_id;
    msg->dir_id = dir_id;
    msg->key = key;
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

    dsm_->reliable_send(rdma_buf, sizeof(AcquireRequest), node_id, mid);

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

    return ret_lease;
}

bool Patronus::read_write_impl(char *iobuf,
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
    return ret;
}

bool Patronus::buffer_rw_impl(Lease &lease,
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

bool Patronus::protection_region_rw_impl(Lease &lease,
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

Lease Patronus::lease_modify_impl(Lease &lease,
                                  RequestType type,
                                  time::ns_t ns,
                                  uint8_t flag,
                                  CoroContext *ctx)
{
    CHECK(type == RequestType::kExtend || type == RequestType::kRelinquish ||
          type == RequestType::kUpgrade)
        << "** invalid type " << (int) type;

    // TODO(patronus): see if this mid to dir_id binding is correct
    auto target_node_id = lease.node_id_;
    auto dir_id = lease.dir_id();
    auto mid = dir_id;
    auto tid = get_thread_id();

    char *rdma_buf = get_rdma_message_buffer();
    auto *rpc_context = get_rpc_context();
    uint16_t rpc_ctx_id = get_rpc_context_id(rpc_context);

    Lease ret_lease;
    ret_lease.node_id_ = target_node_id;

    auto *msg = (LeaseModifyRequest *) rdma_buf;
    msg->type = type;
    msg->cid.node_id = get_node_id();
    msg->cid.thread_id = tid;
    msg->cid.mid = mid;
    msg->cid.coro_id = ctx ? ctx->coro_id() : kNotACoro;
    msg->cid.rpc_ctx_id = rpc_ctx_id;
    msg->lease_id = lease.id();
    msg->ns = ns;
    msg->flag = flag;

    rpc_context->ret_lease = &ret_lease;
    rpc_context->ready = false;
    rpc_context->request = (BaseMessage *) msg;

    if constexpr (debug())
    {
        msg->digest = 0;
        msg->digest = djb2_digest(msg, sizeof(LeaseModifyRequest));
    }

    dsm_->reliable_send(
        rdma_buf, sizeof(LeaseModifyRequest), target_node_id, mid);

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
        ret_lease.set_error();  // actually no return. so never use me
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
            DVLOG(4) << "[patronus] handling acquire response. " << *msg;
            handle_response_acquire(msg);
            break;
        }
        case RequestType::kUpgrade:
        case RequestType::kExtend:
        case RequestType::kRelinquish:
        {
            auto *msg = (LeaseModifyResponse *) base;
            DVLOG(4) << "[patronus] handling lease modify response " << *msg;
            handle_response_lease_modify(msg);
            break;
        }
        case RequestType::kAdmin:
        {
            auto *msg = (AdminRequest *) base;
            DVLOG(4) << "[patronus] handling admin request " << *msg;
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
    DVLOG(5) << "[debug] getting rpc_context " << (void *) rpc_context
             << " at id " << rpc_ctx_id;
    auto *request = (AcquireRequest *) rpc_context->request;
    if constexpr (debug())
    {
        uint64_t request_digest = request->digest.get();
        request->digest = 0;
        uint64_t resp_digest = resp->digest.get();
        resp->digest = 0;
        DCHECK_EQ(request_digest, djb2_digest(request, sizeof(AcquireRequest)));
        DCHECK_EQ(resp_digest, djb2_digest(resp, sizeof(AcquireResponse)));
    }

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
        ret_lease.rkey_0_ = resp->rkey_0;
        ret_lease.header_rkey_ = resp->rkey_header;
        ret_lease.cur_rkey_ = ret_lease.rkey_0_;
        ret_lease.cur_ddl_term_ = time::PatronusTime(resp->ddl_term);
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
        ret_lease.set_error();
    }

    rpc_context->ready.store(true, std::memory_order_release);
}

void Patronus::handle_request_acquire(AcquireRequest *req, CoroContext *ctx)
{
    DCHECK_NE(req->type, RequestType::kAcquireNoLease) << "** Deprecated";

    auto dirID = req->dir_id;
    auto *buffer_mw = get_mw(dirID);
    auto *header_mw = get_mw(dirID);

    // auto internal = dsm_->get_server_buffer();
    auto *protection_region = get_protection_region();
    auto protection_region_id =
        protection_region_pool_->buf_to_id(protection_region);

    if constexpr (debug())
    {
        uint64_t digest = req->digest.get();
        req->digest = 0;
        DCHECK_EQ(digest, djb2_digest(req, sizeof(AcquireRequest)))
            << "** digest mismatch for req " << *req;
    }

    AcquireRequestStatus status = AcquireRequestStatus::kSuccess;

    size_t object_buffer_offset = 0;
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

    bool with_lock =
        req->flag & (uint8_t) AcquireRequestFlag::kWithConflictDetect;
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

    object_buffer_offset = locator_(req->key);
    object_dsm_offset = dsm_->buffer_offset_to_dsm_offset(object_buffer_offset);
    object_addr = dsm_->buffer_offset_to_addr(object_buffer_offset);
    DCHECK_EQ(object_addr, dsm_->dsm_offset_to_addr(object_dsm_offset));

    header_addr = protection_region;
    header_dsm_offset = dsm_->addr_to_dsm_offset(header_addr);

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
        constexpr static uint32_t magic = 0b1010101010;
        constexpr static uint16_t mask = 0b1111111111;

        auto *qp =
            dsm_->get_dir_qp(req->cid.node_id, req->cid.thread_id, dirID);
        auto *mr = dsm_->get_dir_mr(dirID);
        int access_flag = req->type == RequestType::kAcquireRLease
                              ? IBV_ACCESS_CUSTOM_REMOTE_RO
                              : IBV_ACCESS_CUSTOM_REMOTE_RW;
        // for buffer
        fill_bind_mw_wr(wrs[0],
                        &wrs[1],
                        buffer_mw,
                        mr,
                        (uint64_t) object_addr,
                        req->size,
                        access_flag);
        wrs[0].send_flags = 0;
        DVLOG(4) << "[patronus] Bind mw for buffer. addr "
                 << (void *) object_addr << "(dsm_offset: " << object_dsm_offset
                 << "), size: " << req->size << " with access flag "
                 << (int) access_flag;
        // for header
        fill_bind_mw_wr(wrs[1],
                        nullptr,
                        header_mw,
                        mr,
                        (uint64_t) header_addr,
                        sizeof(ProtectionRegion),
                        IBV_ACCESS_CUSTOM_REMOTE_RW);
        DVLOG(4) << "[patronus] Bind mw for header. addr: "
                 << (void *) header_addr
                 << " (dsm_offset: " << header_dsm_offset << ")"
                 << ", size: " << sizeof(ProtectionRegion)
                 << " with R/W access";
        wrs[1].send_flags = IBV_SEND_SIGNALED;
        wrs[1].wr_id = WRID(WRID_PREFIX_PATRONUS_BIND_MW, rw_ctx_id).val;
        if constexpr (config::kEnableSkipMagicMw)
        {
            for (size_t time = 0; time < 2; time++)
            {
                size_t id = allocated_mw_nr_++;
                if (unlikely((id & mask) == magic))
                {
                    status = AcquireRequestStatus::kMagicMwErr;
                }
            }
        }

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
    if (likely(status == AcquireRequestStatus::kSuccess))
    {
        auto *lease_ctx = get_lease_context();
        lease_ctx->client_cid = req->cid;
        lease_ctx->buffer_mw = buffer_mw;
        lease_ctx->header_mw = header_mw;
        lease_ctx->dir_id = dirID;
        lease_ctx->addr_to_bind = (uint64_t) object_addr;
        lease_ctx->buffer_size = req->size;
        lease_ctx->protection_region_id = protection_region_id;
        lease_ctx->with_conflict_detect = with_conflict_detect;
        lease_ctx->key_bucket_id = bucket_id;
        lease_ctx->key_slot_id = slot_id;
        lease_ctx->valid = true;
        auto lease_id = lease_context_.obj_to_id(lease_ctx);

        resp_msg->rkey_0 = buffer_mw->rkey;
        resp_msg->rkey_header = header_mw->rkey;
        resp_msg->buffer_base = object_dsm_offset;
        resp_msg->header_base = header_dsm_offset;
        resp_msg->lease_id = lease_id;

        bool reserved = req->flag & (uint8_t) AcquireRequestFlag::kReserved;
        DCHECK(!reserved)
            << "reserved flag should not be set. Possible corrupted message";

        // success, so register Lease expiring logic here.
        bool no_gc = req->flag & (uint8_t) AcquireRequestFlag::kNoGc;
        if (likely(!no_gc))
        {
            auto patronus_ddl = time_syncer_->patronus_later(req->required_ns);
            // set the DDL a little bit (@epsilon()) before actual DDL.
            ddl_manager_.push(
                patronus_ddl.term(),
                [this, lease_id, ctx, cid = req->cid]()
                { task_gc_lease(lease_id, cid, 0 /* flag */, ctx); });

            DVLOG(4) << "[debug] get client require ns " << req->required_ns
                     << ", patronus_ddl: " << patronus_ddl
                     << ", epsilon: " << time_syncer_->epsilon();

            resp_msg->ddl_term = patronus_ddl.term();
        }
        else
        {
            resp_msg->ddl_term =
                std::numeric_limits<decltype(resp_msg->ddl_term)>::max();
        }
    }
    else
    {
        // gc here for all allocated resources
        resp_msg->lease_id = 0;
        put_mw(dirID, buffer_mw);
        buffer_mw = nullptr;
        put_mw(dirID, header_mw);
        header_mw = nullptr;
        put_protection_region(protection_region);
        protection_region = nullptr;
    }

    if constexpr (debug())
    {
        resp_msg->digest = 0;
        resp_msg->digest = djb2_digest(resp_msg, sizeof(AcquireResponse));
    }

    auto from_mid = req->cid.mid;
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
    auto lease_id = req->lease_id;
    task_gc_lease(lease_id, req->cid, req->flag, ctx);
}

void Patronus::handle_request_lease_upgrade(LeaseModifyRequest *req,
                                            CoroContext *ctx)
{
    DCHECK_EQ(req->type, RequestType::kUpgrade);
    auto lease_id = req->lease_id;
    auto *lease_ctx = CHECK_NOTNULL(get_lease_context(lease_id));
    CHECK_EQ(lease_ctx->client_cid, req->cid)
        << "if cid not match, should return false";
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
    CHECK_EQ(lease_ctx->client_cid, req->cid)
        << "If cid not match, should return error the client.";
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
        CHECK(wr_id.prefix == WRID_PREFIX_PATRONUS_RW ||
              wr_id.prefix == WRID_PREFIX_PATRONUS_PR_RW)
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
            DLOG(WARNING) << "[patronus] rdma R/W failed. wr_id: " << wr_id
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

void Patronus::finished(CoroContext *ctx)
{
    DLOG_IF(WARNING, ctx && (ctx->coro_id() != kMasterCoro))
        << "[Patronus] Admin request better be sent by master coroutine.";
    exits_[dsm_->get_node_id()] = true;

    auto mid = dsm_->get_thread_id() % RMSG_MULTIPLEXING;
    auto tid = mid;

    char *rdma_buf = get_rdma_message_buffer();
    auto *msg = (AdminRequest *) rdma_buf;
    msg->type = RequestType::kAdmin;
    msg->flag = (uint8_t) AdminFlag::kAdminReqExit;
    msg->cid.node_id = get_node_id();
    msg->cid.thread_id = tid;
    msg->cid.mid = mid;
    msg->cid.coro_id = ctx ? ctx->coro_id() : kMasterCoro;

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
        dsm_->reliable_send(rdma_buf, sizeof(AdminRequest), i, 0);
    }

    // TODO(patronus): this may have problem
    // if the buffer is reused when NIC is DMA-ing
    put_rdma_message_buffer(rdma_buf);
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
    exits_[from_node].store(true);

    for (size_t i = 0; i < dsm_->getClusterSize(); ++i)
    {
        if (!exits_[i])
        {
            DVLOG(1) << "[patronus] receive exit request by " << from_node
                     << ". but node " << i << " not finished yet.";
            return;
        }
    }

    DVLOG(1) << "[patronus] set should_exit to true";
    should_exit_.store(true, std::memory_order_release);
}
void Patronus::signal_server_to_recover_qp(size_t node_id, size_t dir_id)
{
    auto mid = dsm_->get_thread_id() % RMSG_MULTIPLEXING;
    auto tid = mid;
    char *rdma_buf = get_rdma_message_buffer();

    auto *msg = (AdminRequest *) rdma_buf;
    msg->type = RequestType::kAdmin;
    msg->cid.node_id = get_node_id();
    msg->cid.thread_id = tid;
    msg->cid.mid = mid;
    msg->cid.coro_id = kNotACoro;
    msg->cid.rpc_ctx_id = 0;
    msg->dir_id = dir_id;
    msg->flag = (uint8_t) AdminFlag::kAdminReqRecovery;

    if constexpr (debug())
    {
        msg->digest = 0;
        msg->digest = djb2_digest(msg, sizeof(AdminRequest));
    }

    dsm_->reliable_send(rdma_buf, sizeof(AdminRequest), node_id, mid);

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
    auto protection_region_buffer = get_protection_region_buffer();
    protection_region_pool_ =
        std::make_unique<ThreadUnsafeBufferPool<sizeof(ProtectionRegion)>>(
            protection_region_buffer.buffer, protection_region_buffer.size);
}

void Patronus::registerClientThread()
{
    dsm_->registerThread();
    // - reserve 4MB for message pool. total 65536 messages, far then enough
    // - reserve other 12 MB for client's usage. If coro_nr == 8, could
    // get 1.5 MB each coro.

    auto rdma_buffer = dsm_->get_rdma_buffer();
    auto *dsm_rdma_buffer = rdma_buffer.buffer;

    size_t message_pool_size = 4 * define::MB;
    CHECK_GT(rdma_buffer.size, message_pool_size);
    size_t rdma_buffer_size = rdma_buffer.size - message_pool_size;
    CHECK_GE(message_pool_size / kMessageSize, 65536)
        << "Consider to tune up message pool size? Less than 64436 "
           "possible messages";
    CHECK_GE(rdma_buffer_size, kMaxCoroNr * kClientRdmaBufferSize)
        << "rdma_buffer not enough for maximum coroutine";

    auto *client_rdma_buffer = dsm_rdma_buffer + message_pool_size;

    rdma_message_buffer_pool_ =
        std::make_unique<ThreadUnsafeBufferPool<kMessageSize>>(
            dsm_rdma_buffer, message_pool_size);
    rdma_client_buffer_ =
        std::make_unique<ThreadUnsafeBufferPool<kClientRdmaBufferSize>>(
            client_rdma_buffer, rdma_buffer_size);
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
    DLOG_IF(WARNING, fail_nr > 0)
        << "[patronus] handle rdma finishes got failure nr: " << fail_nr
        << ". expect " << rw_context_.ongoing_size();
    cur_idx += rdma_nr;
    DCHECK_LT(cur_idx, limit);

    if (unlikely(fail_nr))
    {
        ContTimer<config::kMonitorFailureRecovery> timer;
        timer.init("Failure recovery");

        DLOG(WARNING) << "[patronus] failed. expect nr: "
                      << rw_context_.ongoing_size();
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
        lease.set_error();
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

void Patronus::server_serve()
{
    auto &server_workers = server_coro_ctx_.server_workers;
    auto &server_master = server_coro_ctx_.server_master;

    for (size_t i = 0; i < kServerCoroNr; ++i)
    {
        server_workers[i] = CoroCall([this, i](CoroYield &yield)
                                     { server_coro_worker(i, yield); });
    }
    server_master =
        CoroCall([this](CoroYield &yield) { server_coro_master(yield); });

    server_master();
}

void Patronus::server_coro_master(CoroYield &yield)
{
    auto tid = get_thread_id();
    auto mid = tid;
    auto dir_id = mid;

    auto &server_workers = server_coro_ctx_.server_workers;
    CoroContext mctx(tid, &yield, server_workers);
    auto &comm = server_coro_ctx_.comm;
    auto &task_pool = server_coro_ctx_.task_pool;

    CHECK(mctx.is_master());

    DLOG(WARNING) << "[system] about system design: use dir_id " << (int) dir_id
                  << " equal to mid " << mid << ". see if it correct.";

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

    while (likely(!should_exit()))
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
            DCHECK_EQ(wrid.prefix, WRID_PREFIX_PATRONUS_BIND_MW);
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
                VLOG(4) << "[patronus] server got dir CQE for coro "
                        << (int) coro_id << ". wr_id: " << wrid;
                *(rw_ctx->success) = true;
            }
            rw_ctx->ready.store(true, std::memory_order_release);

            VLOG(4) << "[patronus] server yield to coro " << (int) coro_id
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
                             uint8_t flag,
                             CoroContext *ctx)
{
    DVLOG(4) << "[patronus][gc_lease] task_gc_lease for lease_id " << lease_id
             << ", expect cid " << cid << ", coro: " << (ctx ? *ctx : nullctx)
             << ", at patronus time: " << time_syncer_->patronus_now();
    bool reserved = flag & (uint8_t) LeaseModifyFlag::kReserved;
    DCHECK(!reserved) << "** Reserved flag set detected";

    auto *lease_ctx = get_lease_context(lease_id);

    if (unlikely(lease_ctx == nullptr))
    {
        DVLOG(4) << "[patronus][gc_lease] skip relinquish. lease_id "
                 << lease_id << " no valid lease context.";
        return;
    }
    DCHECK(lease_ctx->valid);
    if (unlikely(lease_ctx->client_cid != cid))
    {
        DVLOG(4)
            << "[patronus][gc_lease] skip relinquish. cid mismatch: expect: "
            << lease_ctx->client_cid << ", got: " << cid;
    }

    bool no_unbind = flag & (uint8_t) LeaseModifyFlag::kNoRelinquishUnbind;
    if (likely(!no_unbind))
    {
        // should issue unbind here.
        static thread_local ibv_send_wr wrs[8];
        auto dir_id = lease_ctx->dir_id;
        auto *qp = dsm_->get_dir_qp(lease_ctx->client_cid.node_id,
                                    lease_ctx->client_cid.thread_id,
                                    dir_id);
        auto *mr = dsm_->get_dir_mr(dir_id);
        void *bind_nulladdr = dsm_->dsm_offset_to_addr(0);

        // NOTE: if size == 0, no matter access set to RO, RW or NORW
        // and no matter allocated_mw_nr +2/-2/0, it generates corrupted mws.
        // But if set size to 1 and allocated_mw_nr + 2, it works well.
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

        // never signal
        wrs[0].send_flags = 0;
        wrs[1].send_flags = 0;
        // but want to detect failure
        wrs[0].wr_id = wrs[1].wr_id =
            WRID(WRID_PREFIX_PATRONUS_UNBIND_MW, lease_id).val;
        ibv_send_wr *bad_wr;
        int ret = ibv_post_send(qp, wrs, &bad_wr);
        PLOG_IF(ERROR, ret != 0) << "[patronus][gc_lease] failed to "
                                    "ibv_post_send to unbind memory window. "
                                 << *bad_wr;
        if constexpr (config::kEnableSkipMagicMw)
        {
            allocated_mw_nr_ += 2;
        }
    }
    bool with_conflict_detect = lease_ctx->with_conflict_detect;
    if (with_conflict_detect)
    {
        auto bucket_id = lease_ctx->key_bucket_id;
        auto slot_id = lease_ctx->key_slot_id;
        lock_manager_.unlock(bucket_id, slot_id);
    }

    put_mw(lease_ctx->dir_id, lease_ctx->header_mw);
    put_mw(lease_ctx->dir_id, lease_ctx->buffer_mw);
    auto *pr = get_protection_region(lease_ctx->protection_region_id);
    // TODO(patronus): not considering any checks here
    // for example, the pr->meta.relinquished bits.
    put_protection_region(pr);
    put_lease_context(lease_ctx);
}

void Patronus::server_coro_worker(coro_t coro_id, CoroYield &yield)
{
    auto tid = get_thread_id();
    CoroContext ctx(tid, &yield, &server_coro_ctx_.server_master, coro_id);
    auto &comm = server_coro_ctx_.comm;
    auto &task_queue = comm.task_queue;
    auto &task_pool = server_coro_ctx_.task_pool;
    auto &buffer_pool = *server_coro_ctx_.buffer_pool;

    while (likely(!should_exit() || !task_queue.empty()))
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

void Patronus::wait_join(std::vector<std::thread> &threads)
{
    auto tid = get_thread_id();
    auto mid = tid;
    CHECK_EQ(tid, 0) << "[patronus] should use the master thread, i.e. the "
                        "thread constructing Patronus, to join";

    constexpr static size_t kAdminBufferNr = MAX_MACHINE;
    std::vector<char> __buffer;
    __buffer.resize(ReliableConnection::kMaxRecvBuffer * kAdminBufferNr);

    auto buffer_pool = std::make_unique<
        ThreadUnsafeBufferPool<ReliableConnection::kMaxRecvBuffer>>(
        __buffer.data(), ReliableConnection::kMaxRecvBuffer * kAdminBufferNr);
    while (!should_exit())
    {
        char *buffer = (char *) CHECK_NOTNULL(buffer_pool->get());
        DCHECK_EQ(mid, 0)
            << "[patronus] by design, use mid == 0 for admin messages";
        size_t nr =
            reliable_try_recv(mid, buffer, ReliableConnection::kRecvLimit);
        for (size_t i = 0; i < nr; ++i)
        {
            auto *base = (BaseMessage *) (buffer + i * kMessageSize);
            auto request_type = base->type;
            CHECK_EQ(request_type, RequestType::kAdmin)
                << "[patronus] Master thread only handles admin request. got "
                << request_type;
            auto *msg = (AdminRequest *) base;
            auto admin_type = (AdminFlag) msg->flag;
            CHECK_EQ(admin_type, AdminFlag::kAdminReqExit)
                << "[patronus] only handles exit admin requests";
            handle_admin_exit(msg, nullptr);
        }
        buffer_pool->put(buffer);
    }
    LOG(INFO) << "[patronus] all nodes finishes their work. joining...";

    for (auto &t : threads)
    {
        t.join();
    }
}

}  // namespace patronus