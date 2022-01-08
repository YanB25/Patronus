#include "patronus/Patronus.h"

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

Patronus::KeyLocator Patronus::identity_locator = [](key_t key) -> uint64_t
{ return (uint64_t) key; };

Patronus::Patronus(const DSMConfig &conf)
{
    dsm_ = DSM::getInstance(conf);
}
Patronus::~Patronus()
{
    for (ibv_mw *mw : allocated_mws_)
    {
        dsm_->free_mw(mw);
    }
}

Lease Patronus::get_lease_impl(uint16_t node_id,
                               uint16_t dir_id,
                               id_t key,
                               size_t size,
                               term_t term,
                               bool is_read_lease,
                               CoroContext *ctx)
{
    // TODO(patronus): see if this tid to mid binding is correct.
    auto mid = dsm_->get_thread_id() % RMSG_MULTIPLEXING;
    auto tid = mid;

    char *rdma_buf = get_rdma_message_buffer();
    auto *rpc_context = get_rpc_context();
    uint16_t rpc_ctx_id = get_rpc_context_id(rpc_context);
    // DVLOG(3) << "[debug] allocating rpc_context " << (void *) rpc_context
    //          << " at id " << rpc_ctx_id;

    Lease ret_lease;
    ret_lease.node_id_ = node_id;

    auto *msg = (AcquireRequest *) rdma_buf;
    msg->type = is_read_lease ? RequestType::kAcquireRLease
                              : RequestType::kAcquireWLease;
    msg->cid.node_id = get_node_id();
    msg->cid.thread_id = tid;
    msg->cid.mid = mid;
    msg->cid.coro_id = ctx ? ctx->coro_id() : kNotACoro;
    msg->cid.rpc_ctx_id = rpc_ctx_id;
    msg->dir_id = dir_id;
    msg->key = key;
    msg->size = size;
    msg->require_term = term;

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
        ctx->yield_to_master();
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

bool Patronus::read_write_impl(Lease &lease,
                               char *iobuf,
                               size_t size,
                               size_t offset,
                               size_t dir_id,
                               bool is_read,
                               CoroContext *ctx = nullptr)
{
    CHECK(lease.success());

    if (is_read)
    {
        CHECK(lease.is_read_lease());
    }
    else
    {
        CHECK(lease.is_write_lease());
    }
    bool ret = false;

    GlobalAddress gaddr;
    gaddr.nodeID = lease.node_id_;
    DCHECK_NE(gaddr.nodeID, get_node_id())
        << "make no sense to read local buffer with RDMA.";
    gaddr.offset = offset + lease.base_addr_;
    CHECK_LE(size, lease.buffer_size_);
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
        dsm_->rkey_read(lease.cur_rkey_,
                        iobuf,
                        gaddr,
                        size,
                        dir_id,
                        true,
                        ctx,
                        WRID(WRID_PREFIX_PATRONUS_RW, rw_ctx_id).val);
    }
    else
    {
        dsm_->rkey_write(lease.cur_rkey_,
                         iobuf,
                         gaddr,
                         size,
                         dir_id,
                         true,
                         ctx,
                         WRID(WRID_PREFIX_PATRONUS_RW, rw_ctx_id).val);
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

Lease Patronus::lease_modify_impl(Lease &lease,
                                  RequestType type,
                                  term_t term,
                                  CoroContext *ctx)
{
    CHECK(type == RequestType::kExtend || type == RequestType::kRelinquish ||
          type == RequestType::kUpgrade)
        << "** invalid type " << (int) type;

    // TODO(patronus): see if this tid to mid binding is correct
    auto mid = dsm_->get_thread_id() % RMSG_MULTIPLEXING;
    auto tid = mid;
    auto target_node_id = lease.node_id_;

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
    msg->term = term;

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
           resp->type == RequestType::kAcquireWLease)
        << "** unexpected request type received. got: " << (int) resp->type;

    auto &ret_lease = *rpc_context->ret_lease;
    ret_lease.base_addr_ = resp->base;
    ret_lease.node_id_ = resp->cid.node_id;
    // TODO(patronus), CRITICAL(patronus):
    // not consider header yet. please fix me.
    ret_lease.buffer_size_ = request->size;
    ret_lease.rkey_0_ = resp->rkey_0;
    ret_lease.cur_rkey_ = ret_lease.rkey_0_;
    ret_lease.ex_rkey_ = 0;  // TODO(patronus): same reason above
    ret_lease.cur_ddl_term_ = resp->term;
    ret_lease.id_ = resp->lease_id;
    ret_lease.dir_id_ = rpc_context->dir_id;
    if (resp->success)
    {
        ret_lease.set_finish();
    }
    else
    {
        ret_lease.set_error();
    }
    if (resp->type == RequestType::kAcquireRLease)
    {
        ret_lease.lease_type_ = LeaseType::kReadLease;
    }
    else
    {
        ret_lease.lease_type_ = LeaseType::kWriteLease;
    }

    rpc_context->ready.store(true, std::memory_order_release);
}

void Patronus::handle_request_acquire(AcquireRequest *req, CoroContext *ctx)
{
    auto dirID = req->dir_id;
    // TODO(patronus): when to put_mw ?
    auto *mw = get_mw(dirID);
    auto internal = dsm_->get_server_internal_buffer();
    if constexpr (debug())
    {
        uint64_t digest = req->digest.get();
        req->digest = 0;
        DCHECK_EQ(digest, djb2_digest(req, sizeof(AcquireRequest)));
    }
    CHECK_LT(req->size, internal.size);
    DVLOG(4) << "[patronus] Trying to bind with dirID " << dirID
             << ", internal_buf: " << (void *) internal.buffer << ", buf size "
             << internal.size << ", bind size " << req->size;
    uint64_t actual_position = locator_(req->key);

    bool success = false;

    auto *rw_ctx = get_rw_context();
    auto rw_ctx_id = rw_context_.obj_to_id(rw_ctx);
    rw_ctx->coro_id = ctx ? ctx->coro_id() : kNotACoro;
    rw_ctx->dir_id = dirID;
    rw_ctx->ready = false;
    rw_ctx->success = &success;

    bool call_succ = dsm_->bind_memory_region_sync(
        mw,
        req->cid.node_id,
        req->cid.thread_id,
        internal.buffer + actual_position,
        req->size,
        dirID,
        WRID(WRID_PREFIX_PATRONUS_BIND_MW, rw_ctx_id).val,
        ctx);

    if (likely(ctx != nullptr))
    {
        DCHECK(call_succ);
        DCHECK(rw_ctx->success)
            << "** Should set to finished when switch back to worker "
               "coroutine. coro: "
            << *ctx << " ready: @" << (void *) rw_ctx->success;
    }
    else
    {
        success = call_succ;
    }

    auto *lease_ctx = get_lease_context();
    lease_ctx->mw = mw;
    lease_ctx->dir_id = dirID;
    lease_ctx->addr_to_bind = (uint64_t) (internal.buffer + actual_position);
    lease_ctx->size = req->size;
    auto lease_id = lease_context_.obj_to_id(lease_ctx);

    auto *resp_buf = get_rdma_message_buffer();
    auto *resp_msg = (AcquireResponse *) resp_buf;
    resp_msg->type = req->type;
    resp_msg->rkey_0 = mw->rkey;
    resp_msg->base = actual_position;
    resp_msg->term = req->require_term;
    resp_msg->cid = req->cid;
    resp_msg->cid.node_id = get_node_id();
    resp_msg->cid.thread_id = get_thread_id();

    if (likely(success))
    {
        resp_msg->lease_id = lease_id;
        resp_msg->success = true;
    }
    else
    {
        resp_msg->lease_id = 0;
        resp_msg->success = false;
        put_lease_context(lease_ctx);
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
    put_rw_context(rw_ctx);
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
    auto *lease_ctx = get_lease_context(lease_id);

    DVLOG(4) << "[patronus] reqlinquish lease. lease_id " << (id_t) lease_id
             << ", coro: " << (ctx ? *ctx : nullctx);
    put_mw(lease_ctx->dir_id, lease_ctx->mw);

    put_lease_context(lease_ctx);

    // no rely is needed
}

void Patronus::handle_request_lease_upgrade(LeaseModifyRequest *req,
                                            CoroContext *ctx)
{
    DCHECK_EQ(req->type, RequestType::kUpgrade);
    auto lease_id = req->lease_id;
    auto *lease_ctx = get_lease_context(lease_id);
    auto dir_id = lease_ctx->dir_id;
    auto *mw = lease_ctx->mw;

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
        lease_ctx->size,
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
    auto *lease_ctx = get_lease_context(lease_id);
    auto dir_id = lease_ctx->dir_id;
    auto *mw = lease_ctx->mw;

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
        lease_ctx->size,
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
        DCHECK_EQ(wr_id.prefix, WRID_PREFIX_PATRONUS_RW);
        auto *rw_context = rw_context_.id_to_obj(id);
        auto node_id = rw_context->target_node;
        auto dir_id = rw_context->dir_id;
        auto coro_id = rw_context->coro_id;
        CHECK_NE(coro_id, kMasterCoro)
            << "** Coro master should not issue R/W.";

        if (likely(wc.status == IBV_WC_SUCCESS))
        {
            *(rw_context->success) = true;
            VLOG(4) << "[patronus] handle rdma finishes SUCCESS for coro "
                    << (int) coro_id << ". set " << (void *) rw_context->success
                    << " to true.";
        }
        else
        {
            VLOG(1) << "[patronus] rdma R/W failed. wr_id: " << wr_id
                    << " for node " << node_id << ", dir: " << dir_id
                    << ", coro_id: " << (int) coro_id;
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

    auto from_node = req->cid.node_id;
    auto tid = req->cid.thread_id;
    auto dir_id = req->dir_id;
    CHECK(dsm_->recoverDirQP(from_node, tid, dir_id));
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
            DVLOG(4) << "[patronus] Not exit becasue node " << i
                     << " not finished yet.";
            return;
        }
    }

    DVLOG(4) << "[patronux] set should_exit to true.";
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
    // for server, all the buffers are given to rdma_message_buffer_pool_
    dsm_->registerThread();

    auto *dsm_rdma_buffer = dsm_->get_rdma_buffer();
    size_t message_pool_size = define::kRDMABufferSize;
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
}

void Patronus::registerClientThread()
{
    dsm_->registerThread();
    // - reserve 4MB for message pool. total 65536 messages, far then enough
    // - reserve other 12 MB for client's usage. If coro_nr == 8, could
    // get 1.5 MB each coro.
    size_t message_pool_size = 4 * define::MB;
    CHECK_GT(define::kRDMABufferSize, message_pool_size);
    size_t rdma_buffer_size = define::kRDMABufferSize - message_pool_size;
    CHECK_GE(message_pool_size / kMessageSize, 65536)
        << "Consider to tune up message pool size? Less than 64436 "
           "possible messages";
    CHECK_GE(rdma_buffer_size, kMaxCoroNr * kClientRdmaBufferSize)
        << "rdma_buffer not enough for maximum coroutine";

    auto *dsm_rdma_buffer = dsm_->get_rdma_buffer();
    auto *client_rdma_buffer = dsm_rdma_buffer + message_pool_size;

    rdma_message_buffer_pool_ =
        std::make_unique<ThreadUnsafeBufferPool<kMessageSize>>(
            dsm_rdma_buffer, message_pool_size);
    rdma_client_buffer_ =
        std::make_unique<ThreadUnsafeBufferPool<kClientRdmaBufferSize>>(
            client_rdma_buffer, rdma_buffer_size);
}

size_t Patronus::try_get_server_finished_coros(coro_t *buf,
                                               size_t dirID,
                                               size_t limit)
{
    constexpr static size_t kBufferSize = 16;
    static thread_local ibv_wc wc_buffer[kBufferSize];
    auto nr = dsm_->try_poll_dir_cq(wc_buffer, dirID, limit);

    // TODO(patronus): server can recovery before client telling it to do.
    // bool need_recovery = false;
    for (size_t i = 0; i < nr; ++i)
    {
        auto &wc = wc_buffer[i];
        log_wc_handler(&wc);
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
            LOG(WARNING) << "[patronus] server got failed wc: " << wrid
                         << " for coro " << (int) coro_id;
            // need_recovery = true;
            *(rw_ctx->success) = false;
        }
        else
        {
            VLOG(4) << "[patronus] server got dir CQE for coro " << coro_id
                    << ". wr_id: " << wrid;
            *(rw_ctx->success) = true;
        }
        rw_ctx->ready.store(true, std::memory_order_release);

        buf[i] = coro_id;
    }
    return nr;
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
    }

    for (const auto &[node_id, dir_id] : recovery)
    {
        signal_server_to_recover_qp(node_id, dir_id);
        CHECK(dsm_->recoverThreadQP(node_id, dir_id));
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
           resp->type != RequestType::kAcquireWLease)
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

}  // namespace patronus