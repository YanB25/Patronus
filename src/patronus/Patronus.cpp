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

Patronus::KeyLocator Patronus::identity_locator = [](key_t key) -> uint64_t
{ return (uint64_t) key; };

template <typename T>
using PResult = Patronus::PResult<T>;

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

Lease Patronus::get_rlease(uint16_t node_id,
                           uint16_t dir_id,
                           id_t key,
                           size_t size,
                           term_t term,
                           CoroContext *ctx)
{
    auto mid = dsm_->get_thread_id() % RMSG_MULTIPLEXING;
    auto tid = mid;

    char *rdma_buf = get_rdma_message_buffer();
    auto *rpc_context = get_rpc_context();
    uint16_t rpc_ctx_id = get_rpc_context_id(rpc_context);
    DVLOG(3) << "[debug] allocating rpc_context " << (void *) rpc_context
             << " at id " << rpc_ctx_id;

    Lease ret_lease;
    ret_lease.node_id_ = node_id;

    auto *msg = (AcquireRequest *) rdma_buf;
    msg->type = RequestType::kAcquireRLease;
    msg->cid.node_id = get_node_id();
    msg->cid.thread_id = tid;
    msg->cid.mid = mid;
    msg->cid.coro_id = ctx ? ctx->coro_id() : kNotACoro;
    msg->cid.rpc_ctx_id = rpc_ctx_id;
    msg->dir_id = dir_id;
    msg->key = key;
    msg->size = size;
    msg->require_term = term;

    rpc_context->lease = &ret_lease;
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

bool Patronus::read(const Lease &lease,
                    char *obuf,
                    size_t size,
                    size_t offset,
                    size_t dir_id,
                    CoroContext *ctx)
{
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
        debug_valid_rdma_buffer(obuf);
    }

    auto *rw_context = get_rw_context();
    uint16_t rw_ctx_id = get_rw_context_id(rw_context);
    rw_context->success = &ret;
    rw_context->ready = false;
    rw_context->coro_id = coro_id;
    rw_context->target_node = gaddr.nodeID;
    rw_context->dir_id = dir_id;

    // already switch ctx if not null
    dsm_->rkey_read(lease.cur_rkey_,
                    obuf,
                    gaddr,
                    size,
                    dir_id,
                    true,
                    ctx,
                    WRID(WRID_PREFIX_PATRONUS_RW, rw_ctx_id).val);

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
        {
            DVLOG(4) << "[patronus] handling upgrade response";
            LOG(FATAL) << "TODO";
            break;
        }
        case RequestType::kExtend:
        {
            DVLOG(4) << "[patronus] handling extend response";
            LOG(FATAL) << "TODO";
            break;
        }
        case RequestType::kRelinquish:
        {
            DVLOG(4) << "[patronus] handling relinquish response";
            LOG(FATAL) << "TODO";
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
        {
            DVLOG(4) << "[patronus] handling upgrade request";
            LOG(FATAL) << "TODO";
            break;
        }
        case RequestType::kExtend:
        {
            DVLOG(4) << "[patronus] handling extend request";
            LOG(FATAL) << "TODO";
            break;
        }
        case RequestType::kRelinquish:
        {
            DVLOG(4) << "[patronus] handling relinquish request";
            LOG(FATAL) << "TODO";
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

    auto &lease = *rpc_context->lease;
    lease.base_addr_ = resp->base;
    lease.node_id_ = resp->cid.node_id;
    // TODO(patronus), CRITICAL(patronus):
    // not consider header yet. please fix me.
    lease.buffer_size_ = request->size;
    lease.rkey_0_ = resp->rkey_0;
    lease.cur_rkey_ = lease.rkey_0_;
    lease.ex_rkey_ = 0;  // TODO(patronus): same reason above
    lease.cur_ddl_term_ = resp->term;
    if (resp->success)
    {
        lease.set_finish();
    }
    else
    {
        lease.set_error();
    }
    if (resp->type == RequestType::kAcquireRLease)
    {
        lease.lease_type_ = LeaseType::kReadLease;
    }
    else
    {
        lease.lease_type_ = LeaseType::kWriteLease;
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

    dsm_->bind_memory_region_sync(
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
        DCHECK(rw_ctx->success)
            << "** Should set to finished when switch back to worker "
               "coroutine. coro: "
            << *ctx << " ready: @" << (void *) rw_ctx->success;
    }

    auto *resp_buf = get_rdma_message_buffer();
    auto *resp_msg = (AcquireResponse *) resp_buf;
    resp_msg->type = req->type;
    resp_msg->rkey_0 = mw->rkey;
    resp_msg->base = actual_position;
    resp_msg->term = req->require_term;
    resp_msg->cid = req->cid;
    resp_msg->cid.node_id = get_node_id();
    resp_msg->cid.thread_id = get_thread_id();
    resp_msg->success = success;

    if constexpr (debug())
    {
        resp_msg->digest = 0;
        resp_msg->digest = djb2_digest(resp_msg, sizeof(AcquireResponse));
    }
    dsm_->reliable_send(
        (char *) resp_msg, sizeof(AcquireResponse), req->cid.node_id, dirID);

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
        log_wc_handler(&wc);
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
            LOG(WARNING) << "[patronus] rdma R/W failed. wr_id: " << wr_id
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
    // TODO: not put rdma_message_buffer here.

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
}
}  // namespace patronus