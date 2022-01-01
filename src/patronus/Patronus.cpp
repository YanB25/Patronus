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
thread_local std::queue<ibv_mw *> Patronus::mw_pool_[NR_DIRECTORY];

Patronus::KeyLocator Patronus::identity_locator = [](key_t key) -> uint64_t
{
    return (uint64_t) key;
};

template <typename T>
using PResult = Patronus::PResult<T>;

Patronus::Patronus(const DSMConfig &conf)
{
    dsm_ = DSM::getInstance(conf);
}
Patronus::~Patronus()
{
    for (ibv_mw* mw: allocated_mws_)
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
    uint16_t rpc_ctx_id = get_context_id(rpc_context);
    DVLOG(3) << "[debug] allocating rpc_context " << (void *) rpc_context
             << " at id " << rpc_ctx_id;

    Lease ret_lease;
    ret_lease.node_id_ = node_id;

    auto *msg = (AcquireRequest *) rdma_buf;
    msg->type = RequestType::kAcquireRLease;
    msg->cid.node_id = get_node_id();
    msg->cid.thread_id = tid;
    msg->cid.mid = mid;
    msg->cid.coro_id = ctx ? ctx->coro_id : 0;
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
        while (rpc_context->ready.load(std::memory_order_acquire))
        {
            std::this_thread::yield();
        }
    }
    else
    {
        (*ctx->yield)(*ctx->master);
    }
    DCHECK(rpc_context->ready)
        << "** Should have been ready when switch back to worker coro";

    if constexpr (debug())
    {
        VLOG(3) << "[debug] setting rpc_context to zero. at "
                << (void *) rpc_context;
        memset(rpc_context, 0, sizeof(RpcContext));
    }

    put_rpc_context(rpc_context);
    put_rdma_message_buffer(rdma_buf);
    return ret_lease;
}

void Patronus::read(const Lease &lease,
                    char *obuf,
                    size_t size,
                    size_t offset,
                    size_t dir_id,
                    CoroContext *ctx)
{
    GlobalAddress gaddr;
    gaddr.nodeID = lease.node_id_;
    DCHECK_NE(gaddr.nodeID, get_node_id())
        << "make no sense to read local buffer with RDMA.";
    gaddr.offset = offset + lease.base_addr_;
    CHECK_LE(size, lease.buffer_size_);
    auto coro_id = ctx ? ctx->coro_id : 0;
    if constexpr (debug())
    {
        debug_valid_rdma_buffer(obuf);
    }
    dsm_->rkey_read(lease.cur_rkey_,
                    obuf,
                    gaddr,
                    size,
                    dir_id,
                    true,
                    ctx,
                    WRID(WRID_PREFIX_PATRONUS_RW, coro_id).val);
}

void Patronus::handle_response_messages(const char *msg_buf, size_t msg_nr)
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
            handle_admin(msg, nullptr);
            break;
        }
        default:
        {
            LOG(FATAL) << "Unknown response type " << (int) request_type
                       << ". Possible corrupted message";
        }
        }
    }
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
            DVLOG(4) << "[patronus] handling acquire request " << *msg << " coro " << *ctx;
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
            DVLOG(4) << "[patronus] handling admin request " << *msg << " " << *ctx;
            handle_admin(msg, ctx);
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
    VLOG(3) << "[debug] getting rpc_context " << (void *) rpc_context
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
             << ", internal_buf: " << (void *) internal.buffer << ", size "
             << internal.size;
    uint64_t actual_position = locator_(req->key);
    dsm_->bind_memory_region_sync(mw,
                                  req->cid.node_id,
                                  req->cid.thread_id,
                                  internal.buffer + actual_position,
                                  req->size,
                                  dirID,
                                  ctx);

    auto *resp_buf = get_rdma_message_buffer();
    auto *resp_msg = (AcquireResponse *) resp_buf;
    resp_msg->type = req->type;
    resp_msg->rkey_0 = mw->rkey;
    resp_msg->base = actual_position;
    resp_msg->term = req->require_term;
    resp_msg->cid = req->cid;
    resp_msg->cid.node_id = get_node_id();
    resp_msg->cid.thread_id = get_thread_id();
    if constexpr (debug())
    {
        resp_msg->digest = 0;
        resp_msg->digest = djb2_digest(resp_msg, sizeof(AcquireResponse));
    }
    dsm_->reliable_send(
        (char *) resp_msg, sizeof(AcquireResponse), req->cid.node_id, dirID);

    put_rdma_message_buffer(resp_buf);
}

void Patronus::finished(CoroContext *ctx)
{
    DLOG_IF(WARNING, ctx && (ctx->coro_id != kMasterCoro))
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
    msg->cid.coro_id = ctx ? ctx->coro_id : kMasterCoro;

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

void Patronus::handle_admin(AdminRequest *req,
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
}  // namespace patronus