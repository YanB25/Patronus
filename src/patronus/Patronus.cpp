#include "patronus/Patronus.h"

namespace patronus
{
thread_local std::unique_ptr<ThreadUnsafeBufferPool<Patronus::kMessageSize>>
    Patronus::rdma_buffers_;
thread_local ThreadUnsafePool<RpcContext, Patronus::kMaxCoroNr>
    Patronus::rpc_context_;

template <typename T>
using PResult = Patronus::PResult<T>;

Patronus::Patronus(const DSMConfig &conf)
{
    dsm_ = DSM::getInstance(conf);
}

Lease Patronus::get_rlease(uint16_t node_id,
                           id_t key,
                           term_t term,
                           CoroContext *ctx)
{
    auto mid = dsm_->get_thread_id() % RMSG_MULTIPLEXING;
    auto tid = mid;

    char *rdma_buf = get_rdma_buffer();
    auto *rpc_context = get_rpc_context();
    uint16_t rpc_ctx_id = get_context_id(rpc_context);

    Lease ret_lease;
    ret_lease.node_id_ = node_id;

    rpc_context->lease = &ret_lease;
    rpc_context->ready = false;

    auto *msg = (AcquireRequest *) rdma_buf;
    msg->cid.node_id = node_id;
    msg->cid.thread_id = tid;
    msg->cid.mid = mid;
    msg->cid.coro_id = ctx ? ctx->coro_id : 0;
    msg->cid.rpc_ctx_id = rpc_ctx_id;
    msg->key = key;
    msg->require_term = term;
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

    put_rpc_context(rpc_context);
    put_rdma_buffer(rdma_buf);
    return ret_lease;
}

void Patronus::read(const Lease &lease,
                    char *obuf,
                    size_t size,
                    size_t offset,
                    CoroContext *ctx)
{
    GlobalAddress gaddr;
    gaddr.nodeID = lease.node_id_;
    gaddr.offset = offset;
    // TODO(patronus): use dir_id = 0 here. Is it correct?
    dsm_->rkey_read(lease.cur_rkey_,
                    obuf,
                    gaddr,
                    size,
                    0,
                    true,
                    ctx,
                    ctx ? ctx->coro_id : 0);
}

void Patronus::handle_messages(const char *msg_buf, size_t msg_nr, size_t dirID)
{
    for (size_t i = 0; i < msg_nr; ++i)
    {
        auto *base = (BaseRequest *) (msg_buf + i * kMessageSize);
        auto request_type = base->type;
        switch (request_type)
        {
        case RequestType::kAcquire:
        {
            VLOG(2) << "[patronus] handling kAcquire";
            auto *msg = (AcquireRequest *) base;
            handle_acquire(msg, dirID);
            break;
        }
        case RequestType::kUpgrade:
        {
            VLOG(2) << "[patronus] handling kUpgrade";
            break;
        }
        case RequestType::kExtend:
        {
            VLOG(2) << "[patronus] handling kExtend";
            break;
        }
        case RequestType::kRelinquish:
        {
            VLOG(2) << "[patronus] handling kRelinquish";
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

void Patronus::handle_acquire(AcquireRequest *req, size_t dirID)
{
    auto *mw = dsm_->alloc_mw(dirID);
    auto internal = dsm_->get_server_internal_buffer();
    // TODO(patronus): use coroutine here.
    // TODO(patronus): use pre-allocated mw here.
    dsm_->bind_memory_region_sync(mw,
                                  req->cid.node_id,
                                  req->cid.thread_id,
                                  internal.buffer,
                                  4 * 1024,
                                  dirID);

    auto *resp_buf = get_rdma_buffer();
    auto *resp_msg = (AcquireResponse *) resp_buf;
    resp_msg->type = req->type;
    resp_msg->rkey_0 = mw->rkey;
    dsm_->reliable_send(
        (char*) resp_msg, sizeof(AcquireResponse), req->cid.node_id, dirID);

    put_rdma_buffer(resp_buf);
}

}  // namespace patronus