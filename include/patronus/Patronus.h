#pragma once
#ifndef PATRONUS_H_
#define PATRONUS_H_

#include "DSM.h"
#include "Result.h"
#include "patronus/Lease.h"
#include "patronus/Type.h"
#include "util/Debug.h"

namespace patronus
{
class Patronus;

struct RpcContext
{
    Lease *lease{nullptr};
    BaseMessage *request{nullptr};
    std::atomic<bool> ready{false};
};

class Patronus
{
public:
    constexpr static size_t kMaxCoroNr = 8;
    using pointer = std::shared_ptr<Patronus>;
    template <typename T>
    using PResult = Result<T, Void>;

    constexpr static size_t kMessageSize = ReliableConnection::kMessageSize;
    constexpr static size_t kClientRdmaBufferSize = 1 * define::MB;

    // TODO(patronus): try to tune this parameter up.
    constexpr static size_t kTotalMwPoolSize = 1 * define::M;

    static pointer ins(const DSMConfig &conf)
    {
        return std::make_shared<Patronus>(conf);
    }
    Patronus &operator=(const Patronus &) = delete;
    Patronus(const Patronus &) = delete;
    Patronus(const DSMConfig &conf);

    /**
     * @brief Get the rlease object
     *
     * @param node_id the id of target node
     * @param dir_id  the dir_id.
     * @param key the unique key to identify the object / address you want to
     * access
     * @param size the length of the object / address
     * @param term the length of term requesting for protection
     * @param ctx sync call if ctx is nullptr. Otherwise coroutine context.
     * @return Read Lease
     */
    Lease get_rlease(uint16_t node_id,
                     uint16_t dir_id,
                     id_t key,
                     size_t size,
                     term_t term,
                     CoroContext *ctx = nullptr);
    Lease get_wlease(uint16_t node_id,
                     uint16_t dir_id,
                     id_t key,
                     size_t size,
                     term_t term,
                     CoroContext *ctx = nullptr);
    Lease upgrade(const Lease &rlease, CoroContext *ctx = nullptr);
    Lease downgrade(const Lease &wlease, CoroContext *ctx = nullptr);
    Lease extend(const Lease &rlease, CoroContext *ctx = nullptr);
    Lease relinquish(const Lease &rlease, CoroContext *ctx = nullptr);
    void read(const Lease &,
              char *obuf,
              size_t size,
              size_t offset,
              size_t dir_id,
              CoroContext *ctx = nullptr);
    void write(const Lease &,
               const char *ibuf,
               size_t size,
               size_t offset,
               CoroContext *ctx = nullptr);

    void registerServerThread()
    {
        // for server, all the buffers are given to rdma_message_buffer_pool_
        dsm_->registerThread();

        auto *dsm_rdma_buffer = dsm_->get_rdma_buffer();
        size_t message_pool_size = define::kRDMABufferSize;
        rdma_message_buffer_pool_ =
            std::make_unique<ThreadUnsafeBufferPool<kMessageSize>>(
                dsm_rdma_buffer, message_pool_size);

        size_t alloc_mw_nr = kTotalMwPoolSize / MAX_APP_THREAD / NR_DIRECTORY;
        for (size_t dirID = 0; dirID < NR_DIRECTORY; ++dirID)
        {
            for (size_t i = 0; i < alloc_mw_nr; ++i)
            {
                auto *mw = CHECK_NOTNULL(dsm_->alloc_mw(dirID));
                mw_pool_[dirID].push(mw);
            }
        }
    }
    void registerClientThread()
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
        LOG_IF(WARNING, rdma_buffer_size < kMaxCoroNr * 1 * define::MB)
            << "Can not ensure 1MB buffer for each coroutine. consider to tune "
               "the size up.";

        auto *dsm_rdma_buffer = dsm_->get_rdma_buffer();
        auto *client_rdma_buffer = dsm_rdma_buffer + message_pool_size;

        rdma_message_buffer_pool_ =
            std::make_unique<ThreadUnsafeBufferPool<kMessageSize>>(
                dsm_rdma_buffer, message_pool_size);
        rdma_client_buffer_ =
            std::make_unique<ThreadUnsafeBufferPool<1 * define::MB>>(
                client_rdma_buffer, rdma_buffer_size);
    }
    size_t get_node_id() const
    {
        return dsm_->get_node_id();
    }
    size_t get_thread_id() const
    {
        return dsm_->get_thread_id();
    }

    void handle_request_messages(const char *msg_buf,
                                 size_t msg_nr,
                                 CoroContext *ctx = nullptr);
    void handle_response_messages(const char *msg_buf, size_t msg_nr);

    size_t reliable_try_recv(size_t from_mid, char *ibuf, size_t limit = 1)
    {
        return dsm_->reliable_try_recv(from_mid, ibuf, limit);
    }
    /**
     * @brief
     *
     * @param buf at least
     * @param limit
     * @return size_t
     */
    size_t try_get_rdma_finished_coros(coro_t *buf, size_t limit)
    {
        constexpr static size_t kBufferSize = 16;
        static thread_local ibv_wc wc_buffer[kBufferSize];
        auto nr =
            dsm_->try_poll_rdma_cq(wc_buffer, std::min(kBufferSize, limit));
        for (size_t i = 0; i < nr; ++i)
        {
            auto &wc = wc_buffer[i];
            log_wc_handler(&wc);
            if (unlikely(wc.status == IBV_WC_WR_FLUSH_ERR))
            {
                LOG(WARNING)
                    << "[patronus] QP error. skip all the following wrs.";
                break;
            }
            auto wrid = WRID(wc.wr_id);
            DCHECK_EQ(wrid.prefix, WRID_PREFIX_PATRONUS_RW);
            // this will be coro_id by design
            buf[i] = wrid.id;
        }
        return nr;
    }
    size_t try_get_server_finished_coros(coro_t *buf,
                                         size_t dirID,
                                         size_t limit)
    {
        constexpr static size_t kBufferSize = 16;
        static thread_local ibv_wc wc_buffer[kBufferSize];
        auto nr = dsm_->try_poll_dir_cq(wc_buffer, dirID, limit);
        for (size_t i = 0; i < nr; ++i)
        {
            auto &wc = wc_buffer[i];
            log_wc_handler(&wc);
            if (unlikely(wc.status == IBV_WC_WR_FLUSH_ERR))
            {
                LOG(WARNING)
                    << "[patronus] QP error. skip all the following wrs.";
                break;
            }
            auto wrid = WRID(wc.wr_id);
            DCHECK_EQ(wrid.prefix, WRID_PREFIX_PATRONUS_BIND_MW);
            // this will be coro_id by design
            buf[i] = wrid.id;
        }
        return nr;
    }
    std::shared_ptr<DSM> get_dsm()
    {
        return dsm_;
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

private:
    ibv_mw *get_mw(size_t dirID)
    {
        auto *ret = mw_pool_[dirID].front();
        mw_pool_[dirID].pop();
        return ret;
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
    uint16_t get_context_id(RpcContext *ctx)
    {
        return rpc_context_.obj_to_id(ctx);
    }

    void handle_request_acquire(AcquireRequest *, CoroContext *ctx);
    void handle_response_acquire(AcquireResponse *);

    // owned by both
    DSM::pointer dsm_;
    static thread_local std::unique_ptr<ThreadUnsafeBufferPool<kMessageSize>>
        rdma_message_buffer_pool_;

    // owned by client threads
    static thread_local ThreadUnsafePool<RpcContext, kMaxCoroNr> rpc_context_;
    static thread_local std::unique_ptr<
        ThreadUnsafeBufferPool<kClientRdmaBufferSize>>
        rdma_client_buffer_;

    // owned by server threads
    // [NR_DIRECTORY]
    static thread_local std::queue<ibv_mw *> mw_pool_[NR_DIRECTORY];
};
}  // namespace patronus

#endif