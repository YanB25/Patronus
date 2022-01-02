#pragma once
#ifndef PATRONUS_H_
#define PATRONUS_H_

#include <set>
#include <unordered_set>

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

struct RWContext
{
    bool *success{nullptr};
    std::atomic<bool> ready{false};
    coro_t coro_id;
    size_t target_node;
    size_t dir_id;
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
    constexpr static size_t kMwPoolSizePerThread = 50 * define::K;

    using KeyLocator = std::function<uint64_t(key_t)>;
    static KeyLocator identity_locator;

    static pointer ins(const DSMConfig &conf)
    {
        return std::make_shared<Patronus>(conf);
    }
    Patronus &operator=(const Patronus &) = delete;
    Patronus(const Patronus &) = delete;
    Patronus(const DSMConfig &conf);
    ~Patronus();

    void reg_locator(const KeyLocator &locator = identity_locator)
    {
        locator_ = locator;
    }
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
    bool read(const Lease &,
              char *obuf,
              size_t size,
              size_t offset,
              size_t dir_id,
              CoroContext *ctx = nullptr);
    bool write(const Lease &,
               const char *ibuf,
               size_t size,
               size_t offset,
               CoroContext *ctx = nullptr);
    void finished([[maybe_unused]] CoroContext *ctx = nullptr);

    void registerServerThread()
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

    size_t reliable_try_recv(size_t from_mid, char *ibuf, size_t limit = 1)
    {
        return dsm_->reliable_try_recv(from_mid, ibuf, limit);
    }
    size_t try_get_server_finished_coros(coro_t *buf,
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

    bool should_exit() const
    {
        return should_exit_.load(std::memory_order_relaxed);
    }

    size_t try_get_client_continue_coros(size_t mid,
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
        size_t fail_nr = handle_rdma_finishes(
            wc_buffer, rdma_nr, coro_buf + cur_idx, recovery);
        LOG_IF(WARNING, fail_nr > 0)
            << "[patronus] handle rdma finishes got failure nr: " << fail_nr
            << ". expect " << rw_context_.ongoing_size();
        cur_idx += rdma_nr;
        DCHECK_LT(cur_idx, limit);

        if (unlikely(fail_nr))
        {
            while (fail_nr < rw_context_.ongoing_size())
            {
                auto another_nr =
                    dsm_->try_poll_rdma_cq(wc_buffer, kBufferSize);
                auto another_rdma_nr = handle_rdma_finishes(
                    wc_buffer, another_nr, coro_buf + cur_idx, recovery);
                DCHECK_EQ(another_rdma_nr, another_nr);
                cur_idx += another_nr;
                DCHECK_LT(cur_idx, limit)
                    << "** Provided buffer not enough to handle error message.";
                fail_nr += another_nr;
            }
            // TODO: why we need to join before recovering QP?
            size_t got = msg_nr;
            size_t expect_rpc_nr = rpc_context_.ongoing_size();
            LOG(INFO) << "expect rpc: " << expect_rpc_nr;
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

private:
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
        return rw_context_.get();
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

    size_t handle_response_messages(const char *msg_buf,
                                    size_t msg_nr,
                                    coro_t *o_coro_buf);
    size_t handle_rdma_finishes(ibv_wc *buffer,
                                size_t rdma_nr,
                                coro_t *o_coro_buf,
                                std::set<std::pair<size_t, size_t>> &recov);

    void signal_server_to_recover_qp(size_t node_id, size_t dir_id);
    void handle_request_acquire(AcquireRequest *, CoroContext *ctx);
    void handle_response_acquire(AcquireResponse *);
    void handle_admin_exit(AdminRequest *req, CoroContext *ctx);
    void handle_admin_recover(AdminRequest *req, CoroContext *ctx);

    // owned by both
    DSM::pointer dsm_;
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

    // for admin management
    std::array<std::atomic<bool>, MAX_MACHINE> exits_;
    std::atomic<bool> should_exit_{false};

    // for user interfaces
    KeyLocator locator_;
};
}  // namespace patronus

#endif