#pragma once
#ifndef PATRONUS_H_
#define PATRONUS_H_

#include "DSM.h"
#include "Result.h"
#include "patronus/Lease.h"
#include "patronus/Type.h"

namespace patronus
{
class Patronus;

struct RpcContext
{
    Lease *lease;
    std::atomic<bool> ready{false};
};

class Patronus
{
public:
    constexpr static size_t kMaxCoroNr = 8;
    constexpr static size_t kMaxLeasePerCoro = 8;
    using pointer = std::unique_ptr<Patronus>;
    template <typename T>
    using PResult = Result<T, Void>;

    constexpr static size_t kReserveMessageNr =
        ReliableConnection::kPostRecvBufferBatch;
    constexpr static size_t kReserveBufferSize =
        kReserveMessageNr * ReliableConnection::kMessageSize;

    constexpr static size_t kMessageSize = ReliableConnection::kMessageSize;
    static pointer ins(const DSMConfig &conf)
    {
        return std::make_unique<Patronus>(conf);
    }
    Patronus &operator=(const Patronus &) = delete;
    Patronus(const Patronus &) = delete;
    Patronus(const DSMConfig &conf);

    Lease get_rlease(uint16_t node_id,
                     id_t key,
                     term_t term,
                     CoroContext *ctx = nullptr);
    Lease get_wlease(uint16_t node_id,
                     id_t key,
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
              CoroContext *ctx = nullptr);
    void write(const Lease &,
               const char *ibuf,
               size_t size,
               size_t offset,
               CoroContext *ctx = nullptr);

    void registerThread()
    {
        dsm_->registerThread();
        auto *rdma_buffer = dsm_->get_rdma_buffer();
        rdma_buffers_ = std::make_unique<ThreadUnsafeBufferPool<kMessageSize>>(
            rdma_buffer, define::kRDMABufferSize);
    }
    size_t node_id() const
    {
        return dsm_->get_node_id();
    }
    size_t thread_id() const
    {
        return dsm_->get_thread_id();
    }

    void handle_messages(const char *msg_buf, size_t msg_nr, size_t dirID);

private:
    char *get_rdma_buffer()
    {
        return (char *) rdma_buffers_->get();
    }
    void put_rdma_buffer(char *buf)
    {
        rdma_buffers_->put(buf);
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

    void handle_acquire(AcquireRequest *, size_t dirID);

    DSM::pointer dsm_;

    static thread_local std::unique_ptr<ThreadUnsafeBufferPool<kMessageSize>>
        rdma_buffers_;
    static thread_local ThreadUnsafePool<RpcContext, kMaxCoroNr> rpc_context_;
};
}  // namespace patronus

#endif