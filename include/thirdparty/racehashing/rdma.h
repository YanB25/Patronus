#pragma once
#ifndef PERTRONUS_RACEHASHING_RDMA_H_
#define PERTRONUS_RACEHASHING_RDMA_H_

#include <cinttypes>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <vector>

#include "./utils.h"
#include "Common.h"

namespace patronus::hash
{
struct RdmaContextOp
{
    uint64_t remote;
    void *buffer;
    int op;  // 0 for read, 1 for write, 2 for cas
    size_t size;
    uint64_t expect;
    uint64_t desired;
};
inline std::ostream &operator<<(std::ostream &os, const RdmaContextOp &op)
{
    os << "{op: remote: " << (void *) op.remote << ", buffer: " << op.buffer
       << ", op: " << op.op << ", size: " << op.size
       << ", expect: " << op.expect << ", desired: " << op.desired << "}";
    return os;
}

class RaceHashingRdmaContext
{
public:
    using pointer = std::shared_ptr<RaceHashingRdmaContext>;
    RaceHashingRdmaContext(HashContext *dctx = nullptr) : dctx_(dctx)
    {
    }
    RaceHashingRdmaContext(const RaceHashingRdmaContext &) = delete;
    RaceHashingRdmaContext &operator=(const RaceHashingRdmaContext &) = delete;

    static pointer new_instance(HashContext *dctx = nullptr)
    {
        return std::make_shared<RaceHashingRdmaContext>(dctx);
    }
    void *remote_alloc(size_t size)
    {
        auto *ret = malloc(size);
        remote_allocated_buffers_.insert(ret);
        DLOG_IF(INFO, config::kEnableDebug && dctx_ != nullptr)
            << "[rdma][trace] remote_alloc: " << (void *) ret << " for size "
            << size;
        return ret;
    }
    void remote_free(void *addr)
    {
        CHECK_EQ(remote_allocated_buffers_.count(addr), 1)
            << "Addr " << (void *) addr << " not allocated from remote buffer.";
        remote_allocated_buffers_.erase(addr);
        DLOG_IF(INFO, config::kEnableDebug && dctx_ != nullptr)
            << "[rdma][trace] remote_free: " << (void *) addr;
        free(addr);
    }

    void *get_rdma_buffer(size_t size)
    {
        DCHECK_GT(size, 0) << "Make no sense to alloc size with 0";
        void *ret = malloc(size);
        allocated_buffers_.insert(ret);
        DLOG_IF(INFO, config::kEnableMemoryDebug && dctx_ != nullptr)
            << "[rdma][trace] get_rdma_buffer: " << (void *) ret << " for size "
            << size;
        return ret;
    }
    RetCode rdma_read(uint64_t addr, char *rdma_buf, size_t size)
    {
        addr = adapt_to_remote_addr(addr);
        RdmaContextOp op;
        op.remote = addr;
        op.buffer = rdma_buf;
        op.op = 0;
        op.size = size;

        // DLOG_IF(INFO, config::kEnableMemoryDebug)
        //     << "[race] rdma_read: op.remote: " << (void *) op.remote
        //     << ", op.buffer: " << op.buffer << ", size: " << op.size;

        ops_.emplace_back(std::move(op));
        return kOk;
    }
    RetCode rdma_write(uint64_t addr, const char *rdma_buf, size_t size)
    {
        addr = adapt_to_remote_addr(addr);
        RdmaContextOp op;
        op.remote = addr;
        op.buffer = (char *) rdma_buf;
        op.op = 1;
        op.size = size;
        ops_.emplace_back(std::move(op));
        return kOk;
    }
    RetCode rdma_cas(uint64_t addr,
                     uint64_t expect,
                     uint64_t desired,
                     void *rdma_buf)
    {
        addr = adapt_to_remote_addr(addr);
        RdmaContextOp op;
        op.op = 2;
        op.remote = addr;
        op.buffer = (char *) rdma_buf;
        op.expect = expect;
        op.desired = desired;
        op.size = 8;
        ops_.emplace_back(std::move(op));
        return kOk;
    }
    ~RaceHashingRdmaContext()
    {
        for (auto *buf : allocated_buffers_)
        {
            free(buf);
        }
        LOG_IF(WARNING, remote_allocated_buffers_.size() != 0)
            << "[race][rdma] Possible memory leak. Out-going nr: "
            << remote_allocated_buffers_.size();
    }

    RetCode commit()
    {
        for (const auto &op : ops_)
        {
            DCHECK_EQ(allocated_buffers_.count((void *) op.buffer), 1)
                << "Buffer not allocated from this context. buffer: "
                << (void *) op.buffer;
            if (op.op == 0)
            {
                memcpy(op.buffer, (char *) op.remote, op.size);
            }
            else if (op.op == 1)
            {
                memcpy((char *) op.remote, op.buffer, op.size);
            }
            else if (op.op == 2)
            {
                uint64_t expect = op.expect;
                std::atomic<uint64_t> &atm =
                    *(std::atomic<uint64_t> *) op.remote;
                atm.compare_exchange_strong(expect, op.desired);
                DCHECK_EQ(op.size, 8);
                memcpy(op.buffer, (char *) &expect, op.size);
            }
            else
            {
                CHECK(false) << "Unknown op: " << op.op;
            }
        }
        ops_.clear();

        return kOk;
    }

    void gc()
    {
        for (void *addr : allocated_buffers_)
        {
            free(addr);
            DLOG_IF(INFO, config::kEnableDebug && dctx_ != nullptr)
                << "[rdma][trace] gc: freeing " << (void *) addr;
        }
        allocated_buffers_.clear();
    }

private:
    HashContext *dctx_{nullptr};
    /**
     * @brief sometimes, the addr from client should be transformed before
     * handling to RDMA.
     */
    uint64_t adapt_to_remote_addr(uint64_t addr) const
    {
        return addr;
    }
    // void put_rmda_buffer(void *buf)
    // {
    //     DCHECK_EQ(allocated_buffers_.count(buf), 1);
    //     DCHECK_EQ(allocated_buffers_.erase(buf), 1);
    //     free(buf);
    // }
    std::vector<RdmaContextOp> ops_;
    std::unordered_set<void *> allocated_buffers_;
    std::unordered_set<void *> remote_allocated_buffers_;
};

}  // namespace patronus::hash

#endif