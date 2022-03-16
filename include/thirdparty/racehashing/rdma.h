#pragma once
#ifndef PERTRONUS_RACEHASHING_RDMA_H_
#define PERTRONUS_RACEHASHING_RDMA_H_

#include <cinttypes>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <vector>

#include "./utils.h"

namespace patronus::hash
{
struct RdmaContextOp
{
    uint64_t remote;
    void *buffer;
    int op;  // 0 for read, 1 for write, 2 for cas
    size_t size;
    uint64_t *expect;
    uint64_t desired;
    bool *success;
};
class RaceHashingRdmaContext
{
public:
    using pointer = std::unique_ptr<RaceHashingRdmaContext>;
    RaceHashingRdmaContext() = default;
    RaceHashingRdmaContext(const RaceHashingRdmaContext &) = delete;
    RaceHashingRdmaContext &operator=(const RaceHashingRdmaContext &) = delete;

    static pointer new_instance()
    {
        return std::make_unique<RaceHashingRdmaContext>();
    }

    void *get_rdma_buffer(size_t size)
    {
        void *ret = malloc(size);
        allocated_buffers_.insert(ret);
        // LOG(INFO) << "Allocating " << (void *) ret;
        return ret;
    }
    RetCode rdma_read(uint64_t addr, char *rdma_buf, size_t size)
    {
        RdmaContextOp op;
        op.remote = addr;
        op.buffer = rdma_buf;
        op.op = 0;
        op.size = size;
        op.success = nullptr;
        ops_.emplace_back(std::move(op));
        // LOG(INFO) << "Registering rdma_buf: " << (void *) rdma_buf;
        return kOk;
    }
    RetCode rdma_write(uint64_t addr, const char *rdma_buf, size_t size)
    {
        // memcpy((char *) addr, rdma_buf, size);
        RdmaContextOp op;
        op.remote = addr;
        op.buffer = (char *) rdma_buf;
        op.op = 1;
        op.size = size;
        op.success = nullptr;
        ops_.emplace_back(std::move(op));
        return kOk;
    }
    RetCode rdma_cas(uint64_t addr,
                     uint64_t &expect,
                     uint64_t desired,
                     bool *success)
    {
        // return kOk;
        RdmaContextOp op;
        op.remote = addr;
        op.buffer = nullptr;
        op.expect = &expect;
        op.desired = desired;
        op.size = 8;
        op.success = success;
        ops_.emplace_back(std::move(op));
        return kOk;
    }
    ~RaceHashingRdmaContext()
    {
        for (auto *buf : allocated_buffers_)
        {
            free(buf);
        }
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
                std::atomic<uint64_t> &atm =
                    *(std::atomic<uint64_t> *) op.remote;
                if (atm.compare_exchange_strong(*op.expect, op.desired))
                {
                    (*op.success) = true;
                }
                (*op.success) = false;
            }
            else
            {
                CHECK(false) << "Unknown op: " << op.op;
            }
        }
        ops_.clear();
        for (void *addr : allocated_buffers_)
        {
            free(addr);
        }
        allocated_buffers_.clear();

        return kOk;
    }

private:
    // void put_rmda_buffer(void *buf)
    // {
    //     DCHECK_EQ(allocated_buffers_.count(buf), 1);
    //     DCHECK_EQ(allocated_buffers_.erase(buf), 1);
    //     free(buf);
    // }
    std::vector<RdmaContextOp> ops_;
    std::unordered_set<void *> allocated_buffers_;
};

}  // namespace patronus::hash

#endif