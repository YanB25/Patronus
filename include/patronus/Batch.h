#pragma once
#ifndef PATRONUS_BATCH_H_
#define PATRONUS_BATCH_H_

#include <Rdma.h>

#include <cinttypes>
#include <cstddef>

#include "CoroContext.h"
#include "util/RetCode.h"

namespace patronus
{
class Patronus;

enum class PatronusOp
{
    kWrite,
    kRead,
    kCas
};
class PatronusBatchContext
{
public:
    using node_t = uint32_t;
    using dir_t = uint32_t;
    constexpr static size_t kMaxOp = 16;
    constexpr static auto kNotNodeId = std::numeric_limits<node_t>::max();
    constexpr static auto kNotDirId = std::numeric_limits<dir_t>::max();
    bool empty() const
    {
        return idx_ == 0;
    }
    void fill_meta(ibv_qp *qp, uint32_t node_id, uint32_t dir_id)
    {
        if (unlikely(qp_ == nullptr))
        {
            qp_ = DCHECK_NOTNULL(qp);
        }
        else
        {
            DCHECK_EQ(qp_, qp) << "** inconsistent QP detected";
        }
        if (unlikely(!node_id_.has_value()))
        {
            node_id_ = node_id;
        }
        else
        {
            DCHECK_EQ(node_id_.value(), node_id)
                << "** inconsistent node_id detected";
        }
        if (unlikely(!dir_id_.has_value()))
        {
            dir_id_ = dir_id;
        }
        else
        {
            DCHECK_EQ(dir_id_.value(), dir_id)
                << "** inconsistent dir_id detected";
        }
    }
    RetCode prepare_write(ibv_qp *qp,
                          uint32_t node_id,
                          uint32_t dir_id,
                          uint64_t dest,
                          uint64_t source,
                          size_t size,
                          uint32_t lkey,
                          uint32_t rkey,
                          CoroContext *ctx)
    {
        std::ignore = ctx;
        if (unlikely(idx_ >= kMaxOp))
        {
            return kNoMem;
        }

        fill_meta(qp, node_id, dir_id);

        auto &sge = sges_[idx_];
        auto &wr = send_wrs_[idx_];
        fillSgeWr(sge, wr, source, size, lkey);
        wr.opcode = IBV_WR_RDMA_WRITE;
        wr.send_flags = 0;
        wr.wr.rdma.remote_addr = dest;
        wr.wr.rdma.rkey = rkey;

        idx_++;
        return kOk;
    }
    RetCode prepare_read(ibv_qp *qp,
                         uint32_t node_id,
                         uint32_t dir_id,
                         uint64_t source,
                         uint64_t dest,
                         size_t size,
                         uint32_t lkey,
                         uint32_t rkey,
                         CoroContext *ctx)
    {
        std::ignore = ctx;
        if (unlikely(idx_ >= kMaxOp))
        {
            return kNoMem;
        }

        fill_meta(qp, node_id, dir_id);

        auto &sge = sges_[idx_];
        auto &wr = send_wrs_[idx_];
        fillSgeWr(sge, wr, source, size, lkey);
        wr.opcode = IBV_WR_RDMA_READ;
        wr.send_flags = 0;
        wr.wr.rdma.remote_addr = dest;
        wr.wr.rdma.rkey = rkey;

        idx_++;
        return kOk;
    }
    RetCode prepare_cas(ibv_qp *qp,
                        uint32_t node_id,
                        uint32_t dir_id,
                        uint64_t dest,
                        uint64_t source,
                        uint64_t compare,
                        uint64_t swap,
                        uint32_t lkey,
                        uint32_t rkey,
                        CoroContext *ctx)
    {
        std::ignore = ctx;
        if (unlikely(idx_ >= kMaxOp))
        {
            return kNoMem;
        }

        fill_meta(qp, node_id, dir_id);

        auto &sge = sges_[idx_];
        auto &wr = send_wrs_[idx_];
        fillSgeWr(sge, wr, source, 8, lkey);
        wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        wr.send_flags = 0;
        wr.wr.atomic.remote_addr = dest;
        wr.wr.atomic.rkey = rkey;
        wr.wr.atomic.compare_add = compare;
        wr.wr.atomic.swap = swap;

        DCHECK_EQ((uint64_t) dest % 8, 0)
            << "** CAS addr should be 8-byte aligned. got " << (void *) dest;

        idx_++;
        return kOk;
    }
    RetCode commit(uint64_t wr_id, CoroContext *ctx)
    {
        DCHECK_LT(idx_, kMaxOp);
        for (size_t i = 0; i < idx_; ++i)
        {
            bool last = (i + 1) == idx_;
            send_wrs_[i].next = last ? nullptr : &send_wrs_[i + 1];

            if (last)
            {
                send_wrs_[i].send_flags |= IBV_SEND_SIGNALED;
            }

            send_wrs_[i].wr_id = wr_id;
        }

        auto ret = ibv_post_send(qp_, send_wrs_, &bad_wr_);
        if (unlikely(ret))
        {
            PLOG(ERROR) << "[patronus][batch] commit: failed to "
                           "ibv_post_send. bad_wr: wr_id: "
                        << WRID(bad_wr_->wr_id) << ", ret: " << ret;
            return RetCode::kRdmaExecutionErr;
        }
        CHECK_NOTNULL(ctx)->yield_to_master();

        clear();
        return RetCode::kOk;
    }
    dir_t dir_id() const
    {
        DCHECK(dir_id_.has_value());
        return dir_id_.value();
    }
    node_t node_id() const
    {
        DCHECK(node_id_.has_value());
        return node_id_.value();
    }

private:
    void clear()
    {
        qp_ = nullptr;
        node_id_ = std::nullopt;
        dir_id_ = std::nullopt;
        bad_wr_ = nullptr;
        idx_ = 0;
    }

    ibv_sge sges_[kMaxOp]{};
    ibv_send_wr send_wrs_[kMaxOp]{};
    ibv_send_wr *bad_wr_{nullptr};
    ibv_qp *qp_{nullptr};
    std::optional<node_t> node_id_;
    std::optional<dir_t> dir_id_;
    size_t idx_{0};
};
}  // namespace patronus

#endif