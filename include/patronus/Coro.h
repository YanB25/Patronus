#pragma once
#ifndef PATRONUS_CORO_H_
#define PATRONUS_CORO_H_

#include <infiniband/verbs.h>

#include "Common.h"
#include "DSM.h"
#include "Pool.h"
#include "patronus/Lease.h"
#include "patronus/LeaseContext.h"
#include "patronus/ProtectionRegion.h"
#include "patronus/Type.h"

namespace patronus
{
struct ServerCoroTask
{
    const char *buf{nullptr};  // msg buffer
    size_t msg_nr{0};
};

struct ServerCoroCommunication
{
    bool finished[define::kMaxCoroNr];
    std::queue<ServerCoroTask *> task_queue;
};

struct ServerCoroContext
{
    CoroCall server_workers[define::kMaxCoroNr];
    CoroCall server_master;
    ServerCoroCommunication comm;
    ThreadUnsafePool<ServerCoroTask, define::kMaxCoroNr * MAX_MACHINE>
        task_pool;
    std::unique_ptr<ThreadUnsafeBufferPool<config::umsg::kMaxRecvBuffer>>
        buffer_pool;
};

struct HandleReqContext
{
    patronus::AcquireRequestStatus status{
        patronus::AcquireRequestStatus::kReserved};
    uint64_t lease_id{0};
    struct
    {
        ibv_mw *buffer_mw;
        ibv_mw *header_mw;
        ibv_mr *buffer_mr;
        ibv_mr *header_mr;
        uint64_t object_addr;
        uint64_t protection_region_id;
        uint64_t bucket_id;
        uint64_t slot_id;
        uint64_t object_dsm_offset;
        uint64_t header_dsm_offset;
        patronus::ProtectionRegion *protection_region;
        bool with_conflict_detect;
        bool alloc_lease_ctx;
        bool bind_pr;
        bool bind_buf;
        bool alloc_pr;
        bool use_mr;
        bool with_alloc;
        bool only_alloc;
        bool with_lock;
        bool i_acquire_the_lock;
    } acquire;
    struct
    {
        patronus::LeaseContext *lease_ctx;
        bool with_dealloc;
    } relinquish;
    struct
    {
        std::optional<size_t> buffer_wr_idx;
        std::optional<size_t> header_wr_idx;
    } meta;
};

class ServerCoroBatchExecutionContext
{
public:
    constexpr static size_t kWrNr = 2 * config::umsg::kRecvLimit;

    size_t fetch_wr()
    {
        auto ret = wr_idx_;
        wr_idx_++;
        return ret;
    }
    ibv_send_wr &wr(size_t wr_id)
    {
        DCHECK_LT(wr_id, wr_idx_);
        return wrs_[wr_id];
    }
    void clear()
    {
        memset(wrs_, 0, sizeof(ibv_send_wr) * wr_idx_);
        memset(req_ctx_, 0, sizeof(HandleReqContext) * req_idx_);
        wr_idx_ = 0;
        req_idx_ = 0;
        success_ = false;
    }
    void init(DSM::pointer dsm)
    {
        dsm_ = dsm;
    }
    bool &success()
    {
        return success_;
    }
    size_t fetch_req_idx()
    {
        auto ret = req_idx_;
        req_idx_++;
        return ret;
    }
    HandleReqContext &req_ctx(size_t req_id)
    {
        DCHECK_LT(req_id, req_idx_);
        return req_ctx_[req_id];
    }
    bool commit(uint16_t prefix, uint64_t rw_ctx_id)
    {
        if (unlikely(wr_idx_ == 0))
        {
            return false;
        }
        DCHECK_LT(rw_ctx_id, std::numeric_limits<uint32_t>::max());

        WRID signal_wrid{WRID_PREFIX_RESERVED_1, get_WRID_ID_RESERVED()};
        for (size_t i = 0; i < wr_idx_; ++i)
        {
            bool last = (i + 1 == wr_idx_);
            bool should_signal = last;
            auto &wr = wrs_[i];
            wr.wr_id = WRID(prefix, !!should_signal, rw_ctx_id).val;
            if (last)
            {
                wr.next = nullptr;
                wr.send_flags |= IBV_SEND_SIGNALED;
                signal_wrid = wr.wr_id;
            }
            else
            {
                wr.next = &wrs_[i + 1];
                DCHECK(!(wr.send_flags & IBV_SEND_SIGNALED));
            }
        }

        LOG(INFO) << "[debug] !! posting to QP[" << rr_machine_idx_ << "]["
                  << rr_thread_idx_ << "]. posted: " << wr_idx_
                  << ", req_nr: " << req_idx_;

        auto *qp = get_qp_rr();
        auto ret = ibv_post_send(qp, wrs_, &bad_wr_);
        if (unlikely(ret != 0))
        {
            CHECK(false) << "[patronus] failed to ibv_post_send for bind_mw. "
                            "failed wr : "
                         << WRID(bad_wr_->wr_id)
                         << ". prefix: " << pre_wrid_prefix(prefix)
                         << ", rw_ctx_id: " << rw_ctx_id
                         << ", signal_wrid: " << signal_wrid
                         << ", posted: " << wr_idx_ << ", req_nr: " << req_idx_
                         << ", posting to QP[" << rr_machine_idx_ << "]["
                         << rr_thread_idx_ << "]";

            for (size_t i = 0; i < req_idx_; ++i)
            {
                req_ctx_[i].status = AcquireRequestStatus::kBindErr;
            }
        }
        DVLOG(4) << "[patronus] batch commit with prefix "
                 << pre_wrid_prefix(prefix)
                 << ", rw_ctx_id: " << (uint64_t) rw_ctx_id
                 << ", post size: " << wr_idx_ << ", request size " << req_idx_
                 << ", rr_tid: " << rr_thread_idx_
                 << ", rr_nid: " << rr_machine_idx_
                 << ". signaled wrid: " << signal_wrid;
        return true;
    }
    ibv_qp *get_qp_rr();
    size_t wr_post_idx(size_t wr_idx) const
    {
        DCHECK_LT(wr_idx, wr_idx_);
        return id_per_qp[wr_idx];
    }

private:
    size_t wr_idx_{0};
    ibv_send_wr wrs_[kWrNr]{};
    uint64_t id_per_qp[kWrNr]{};
    size_t req_idx_{0};
    HandleReqContext req_ctx_[kWrNr]{};
    bool success_{false};

    DSM::pointer dsm_;
    ibv_send_wr *bad_wr_;

    static thread_local size_t rr_thread_idx_;
    static thread_local size_t rr_machine_idx_;

    // this one is shared among threads
    static thread_local uint64_t tl_per_qp_mw_post_idx_[MAX_MACHINE]
                                                       [kMaxAppThread];
};

};  // namespace patronus
#endif