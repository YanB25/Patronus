#pragma once
#ifndef PATRONUS_CORO_H_
#define PATRONUS_CORO_H_

#include <infiniband/verbs.h>

#include "Common.h"
#include "DSM.h"
#include "Pool.h"
#include "patronus/Config.h"
#include "patronus/Lease.h"
#include "patronus/LeaseContext.h"
#include "patronus/ProtectionRegion.h"
#include "patronus/Type.h"

namespace patronus
{
class Patronus;
struct ServerCoroTask
{
    DSM::msg_desc_t *msg_descs;
    size_t msg_nr{0};
    size_t fetched_nr{0};
    ssize_t active_coro_nr{0};
};

inline std::ostream &operator<<(std::ostream &os, const ServerCoroTask &task)
{
    os << "{Task: msg_descs: " << (void *) task.msg_descs
       << ", msg_nr: " << task.msg_nr << ", fetch_nr: " << task.fetched_nr
       << ", active_coro_nr: " << task.active_coro_nr << "}";
    return os;
}

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
    ThreadUnsafePool<
        ServerCoroTask,
        define::kMaxCoroNr *
            MAX_MACHINE * ::config::patronus::kClientThreadPerServerThread>
        task_pool;

    std::unique_ptr<LocalityBufferPool> msg_desc_pool;
    CoroControlBlock::pointer cb;
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
        bool do_nothing;
        patronus::LeaseContext *lease_ctx;
        bool with_dealloc;
        bool with_pr;
        uint64_t protection_region_id;
        ProtectionRegion *protection_region;
        bool with_conflict_detect;
        uint64_t key_bucket_id;
        uint64_t key_slot_id;
        uint64_t addr_to_bind;
        size_t buffer_size;
        uint64_t hint;
        uint32_t dir_id;
    } relinquish;
};

struct TaskDesc
{
    bool is_unbind;
    ibv_mw *mw;
    ibv_mr *mr;
    uint64_t bind_addr;
    size_t size;
    int access_flag;
    AcquireRequestStatus *o_status;
    ibv_mw **o_mw;
    TaskDesc(bool is_unbind,
             ibv_mw *mw,
             ibv_mr *mr,
             uint64_t bind_addr,
             size_t size,
             int access_flag,
             AcquireRequestStatus *o_status,
             ibv_mw **o_mw)
        : is_unbind(is_unbind),
          mw(mw),
          mr(mr),
          bind_addr(bind_addr),
          size(size),
          access_flag(access_flag),
          o_status(o_status),
          o_mw(o_mw)
    {
    }
};
inline std::ostream &operator<<(std::ostream &os, const TaskDesc &t)
{
    os << "{TaskDesc is_unbind: " << t.is_unbind << ", mw: " << t.mw
       << ", mr: " << t.mr << ", bind_addr: " << (void *) t.bind_addr
       << ", size: " << t.size << ", access_flag: " << t.access_flag
       << ", o_status: " << (void *) t.o_status << ", o_mw: " << (void *) t.o_mw
       << "}";
    return os;
}
class ServerCoroBatchExecutionContext
{
public:
    // buffer_mw & header_mw: the max ibv_post_send nr is twice the number of
    // message
    // The auto-relinquish experiments may also contain ones. So double again.
    constexpr static size_t V = ::config::verbose::kPatronusUtils;
    ServerCoroBatchExecutionContext()
    {
        prepared_tasks_.reserve(max_wr_size());
    }
    ~ServerCoroBatchExecutionContext();
    constexpr static size_t kBatchLimit =
        4 * config::patronus::kHandleRequestBatchLimit;
    static size_t batch_limit()
    {
        return kBatchLimit;
    }
    void clear();

    void init(Patronus *patronus, size_t dir_id);

    constexpr static size_t max_wr_size()
    {
        return kBatchLimit;
    }

    size_t req_size() const
    {
        return req_idx_;
    }
    void set_configure_reuse_mw_opt(bool val)
    {
        reuse_mw_opt_enabled_ = val;
    }
    bool get_configure_reuse_mw_opt() const
    {
        return reuse_mw_opt_enabled_;
    }

    /**
     * @brief prepare unbinding the memory window
     *
     * @param mr IN
     * @param bind_addr IN
     * @param size IN
     * @param access_flag IN
     */
    void prepare_unbind_mw(ibv_mw *mw,
                           ibv_mr *mr,
                           uint64_t bind_addr,
                           size_t size,
                           int access_flag)
    {
        prepared_tasks_.emplace_back(true /* is unbind */,
                                     mw,
                                     mr,
                                     bind_addr,
                                     size,
                                     access_flag,
                                     nullptr /* o_status */,
                                     nullptr /* o_mw */);
        DCHECK_EQ(size, 1) << "since it is unbind, should be size of 1";
        DCHECK_LE(prepared_tasks_.size(), max_wr_size());
    }
    void put_mw(ibv_mw *mw);

    /**
     * @brief prepare binding the memory window
     *
     * @param mr
     * @param bind_addr
     * @param size
     * @param access_flag
     * @param o_status OUT: the status of execution
     * @param o_mw OUT: the memory window bound
     */
    void prepare_bind_mw(ibv_mr *mr,
                         uint64_t bind_addr,
                         size_t size,
                         int access_flag,
                         AcquireRequestStatus *o_status,
                         ibv_mw **o_mw)
    {
        *DCHECK_NOTNULL(o_status) = AcquireRequestStatus::kSuccess;
        *DCHECK_NOTNULL(o_mw) = nullptr;
        prepared_tasks_.emplace_back(false /* is_unbind */,
                                     nullptr /* mw */,
                                     mr,
                                     bind_addr,
                                     size,
                                     access_flag,
                                     o_status,
                                     o_mw);
        DCHECK_LE(prepared_tasks_.size(), max_wr_size());
    }
    size_t fetch_req_idx()
    {
        auto ret = req_idx_;
        req_idx_++;
        DCHECK_LE(req_idx_, kBatchLimit) << "** overflow fetch_mw. " << *this;
        return ret;
    }
    HandleReqContext &req_ctx(size_t req_id)
    {
        DCHECK_LT(req_id, req_idx_);
        return req_ctx_[req_id];
    }
    bool commit(uint16_t prefix, uint64_t rw_ctx_id, WRID &o_wrid);

    size_t prepared_size() const
    {
        return prepared_tasks_.size();
    }
    size_t remain_size() const
    {
        return max_wr_size() - prepared_size();
    }

    friend std::ostream &operator<<(std::ostream &os,
                                    const ServerCoroBatchExecutionContext &ctx);

private:
    bool commit_wo_mw_reuse_optimization(uint16_t prefix,
                                         uint64_t rw_ctx_id,
                                         WRID &o_wrid);
    bool commit_with_mw_reuse_optimization(uint16_t prefix,
                                           uint64_t rw_ctx_id,
                                           WRID &o_wrid);

    Patronus *patronus_{nullptr};
    DSM::pointer dsm_;
    size_t dir_id_{0};

    std::tuple<ibv_qp *, size_t, size_t> get_qp_rr();

    std::vector<TaskDesc> prepared_tasks_{};

    size_t req_idx_{0};
    HandleReqContext req_ctx_[kBatchLimit]{};

    ibv_send_wr *bad_wr_;

    std::queue<ibv_mw *> free_mws_;

    static thread_local size_t rr_thread_idx_;
    static thread_local size_t rr_machine_idx_;

    // this one is shared among threads
    static thread_local uint64_t tl_per_qp_mw_post_idx_[MAX_MACHINE]
                                                       [kMaxAppThread];

    ibv_mw *shadow_mw_{nullptr};
    bool reuse_mw_opt_enabled_{true};
};

inline std::ostream &operator<<(std::ostream &os,
                                const ServerCoroBatchExecutionContext &ctx)
{
    os << "{ex_ctx: prepared_size: " << ctx.prepared_size()
       << ", req_size: " << ctx.req_size() << ", rr_tid: " << ctx.rr_thread_idx_
       << ", rr_nid: " << ctx.rr_machine_idx_ << "}";
    return os;
}

};  // namespace patronus
#endif