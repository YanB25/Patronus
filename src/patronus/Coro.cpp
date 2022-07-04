#include "patronus/Coro.h"

#include "patronus/All.h"

namespace patronus
{
thread_local uint64_t
    ServerCoroBatchExecutionContext::tl_per_qp_mw_post_idx_[MAX_MACHINE]
                                                           [kMaxAppThread]{};
thread_local size_t ServerCoroBatchExecutionContext::rr_thread_idx_{0};
thread_local size_t ServerCoroBatchExecutionContext::rr_machine_idx_{0};

std::tuple<ibv_qp *, size_t, size_t>
ServerCoroBatchExecutionContext::get_qp_rr()
{
    auto machine_nr = dsm_->getClusterSize();
    auto thread_nr = dsm_->get_icon_nr();
    rr_thread_idx_++;
    if (rr_thread_idx_ >= thread_nr)
    {
        rr_thread_idx_ = 0;
        rr_machine_idx_ = (rr_machine_idx_ + 1) % machine_nr;
    }
    DCHECK_LT(rr_thread_idx_, thread_nr);
    DCHECK_LT(rr_machine_idx_, machine_nr);
    auto dir_id = dsm_->get_thread_id();

    return {dsm_->get_dir_qp(rr_machine_idx_, rr_thread_idx_, dir_id),
            rr_thread_idx_,
            rr_machine_idx_};
}

bool ServerCoroBatchExecutionContext::commit(uint16_t prefix,
                                             uint64_t rw_ctx_id,
                                             WRID &o_wrid)
{
    if (unlikely(prepared_tasks_.empty()))
    {
        return false;
    }
    DCHECK_LT(rw_ctx_id, std::numeric_limits<uint32_t>::max());

    bool ret = true;
    if (reuse_mw_opt_enabled_)
    {
        ret = commit_with_mw_reuse_optimization(prefix, rw_ctx_id, o_wrid);
    }
    else
    {
        ret = commit_wo_mw_reuse_optimization(prefix, rw_ctx_id, o_wrid);
    }

    while (!free_mws_.empty())
    {
        auto *mw = free_mws_.front();
        free_mws_.pop();
        patronus_->put_mw(dir_id_, mw);
    }

    prepared_tasks_.clear();
    return ret;
}
void ServerCoroBatchExecutionContext::clear()
{
    prepared_tasks_.clear();
    memset(req_ctx_, 0, sizeof(HandleReqContext) * req_idx_);
    req_idx_ = 0;
}

void ServerCoroBatchExecutionContext::init(Patronus *patronus, size_t dir_id)
{
    patronus_ = patronus;
    dsm_ = patronus->get_dsm();
    dir_id_ = dir_id;
    shadow_mw_ = patronus->get_mw(dir_id);
}
ServerCoroBatchExecutionContext::~ServerCoroBatchExecutionContext()
{
    patronus_->put_mw(dir_id_, shadow_mw_);
}
void ServerCoroBatchExecutionContext::put_mw(ibv_mw *mw)
{
    if (mw)
    {
        patronus_->put_mw(dir_id_, mw);
    }
}
bool ServerCoroBatchExecutionContext::commit_wo_mw_reuse_optimization(
    uint16_t prefix, uint64_t rw_ctx_id, WRID &o_wrid)
{
    WRID signal_wrid{WRID_PREFIX_NULLWRID, get_WRID_ID_RESERVED()};

    static thread_local ibv_send_wr wrs[max_wr_size()];
    static thread_local size_t wr_idx_to_task_idx[max_wr_size()];
    size_t post_wr_nr = 0;

    // LOG(INFO) << "[debug] handling prepared_tasks with size "
    //           << prepared_tasks_.size();
    // one round
    for (size_t task_idx = 0; task_idx < prepared_tasks_.size(); ++task_idx)
    {
        auto &task = prepared_tasks_[task_idx];
        auto idx = post_wr_nr++;
        DCHECK_LT(idx, max_wr_size());
        auto &wr = wrs[idx];
        if constexpr (debug())
        {
            if (!task.is_unbind)
            {
                CHECK_EQ(task.mw, nullptr);
            }
        }
        ibv_mw *mw = nullptr;
        if (task.is_unbind)
        {
            DCHECK_EQ(task.o_mw, nullptr);
            DCHECK_EQ(task.o_status, nullptr);
            mw = task.mw;
            free_mws_.push(task.mw);
        }
        else
        {
            mw = patronus_->get_mw(dir_id_);
            DCHECK_NE(task.o_mw, nullptr);
            if (unlikely(mw == nullptr))
            {
                if (task.o_status)
                {
                    *task.o_status = AcquireRequestStatus::kNoMw;
                }
                // equivalent to do nothing
                mw = shadow_mw_;
            }
            *task.o_mw = mw;
        }
        fill_bind_mw_wr(
            wr, mw, task.mr, task.bind_addr, task.size, task.access_flag);
        wr_idx_to_task_idx[idx] = task_idx;
    }
    // handle signals
    DCHECK_EQ(post_wr_nr, prepared_tasks_.size());
    for (size_t i = 0; i < post_wr_nr; ++i)
    {
        bool last = (i + 1 == post_wr_nr);
        bool should_signal = last;
        auto &wr = wrs[i];
        wr.wr_id = WRID(prefix, !!should_signal, rw_ctx_id).val;
        if (last)
        {
            wr.next = nullptr;
            wr.send_flags |= IBV_SEND_SIGNALED;
            signal_wrid = wr.wr_id;
            o_wrid = wr.wr_id;
        }
        else
        {
            wr.next = &wrs[i + 1];
            DCHECK_LT(i + 1, max_wr_size());
            DCHECK(!(wr.send_flags & IBV_SEND_SIGNALED));
        }
    }

    // now check for magic mw error

    auto [qp, thread_idx, machine_idx] = get_qp_rr();
    for (size_t wr_idx = 0; wr_idx < post_wr_nr; ++wr_idx)
    {
        auto id = tl_per_qp_mw_post_idx_[machine_idx][thread_idx]++;
        if (unlikely(is_mw_magic_err(id)))
        {
            DCHECK_LT(wr_idx, max_wr_size());
            auto task_idx = wr_idx_to_task_idx[wr_idx];
            DCHECK_EQ(task_idx, wr_idx)
                << "Currently, one task will become one wr, so there index "
                   "should be the same. i.e. one-to-one mapping";
            DCHECK_LT(task_idx, prepared_tasks_.size());
            const auto &task = prepared_tasks_[task_idx];
            if (task.o_status)
            {
                (*task.o_status) = AcquireRequestStatus::kMagicMwErr;
            }
        }
    }

    auto ret = ibv_post_send(qp, wrs, &bad_wr_);
    if (unlikely(ret != 0))
    {
        CHECK(false) << "[patronus] failed to ibv_post_send for bind_mw. "
                        "failed wr : "
                     << WRID(bad_wr_->wr_id)
                     << ". prefix: " << pre_wrid_prefix(prefix)
                     << ", rw_ctx_id: " << rw_ctx_id
                     << ", signal_wrid: " << signal_wrid
                     << ", posted: " << post_wr_nr << ", req_nr: " << req_idx_
                     << ", posting to QP[" << rr_machine_idx_ << "]["
                     << rr_thread_idx_ << "]";

        for (size_t i = 0; i < req_idx_; ++i)
        {
            req_ctx_[i].status = AcquireRequestStatus::kBindErr;
        }
    }
    DVLOG(V) << "[patronus] batch commit with prefix "
             << pre_wrid_prefix(prefix)
             << ", rw_ctx_id: " << (uint64_t) rw_ctx_id
             << ", post size: " << post_wr_nr << ", request size " << req_idx_
             << ", rr_tid: " << rr_thread_idx_
             << ", rr_nid: " << rr_machine_idx_
             << ". signaled wrid: " << signal_wrid;
    return true;
}

bool ServerCoroBatchExecutionContext::commit_with_mw_reuse_optimization(
    uint16_t prefix, uint64_t rw_ctx_id, WRID &o_wrid)
{
    WRID signal_wrid{WRID_PREFIX_NULLWRID, get_WRID_ID_RESERVED()};

    static thread_local ibv_send_wr wrs[max_wr_size()];
    static thread_local size_t wr_idx_to_task_idx[max_wr_size()];
    // first round for the unbinds
    size_t unbind_wr_idx = 0;

    for (size_t task_idx = 0; task_idx < prepared_tasks_.size(); ++task_idx)
    {
        const auto &task = prepared_tasks_[task_idx];
        if (!task.is_unbind)
        {
            continue;
        }
        auto idx = unbind_wr_idx++;
        DCHECK_LT(idx, max_wr_size());
        auto &wr = wrs[idx];
        fill_bind_mw_wr(
            wr, task.mw, task.mr, task.bind_addr, task.size, task.access_flag);
        free_mws_.push(DCHECK_NOTNULL(task.mw));
        wr_idx_to_task_idx[idx] = task_idx;
    }
    // second round for the binds
    size_t bind_wr_idx = 0;
    for (size_t task_idx = 0; task_idx < prepared_tasks_.size(); ++task_idx)
    {
        const auto &task = prepared_tasks_[task_idx];
        if (task.is_unbind)
        {
            continue;
        }
        auto idx = bind_wr_idx++;
        DCHECK_LT(idx, max_wr_size());
        auto &wr = wrs[idx];
        wr_idx_to_task_idx[idx] = task_idx;
        if (idx < unbind_wr_idx)
        {
            // okay, here we reuse the unbind.
            DCHECK(!free_mws_.empty());
            auto *reuse_mw = free_mws_.front();
            free_mws_.pop();
            *task.o_mw = reuse_mw;

            fill_bind_mw_wr(wr,
                            reuse_mw,
                            task.mr,
                            task.bind_addr,
                            task.size,
                            task.access_flag);
        }
        else
        {
            // unable to reuse
            DCHECK_EQ(task.mw, nullptr);
            DCHECK(free_mws_.empty());
            auto *mw = DCHECK_NOTNULL(patronus_->get_mw(dir_id_));
            if (unlikely(mw == nullptr))
            {
                mw = shadow_mw_;
                if (task.o_status)
                {
                    *task.o_status = AcquireRequestStatus::kNoMw;
                }
            }
            *task.o_mw = mw;
            fill_bind_mw_wr(
                wr, mw, task.mr, task.bind_addr, task.size, task.access_flag);
        }

        if constexpr (debug())
        {
            CHECK_EQ(task.mw, nullptr);
        }
    }

    // now handle the signal, wr_id and the next pointers
    auto post_wr_nr = std::max(unbind_wr_idx, bind_wr_idx);
    DCHECK_LE(post_wr_nr, prepared_tasks_.size())
        << "at least it will not be larger";
    for (size_t i = 0; i < post_wr_nr; ++i)
    {
        bool last = (i + 1 == post_wr_nr);
        bool should_signal = last;
        auto &wr = wrs[i];
        wr.wr_id = WRID(prefix, !!should_signal, rw_ctx_id).val;
        if (last)
        {
            wr.next = nullptr;
            wr.send_flags |= IBV_SEND_SIGNALED;
            signal_wrid = wr.wr_id;
            o_wrid = wr.wr_id;
        }
        else
        {
            wr.next = &wrs[i + 1];
            DCHECK_LT(i + 1, max_wr_size());
            DCHECK(!(wr.send_flags & IBV_SEND_SIGNALED));
        }
    }

    auto [qp, thread_idx, machine_idx] = get_qp_rr();

    // handle magic error
    if constexpr (::config::kEnableSkipMagicMw)
    {
        for (size_t wr_idx = 0; wr_idx < post_wr_nr; ++wr_idx)
        {
            auto id = tl_per_qp_mw_post_idx_[machine_idx][thread_idx]++;
            if (unlikely(is_mw_magic_err(id)))
            {
                DCHECK_LT(wr_idx, max_wr_size());
                auto task_idx = wr_idx_to_task_idx[wr_idx];
                DCHECK_LT(task_idx, prepared_tasks_.size());
                const auto &task = prepared_tasks_[task_idx];
                if (task.o_status)
                {
                    (*task.o_status) = AcquireRequestStatus::kMagicMwErr;
                }
            }
        }
    }

    auto ret = ibv_post_send(qp, wrs, &bad_wr_);
    if (unlikely(ret != 0))
    {
        CHECK(false) << "[patronus] failed to ibv_post_send for bind_mw. "
                        "failed wr : "
                     << WRID(bad_wr_->wr_id)
                     << ". prefix: " << pre_wrid_prefix(prefix)
                     << ", rw_ctx_id: " << rw_ctx_id
                     << ", signal_wrid: " << signal_wrid
                     << ", posted: " << post_wr_nr
                     << "(unbind: " << unbind_wr_idx
                     << ", bind: " << bind_wr_idx << ")"
                     << ", req_nr: " << req_idx_ << ", posting to QP["
                     << rr_machine_idx_ << "][" << rr_thread_idx_ << "]";

        for (size_t i = 0; i < req_idx_; ++i)
        {
            req_ctx_[i].status = AcquireRequestStatus::kBindErr;
        }
    }
    DVLOG(V) << "[patronus] batch commit with prefix "
             << pre_wrid_prefix(prefix)
             << ", rw_ctx_id: " << (uint64_t) rw_ctx_id
             << ", post size: " << post_wr_nr << "(unbind_nr: " << unbind_wr_idx
             << ", bind_nr: " << bind_wr_idx << ")"
             << ", request size " << req_idx_ << ", rr_tid: " << rr_thread_idx_
             << ", rr_nid: " << rr_machine_idx_
             << ". signaled wrid: " << signal_wrid;

    return true;
}

}  // namespace patronus