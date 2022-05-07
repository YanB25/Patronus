#include "patronus/Coro.h"

namespace patronus
{
thread_local uint64_t
    ServerCoroBatchExecutionContext::tl_per_qp_mw_post_idx_[MAX_MACHINE]
                                                           [kMaxAppThread]{};
thread_local size_t ServerCoroBatchExecutionContext::rr_thread_idx_{0};
thread_local size_t ServerCoroBatchExecutionContext::rr_machine_idx_{0};

ibv_qp *ServerCoroBatchExecutionContext::get_qp_rr()
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
    for (size_t i = 0; i < wr_idx_; ++i)
    {
        auto id = tl_per_qp_mw_post_idx_[rr_machine_idx_][rr_thread_idx_]++;

        id_per_qp[i] = id;
    }
    return dsm_->get_dir_qp(rr_machine_idx_, rr_thread_idx_, dir_id);
}
}  // namespace patronus