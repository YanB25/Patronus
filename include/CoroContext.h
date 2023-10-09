#pragma once
#ifndef SHERMEM_CORO_CONTEXT_H_
#define SHERMEM_CORO_CONTEXT_H_

#include "Common.h"
#include "util/Coro.h"

class CoroControlBlock
{
public:
    using pointer = std::shared_ptr<CoroControlBlock>;
    CoroControlBlock(size_t coro_nr)
    {
        worker_wait_wr_ids_.resize(coro_nr, nullwrid);
        worker_is_waiting_.resize(coro_nr, true);
    }
    static pointer new_instance(size_t coro_nr)
    {
        return std::make_shared<CoroControlBlock>(coro_nr);
    }

    void yield_to_master(size_t coro_id, WRID wr_id)
    {
        CHECK_LT(coro_id, worker_is_waiting_.size());
        CHECK(!worker_is_waiting_[coro_id]);
        worker_is_waiting_[coro_id] = true;

        CHECK_LT(coro_id, worker_wait_wr_ids_.size());
        worker_wait_wr_ids_[coro_id] = wr_id;
    }

    void yield_to_worker(size_t coro_id, WRID wr_id)
    {
        CHECK_LT(coro_id, worker_is_waiting_.size());
        CHECK(worker_is_waiting_[coro_id]);
        worker_is_waiting_[coro_id] = false;

        CHECK_LT(coro_id, worker_wait_wr_ids_.size());
        CHECK_EQ(worker_wait_wr_ids_[coro_id], wr_id);
        worker_wait_wr_ids_[coro_id] = nullwrid;
    }

private:
    std::vector<WRID> worker_wait_wr_ids_;
    std::vector<bool> worker_is_waiting_;
};

struct CoroContext
{
public:
    CoroContext(size_t thread_id,
                CoroYield *yield,
                CoroCall *master,
                coro_t coro_id,
                CoroControlBlock::pointer cb = {})
        : thread_id_(thread_id),
          yield_(yield),
          master_(master),
          coro_id_(coro_id),
          cb_(cb)
    {
        CHECK_NE(coro_id, kMasterCoro) << "** This coro should not be a master";
        CHECK_NE(coro_id, kNotACoro) << "** This coro should not be nullctx";
    }
    CoroContext(size_t thread_id,
                CoroYield *yield,
                CoroCall *workers,
                CoroControlBlock::pointer cb = {})
        : thread_id_(thread_id), yield_(yield), workers_(workers), cb_(cb)
    {
        coro_id_ = kMasterCoro;
    }
    CoroContext(const CoroContext &) = delete;
    CoroContext &operator=(const CoroContext &) = delete;

    CoroContext() : coro_id_(kNotACoro)
    {
    }
    bool is_master() const
    {
        return coro_id_ == kMasterCoro;
    }
    bool is_worker() const
    {
        return coro_id_ != kMasterCoro && coro_id_ != kNotACoro;
    }
    bool is_nullctx() const
    {
        return coro_id_ == kNotACoro;
    }
    coro_t coro_id() const
    {
        return coro_id_;
    }
    size_t thread_id() const
    {
        return thread_id_;
    }

    void yield_to_master(WRID wr_id = nullwrid) const
    {
        DLOG_IF(INFO, ::config::kMonitorCoroSwitch)
            << "[Coro] " << *this << " yielding to master for " << wr_id;
        DCHECK(is_worker()) << *this;

        if constexpr (debug())
        {
            if (cb_)
            {
                cb_->yield_to_master(coro_id_, wr_id);
            }
        }

        (*yield_)(*master_);

        DLOG_IF(INFO, ::config::kMonitorCoroSwitch)
            << "[Coro] " << *this << " comming back from master, expecting "
            << wr_id;
    }
    void yield_to_worker(coro_t wid, WRID wr_id = nullwrid)
    {
        DLOG_IF(INFO, ::config::kMonitorCoroSwitch)
            << "[Coro] " << *this << " yielding to worker " << (int) wid
            << " for " << wr_id;
        DCHECK(is_master()) << *this;

        if constexpr (debug())
        {
            if (cb_)
            {
                cb_->yield_to_worker(wid, wr_id);
            }
        }

        (*yield_)(workers_[wid]);

        DLOG_IF(INFO, ::config::kMonitorCoroSwitch)
            << "[Coro] " << *this << " yielding back from worker " << (int) wid;
    }
    friend std::ostream &operator<<(std::ostream &os, const CoroContext &ctx);

    void set_trace(trace_t trace)
    {
        trace_ = trace;
    }
    trace_t trace() const
    {
        return trace_;
    }
    ContTimer<::config::kEnableTrace> &timer()
    {
        return timer_;
    }

private:
    size_t thread_id_{0};
    CoroYield *yield_{nullptr};
    CoroCall *master_{nullptr};
    CoroCall *workers_{nullptr};
    coro_t coro_id_{kNotACoro};
    CoroControlBlock::pointer cb_{};

    trace_t trace_{0};
    ContTimer<::config::kEnableTrace> timer_;

    WRID wait_wrid_{nullwrid};
};

static CoroContext nullctx;

inline std::ostream &operator<<(std::ostream &os, const CoroContext &ctx)
{
    if (ctx.coro_id_ == kMasterCoro)
    {
        os << "{Coro T(" << ctx.thread_id_ << ") Master }";
    }
    else if (ctx.coro_id_ == kNotACoro)
    {
        os << "{Coro Not a coro}";
    }
    else
    {
        os << "{Coro T(" << ctx.thread_id_ << ") " << (int) ctx.coro_id_ << "}";
    }
    return os;
}

struct Void
{
};

template <size_t kCoroCnt_, typename T>
class CoroExecutionContextWith
{
public:
    constexpr static size_t kCoroCnt = kCoroCnt_;

    void worker_finished(size_t coro_id)
    {
        finish_all_[coro_id] = true;
    }
    bool is_finished_all() const
    {
        return std::all_of(
            finish_all_.begin(), finish_all_.end(), [](bool i) { return i; });
    }
    bool is_finished(size_t wid) const
    {
        DCHECK_LT(wid, kCoroCnt);
        return finish_all_[wid];
    }
    CoroCall *workers()
    {
        return workers_.data();
    }
    const CoroCall *workers() const
    {
        return workers_.data();
    }
    CoroCall *master() const
    {
        return master_;
    }
    CoroCall &worker(size_t wid)
    {
        CHECK_LT(wid, kCoroCnt);
        return workers_[wid];
    }
    CoroCall &master()
    {
        return master_;
    }
    T &get_private_data()
    {
        return t_;
    }
    const T &get_private_data() const
    {
        return t_;
    }

private:
    std::array<CoroCall, kCoroCnt> workers_{};
    CoroCall master_;
    std::array<bool, kCoroCnt> finish_all_{};
    T t_;
};
template <size_t size>
using CoroExecutionContext = CoroExecutionContextWith<size, Void>;

class pre_coro_ctx
{
public:
    pre_coro_ctx(CoroContext *ctx) : ctx_(ctx)
    {
    }
    CoroContext *ctx_;
};
inline std::ostream &operator<<(std::ostream &os, const pre_coro_ctx &pctx)
{
    if (pctx.ctx_)
    {
        os << *pctx.ctx_;
    }
    else
    {
        os << "{no coro ctx}";
    }
    return os;
}

#endif