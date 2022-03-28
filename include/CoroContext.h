#pragma once
#ifndef SHERMEM_CORO_CONTEXT_H_
#define SHERMEM_CORO_CONTEXT_H_

#include "Common.h"

struct CoroContext
{
public:
    CoroContext(size_t thread_id,
                CoroYield *yield,
                CoroCall *master,
                coro_t coro_id)
        : thread_id_(thread_id),
          yield_(yield),
          master_(master),
          coro_id_(coro_id)
    {
        CHECK_NE(coro_id, kMasterCoro) << "** This coro should not be a master";
        CHECK_NE(coro_id, kNotACoro) << "** This coro should not be nullctx";
    }
    CoroContext(size_t thread_id, CoroYield *yield, CoroCall *workers)
        : thread_id_(thread_id), yield_(yield), workers_(workers)
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

    void yield_to_master() const
    {
        DVLOG(4) << "[Coro] " << *this << " yielding to master.";
        DCHECK(is_worker()) << *this;
        (*yield_)(*master_);
    }
    void yield_to_worker(coro_t wid)
    {
        DVLOG(4) << "[Coro] " << *this << " yielding to worker " << (int) wid;
        DCHECK(is_master()) << *this;
        (*yield_)(workers_[wid]);
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
    ContTimer<config::kEnableTrace> &timer()
    {
        return timer_;
    }

private:
    size_t thread_id_{0};
    CoroYield *yield_{nullptr};
    CoroCall *master_{nullptr};
    CoroCall *workers_{nullptr};
    coro_t coro_id_{kNotACoro};

    trace_t trace_{0};
    ContTimer<config::kEnableTrace> timer_;
};

static CoroContext nullctx;

inline std::ostream &operator<<(std::ostream &os, const CoroContext &ctx)
{
    if (ctx.coro_id_ == kMasterCoro)
    {
        os << "{Coro Master T(" << ctx.thread_id_ << ") }";
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

template <size_t kCoroCnt_>
class CoroExecutionContext
{
public:
    constexpr static size_t kCoroCnt = kCoroCnt_;

    void worker_finished(size_t wid)
    {
        finish_all_[wid] = true;
    }
    bool is_finished_all() const
    {
        return std::all_of(
            finish_all_.begin(), finish_all_.end(), [](bool i) { return i; });
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

private:
    std::array<CoroCall, kCoroCnt> workers_;
    CoroCall master_;
    std::array<bool, kCoroCnt> finish_all_{};
};

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
    return os;
}

#endif