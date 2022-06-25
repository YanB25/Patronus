#pragma once
#ifndef SHERMEM_TRACER_H_
#define SHERMEM_TRACER_H_

#include <iomanip>
#include <memory>

#include "Common.h"
#include "Timer.h"
#include "util/Pre.h"
#include "util/Rand.h"

namespace util
{
class TracerContext;
struct TraceRecord;
class TraceView
{
public:
    TraceView(TracerContext *impl) : impl_(impl)
    {
    }
    const std::vector<RetrieveTimerRecord> &retrieve_vec() const;
    const std::map<std::string, uint64_t> &retrieve_map() const;
    bool enabled() const;
    std::string name() const;
    uint64_t sum_ns() const;
    std::vector<TraceRecord> get_flat_records(
        size_t depth_limit = std::numeric_limits<size_t>::max()) const;

    uint64_t pin(std::string_view name);
    TraceView child(std::string_view name);
    friend std::ostream &operator<<(std::ostream &os, const TraceView view);
    void set(const std::string &key, const std::string &value);
    std::string get(const std::string &key) const;
    std::map<std::string, std::string> kv() const;

private:
    TracerContext *impl_{nullptr};
};

class pre_trace_ctx;

struct TraceRecord
{
    uint64_t id;
    std::string name;
    uint64_t parent_id;
    std::string parent_name;
    uint64_t ns;
    double local_ratio;
    double global_ratio;
    bool has_child;
};
inline std::ostream &operator<<(std::ostream &os, const TraceRecord &r)
{
    os << "{name: " << r.name << ", id: " << r.id << ", pid: " << r.parent_id
       << ", pname: " << r.parent_name << ", ns: " << r.ns
       << ", local: " << r.local_ratio << ", global_ratio: " << r.global_ratio
       << ", has_child: " << r.has_child << "}";
    return os;
}

class TracerContext
{
public:
    using pointer = std::unique_ptr<TracerContext>;
    static pointer new_instance()
    {
        return std::make_unique<TracerContext>();
    }
    TracerContext()
    {
    }
    std::string name() const
    {
        return name_;
    }
    void clear()
    {
        timer_.clear();
        child_contexts_.clear();
        sum_ns_ = std::nullopt;
        kv_.clear();
    }
    void init(const std::string &name)
    {
        name_ = name;
        timer_.init(name);
        child_contexts_.clear();
        kv_.clear();
    }
    uint64_t pin(const std::string &name)
    {
        return timer_.pin(name);
    }
    TracerContext *child_context(const std::string &name)
    {
        auto child_ctx = new_instance();
        child_ctx->init(name);
        child_contexts_.emplace_back(std::move(child_ctx));
        return child_contexts_.back().get();
    }
    const auto &retrieve_vec() const
    {
        return timer_.retrieve_vec();
    }
    const auto &retrieve_map() const
    {
        return timer_.retrieve_map();
    }
    TraceView view()
    {
        return TraceView(this);
    }
    friend std::ostream &operator<<(std::ostream &os, const TracerContext &ctx);
    friend std::ostream &operator<<(std::ostream &os, const pre_trace_ctx &c);

    uint64_t sum_ns() const
    {
        if (sum_ns_.has_value())
        {
            return sum_ns_.value();
        }
        uint64_t sum = 0;
        sum += timer_.sum_ns();
        for (const auto &child : child_contexts_)
        {
            sum += child->sum_ns();
        }
        sum_ns_ = sum;
        return sum;
    }
    const RetrieveTimer &timer() const
    {
        return timer_;
    }
    void set(const std::string &key, const std::string &value)
    {
        kv_[key] = value;
    }
    std::string get(const std::string &key) const
    {
        auto it = kv_.find(key);
        if (it != kv_.end())
        {
            return it->second;
        }
        return "";
    }
    std::map<std::string, std::string> kv() const
    {
        return kv_;
    }
    std::vector<TraceRecord> get_flat_records(
        size_t depth_limit = std::numeric_limits<size_t>::max()) const
    {
        uint64_t id = 0;
        auto ret = do_get_flat_records(id, 0, name(), depth_limit);
        double sum_ns = 0;
        for (const auto &r : ret)
        {
            sum_ns += r.ns;
        }
        for (auto &r : ret)
        {
            r.global_ratio = 1.0 * r.ns / sum_ns;
        }
        return ret;
    }

    std::vector<TraceRecord> do_get_flat_records(uint64_t &initial_id,
                                                 uint64_t pid,
                                                 const std::string &pname,
                                                 size_t depth_limit) const
    {
        std::vector<TraceRecord> ret;
        auto sum = sum_ns();
        for (const auto &vec : retrieve_vec())
        {
            ret.push_back(TraceRecord{.id = initial_id++,
                                      .name = vec.name,
                                      .parent_id = pid,
                                      .parent_name = pname,
                                      .ns = vec.ns,
                                      .local_ratio = 1.0 * vec.ns / sum,
                                      .has_child = false});
        }
        for (const auto &child : child_contexts_)
        {
            auto this_id = initial_id++;
            auto ns = child->sum_ns();
            double ratio = 1.0 * ns / sum_ns();
            ret.push_back(TraceRecord{.id = this_id,
                                      .name = child->name(),
                                      .parent_id = pid,
                                      .parent_name = pname,
                                      .ns = ns,
                                      .local_ratio = ratio,
                                      .has_child = true});
            if (depth_limit > 0)
            {
                auto child_records = child->do_get_flat_records(
                    initial_id, this_id, child->name(), depth_limit - 1);
                ret.insert(ret.end(),
                           std::make_move_iterator(child_records.begin()),
                           std::make_move_iterator(child_records.end()));
            }
        }
        return ret;
    }

private:
    std::string name_;
    RetrieveTimer timer_;
    std::list<pointer> child_contexts_;
    std::map<std::string, std::string> kv_;

    mutable std::optional<uint64_t> sum_ns_;
};

class TraceManager
{
public:
    /**
     * @brief TraceManager is the central class for managing tracing.
     *
     * TraceManager tm(...); // hold resources
     * {
     *      auto trace = tm.trace(); // get the trace
     *      // ...
     *      trace.pin("finished task 1");
     *      // ...
     *      trace.pin("finished task 2");
     *
     *      if (trace.enabled())
     *      {
     *          // below are three ways to use the trace
     *          auto map = trace.retrieve_map();
     *          auto vec = trace.retrieve_vec();
     *          std::cout << trace << std::endl;
     *      }
     * }
     *
     * @param rate at which rate the trace should be enabled
     * @param limit_nr enable at most limit_nr traces.
     */
    TraceManager(double rate = 1,
                 size_t limit_nr = std::numeric_limits<size_t>::max())
        : trace_rate_(rate),
          limit_nr_{limit_nr},
          tracer_context_(TracerContext::new_instance())
    {
    }
    TraceView trace(const std::string &name)
    {
        if (likely(trace_rate_ == 0))
        {
            return TraceView(nullptr);
        }
        if (unlikely(release_nr_ >= limit_nr_))
        {
            return TraceView(nullptr);
        }
        if (true_with_prob(trace_rate_))
        {
            release_nr_++;
            tracer_context_->clear();
            tracer_context_->init(name);
            return TraceView(tracer_context_.get());
        }
        else
        {
            return TraceView(nullptr);
        }
    }

private:
    double trace_rate_{0};
    size_t limit_nr_{0};
    size_t release_nr_{0};
    TracerContext::pointer tracer_context_;
};

static TraceView nulltrace{nullptr};

inline std::ostream &operator<<(std::ostream &os, const TracerContext &ctx)
{
    os << "{TracerContext: " << ctx.name_ << ", timer: " << ctx.timer_;
    for (const auto &child : ctx.child_contexts_)
    {
        os << child->name() << ": " << *child;
    }
    os << "}. meta: " << util::pre_map(ctx.kv());
    return os;
}

class pre_trace_ctx
{
public:
    pre_trace_ctx(TracerContext *ctx, size_t indent)
        : ctx_(ctx), indent_(indent)
    {
    }

    friend std::ostream &operator<<(std::ostream &os, const pre_trace_ctx &c);

private:
    TracerContext *ctx_;
    size_t indent_{0};
};
inline std::ostream &operator<<(std::ostream &os, const pre_trace_ctx &c)
{
    const auto &timer = c.ctx_->timer();
    std::string prefix = std::string(c.indent_ * 4, ' ');
    auto sum_ns = c.ctx_->sum_ns();
    for (const auto &record : timer.retrieve_vec())
    {
        double rate = sum_ns > 0 ? 1.0 * record.ns / sum_ns : 1;
        os << prefix << "[" << record.name << "] takes " << record.ns << " ns ("
           << rate * 100 << "%)" << std::endl;
    }
    for (const auto &child : c.ctx_->child_contexts_)
    {
        auto take_ns = child->sum_ns();
        double rate = sum_ns > 0 ? 1.0 * take_ns / sum_ns : 1;
        os << prefix << "[" << child->name() << "] takes " << take_ns << " ns ("
           << rate * 100 << "%)" << std::endl;
        os << pre_trace_ctx(child.get(), c.indent_ + 1);
    }
    return os;
}

inline std::ostream &operator<<(std::ostream &os, const TraceView view)
{
    if (view.enabled())
    {
        os << "{TraceView " << view.name() << " takes " << view.sum_ns()
           << " ns " << std::endl;
        os << pre_trace_ctx(view.impl_, 0 /* initial ident */);
        os << "}";
    }
    else
    {
        os << "{TraceView }";
    }
    return os;
}

}  // namespace util

#endif