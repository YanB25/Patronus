#pragma once
#ifndef SHERMEM_TRACER_H_
#define SHERMEM_TRACER_H_

#include <memory>

#include "Common.h"
#include "Timer.h"
#include "util/Rand.h"

namespace util
{
class TracerContext;
class TraceView
{
public:
    TraceView(TracerContext *impl) : impl_(impl)
    {
    }
    std::vector<RetrieveTimerRecord> retrieve_vec() const;
    std::map<std::string, uint64_t> retrieve_map() const;
    bool enabled() const;

    uint64_t pin(const std::string &name);
    friend std::ostream &operator<<(std::ostream &os, const TraceView view);

private:
    TracerContext *impl_{nullptr};
};

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
    void clear()
    {
        timer_.clear();
    }
    void init(const std::string &name)
    {
        name_ = name;
        timer_.init(name);
    }
    uint64_t pin(const std::string &name)
    {
        return timer_.pin(name);
    }
    auto retrieve_vec() const
    {
        return timer_.retrieve_vec();
    }
    auto retrieve_map() const
    {
        return timer_.retrieve_map();
    }
    TraceView view()
    {
        return TraceView(this);
    }
    friend std::ostream &operator<<(std::ostream &os, const TracerContext &ctx);

private:
    std::string name_;
    RetrieveTimer timer_;
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
    return os;
}

inline std::ostream &operator<<(std::ostream &os, const TraceView view)
{
    os << "{TraceView ";
    if (view.impl_)
    {
        os << *view.impl_;
    }
    else
    {
        os << " null ";
    }
    os << "}";
    return os;
}

}  // namespace util

#endif