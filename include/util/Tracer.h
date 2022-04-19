#pragma once
#ifndef SHERMEM_TRACER_H_
#define SHERMEM_TRACER_H_

#include <memory>

#include "Common.h"
#include "Timer.h"

namespace util
{
class TracerContext;
class TraceView
{
public:
    TraceView(TracerContext *impl) : impl_(impl)
    {
    }

    uint64_t pin(const std::string &name);
    friend std::ostream &operator<<(std::ostream &os, const TraceView view);

private:
    TracerContext *impl_{nullptr};
};

class TracerContext
{
public:
    using pointer = std::unique_ptr<TracerContext>;
    static pointer new_instance(const std::string name)
    {
        return std::make_unique<TracerContext>(name);
    }
    TracerContext(const std::string &name) : name_(name)
    {
        timer_.init(name);
    }
    void init(const std::string &name)
    {
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