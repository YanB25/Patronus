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
class TraceNode;
struct TraceRecord;
class TraceView
{
public:
    TraceView(TraceNode *impl) : impl_(impl)
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
    TraceNode *impl_{nullptr};
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

class pre_trace_node;

class TraceNode
{
public:
    using pointer = std::shared_ptr<TraceNode>;
    using TimePoint = std::chrono::time_point<std::chrono::steady_clock>;
    TraceNode(const std::string &s, uint64_t ns, TimePoint &last)
        : name_(s), ns_(ns), last_(last)
    {
    }
    static pointer new_instance(const std::string &name,
                                uint64_t ns,
                                TimePoint &last)
    {
        return std::make_shared<TraceNode>(name, ns, last);
    }
    std::string name() const
    {
        return name_;
    }
    void clear()
    {
        list_.clear();
    }
    void init(const std::string &name)
    {
        name_ = name;
        ns_ = 0;
        list_.clear();
        kv_.clear();
        last_ = std::chrono::steady_clock::now();

        vec_records_ = std::nullopt;
        map_records_ = std::nullopt;
    }
    TraceNode *child(const std::string &name)
    {
        auto now = std::chrono::steady_clock::now();
        auto ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(now - last_)
                .count();
        last_ = now;
        list_.emplace_back(name, ns, last_);
        return &list_.back();
    }
    uint64_t pin(const std::string &name)
    {
        auto now = std::chrono::steady_clock::now();
        auto ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(now - last_)
                .count();
        last_ = now;
        list_.emplace_back(name, ns, last_);
        return ns;
    }
    uint64_t sum_ns() const
    {
        return self_ns() + child_ns();
    }
    uint64_t self_ns() const
    {
        return ns_;
    }
    uint64_t child_ns() const
    {
        uint64_t ret = 0;
        for (const auto &node : list_)
        {
            ret += node.sum_ns();
        }
        return ret;
    }
    const std::map<std::string, uint64_t> &retrieve_map() const
    {
        if (likely(map_records_.has_value()))
        {
            return map_records_.value();
        }
        else
        {
            map_records_ = {};
        }

        for (const auto &node : list_)
        {
            map_records_.value()[node.name()] += node.sum_ns();
        }
        return map_records_.value();
    }
    uint64_t ns() const
    {
        return ns_;
    }
    const std::vector<RetrieveTimerRecord> &retrieve_vec() const
    {
        if (likely(vec_records_.has_value()))
        {
            return vec_records_.value();
        }
        else
        {
            vec_records_ = {};
        }

        for (const auto &node : list_)
        {
            vec_records_.value().emplace_back(node.name(), node.sum_ns());
        }
        return vec_records_.value();
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

    friend std::ostream &operator<<(std::ostream &os, const pre_trace_node &n);

private:
    std::string name_;
    uint64_t ns_{0};
    std::list<TraceNode> list_;
    std::map<std::string, std::string> kv_;
    TimePoint &last_;

    mutable std::optional<std::vector<RetrieveTimerRecord>> vec_records_{
        std::nullopt};
    mutable std::optional<std::map<std::string, uint64_t>> map_records_{
        std::nullopt};
};

class pre_trace_node
{
public:
    pre_trace_node(const TraceNode *node, size_t indent, uint64_t parent_ns)
        : node_(node), indent_(indent), parent_ns_(parent_ns)
    {
    }
    friend std::ostream &operator<<(std::ostream &os, const pre_trace_node &);

private:
    const TraceNode *node_{nullptr};
    size_t indent_{0};
    uint64_t parent_ns_{0};
};

inline std::ostream &operator<<(std::ostream &os, const pre_trace_node &n)
{
    std::string prefix = std::string(n.indent_ * 4, ' ');
    auto sum_ns = n.node_->sum_ns();
    auto self_ns = n.node_->self_ns();

    os << prefix << "[" << n.node_->name() << "] ";
    bool cont = false;
    if (self_ns)
    {
        os << "self " << self_ns << " ns";
        cont = true;
    }
    if (sum_ns != self_ns)
    {
        if (cont)
        {
            os << ", ";
        }
        os << "total " << sum_ns << " ns";
        cont = true;
    }
    if (n.parent_ns_ != 0)
    {
        if (cont)
        {
            os << ", ";
        }
        double rate = 1.0 * sum_ns / n.parent_ns_;
        os << 100.0 * rate << "%";
    }
    os << std::endl;

    for (const auto &node : n.node_->list_)
    {
        os << pre_trace_node(&node, n.indent_ + 1, sum_ns);
    }
    return os;
}

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
          trace_root_(TraceNode::new_instance("", 0, last_))
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
            trace_root_->init(name);
            return TraceView(trace_root_.get());
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
    // TracerContext::pointer tracer_context_;
    TraceNode::pointer trace_root_;
    std::chrono::time_point<std::chrono::steady_clock> last_;
};

static TraceView nulltrace{nullptr};

inline std::ostream &operator<<(std::ostream &os, const TraceView view)
{
    if (view.enabled())
    {
        os << "{TraceView " << view.name() << " takes " << view.sum_ns()
           << " ns " << std::endl;
        os << pre_trace_node(
            view.impl_, 0 /* initial ident */, 0 /* parent ns */);
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