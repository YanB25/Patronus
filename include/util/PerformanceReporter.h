#pragma once
#ifndef PERFORMANCE_REPORTER_H_
#define PERFORMANCE_REPORTER_H_

#include <atomic>
#include <chrono>
#include <list>
#include <string>
#include <thread>

#include "PerThread.h"

class Averager
{
public:
    Averager() = default;
    void add(double n)
    {
        num_ += n;
        absolute_num_ += abs(n);
        times_++;
        avg_ = 1.0 * num_ / times_;
        absolute_avg_ = 1.0 * absolute_num_ / times_;
    }
    double average() const
    {
        return avg_;
    }
    double abs_average() const
    {
        return absolute_avg_;
    }
    double sum() const
    {
        return num_;
    }
    double abs_sum() const
    {
        return absolute_num_;
    }

private:
    double num_{0};
    double absolute_num_{0};
    size_t times_{0};
    double avg_{0};
    double absolute_avg_{0};
};

template <typename T>
class OnePassMonitorImpl
{
public:
    OnePassMonitorImpl() = default;
    void collect(T n)
    {
        min_ = std::min(min_, n);
        max_ = std::max(max_, n);
        abs_min_ = std::min(abs_min_, abs(n));
        avg_.add(n);
        data_nr_++;
    }
    T min() const
    {
        return min_;
    }
    T abs_min() const
    {
        return abs_min_;
    }
    T sum() const
    {
        return avg_.sum();
    }
    T abs_sum() const
    {
        return avg_.abs_sum();
    }
    T max() const
    {
        return max_;
    }
    T average() const
    {
        return avg_.average();
    }
    T abs_average() const
    {
        return avg_.abs_average();
    }
    size_t data_nr() const
    {
        return data_nr_;
    }

private:
    T min_{std::numeric_limits<T>::max()};
    T max_{std::numeric_limits<T>::lowest()};
    T abs_min_{std::numeric_limits<T>::max()};

    Averager avg_;
    size_t data_nr_{0};
};

template <typename T>
inline std::ostream &operator<<(std::ostream &os,
                                const OnePassMonitorImpl<T> &m)
{
    os << "{min: " << m.min() << ", max: " << m.max()
       << ", avg: " << m.average() << ", abs_avg: " << m.abs_average()
       << ", abs_min: " << m.abs_min() << ". collected " << m.data_nr()
       << " data}";
    return os;
}
using OnePassMonitor = OnePassMonitorImpl<double>;
using OnePassIntegerMonitor = OnePassMonitorImpl<int64_t>;

class PerformanceReporter
{
public:
    PerformanceReporter(const std::string &name) : name_(name)
    {
        thread_finished_.fill(false);
        thread_op_.fill(0);
    }

    template <typename T>
    void start(const T &duration, bool report)
    {
        monitor_thread = std::thread([this, duration, report]() {
            while (likely(!is_finish()))
            {
                auto before = sum_op();
                auto before_time = std::chrono::steady_clock::now();

                std::this_thread::sleep_for(duration);

                auto after_time = std::chrono::steady_clock::now();
                auto after = sum_op();

                auto op = after - before;
                size_t ns =
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        after_time - before_time)
                        .count();
                double ops = 1.0 * op * 1e9 / ns;

                double ns_per_op = 1.0 * ns / op;
                double ns_per_op_per_thread = ns_per_op / bench_threads_.size();

                // determine finished
                bool should_finish = true;
                for (size_t i = 0; i < bench_threads_.size(); ++i)
                {
                    if (!thread_finished_[i].load(std::memory_order_relaxed))
                    {
                        should_finish = false;
                        break;
                    }
                }
                if (unlikely(should_finish))
                {
                    g_finished_ = true;
                    continue;
                }

                // only calculate the measure if not finished
                // only calculate the before > 0 (already started)
                if (before > 0)
                {
                    op_.collect(op);
                    ops_.collect(ops);
                    avg_ns_.add(ns_per_op);
                    avg_ns_perthread_.add(ns_per_op_per_thread);

                    LOG_IF(INFO, report)
                        << "[performance] `" << name_ << "` op: " << op
                        << ", ns: " << ns << ", ops: " << ops
                        << ", aggregated avg-ns: " << ns_per_op
                        << ", avg-ns-thread: " << ns_per_op_per_thread;
                }
            }
        });
    }

    std::string name() const
    {
        return name_;
    }

    using ThreadID = uint64_t;
    using BenchTask = std::function<void(
        ThreadID, std::atomic<size_t> &, std::atomic<bool> &)>;
    void bench_task(size_t thread_nr, const BenchTask task)
    {
        CHECK_LE(thread_nr, kMaxAppThread);
        for (size_t i = 0; i < thread_nr; ++i)
        {
            bench_threads_.emplace_back(
                [tid = i,
                 task,
                 &thread_op = thread_op_[i],
                 &thread_finish = thread_finished_[i]]() {
                    // leave the 0 core idle
                    bindCore(tid + 1);
                    task(tid, thread_op, thread_finish);
                });
        }
    }

    bool is_finish() const
    {
        return g_finished_.load(std::memory_order_relaxed);
    }

    double max_ops() const
    {
        return ops_.max();
    }
    double min_ops() const
    {
        return ops_.min();
    }
    size_t max_op() const
    {
        return op_.max();
    }
    size_t min_op() const
    {
        return op_.min();
    }
    double avg_ops() const
    {
        return ops_.average();
    }
    double avg_ns() const
    {
        return avg_ns_.average();
    }
    double avg_ns_per_thread() const
    {
        return avg_ns_perthread_.average();
    }
    void wait()
    {
        for (auto &t : bench_threads_)
        {
            if (t.joinable())
            {
                t.join();
            }
        }
        if (monitor_thread.joinable())
        {
            monitor_thread.join();
        }
    }
    ~PerformanceReporter()
    {
        wait();
    }

private:
    uint64_t sum_op() const
    {
        uint64_t ret = 0;
        for (size_t i = 0; i < bench_threads_.size(); ++i)
        {
            ret += thread_op_[i].load(std::memory_order_relaxed);
        }
        return ret;
    }

    friend std::ostream &operator<<(std::ostream &,
                                    const PerformanceReporter &);
    std::string name_;
    std::thread monitor_thread;
    OnePassMonitor ops_;
    OnePassMonitor op_;

    // used to calculate avg
    Averager avg_ns_;
    Averager avg_ns_perthread_;

    // for management
    std::atomic<bool> g_finished_{false};

    // for bench tasks
    std::vector<std::thread> bench_threads_;
    Perthread<std::atomic<uint64_t>> thread_op_;
    Perthread<std::atomic<bool>> thread_finished_;
};

inline std::ostream &operator<<(std::ostream &os, const PerformanceReporter &r)
{
    os << "SUMMARY [Performance `" << r.name() << "`] avg_ops: " << r.avg_ops()
       << ", max_ops: " << r.max_ops() << ", min_ops: " << r.min_ops()
       << ", aggregated avg-ns: " << r.avg_ns()
       << ", avg-ns(thread): " << r.avg_ns_per_thread();
    return os;
}

template <typename T>
class OnePassBucketMonitor
{
public:
    OnePassBucketMonitor(T min, T max, T range)
        : min_(min), max_(max), range_(range)
    {
        range_nr_ = (max_ - min_) / range_;
        // for less-than-min & larger-then-max
        range_nr_ = std::max(range_nr_, (size_t) 1);
        buckets_.resize(range_nr_);
    }
    void collect(T t)
    {
        collect_min_ = std::min(t, collect_min_);
        collect_max_ = std::max(t, collect_max_);
        collected_nr_++;

        if (unlikely(t < min_))
        {
            less_than_min_++;
        }
        else if (unlikely(t >= max_))
        {
            larger_than_max_++;
        }
        else
        {
            auto range_id = (t - min_) / range_;
            DCHECK_LT(range_id, buckets_.size());
            buckets_[range_id]++;
        }
    }
    T min() const
    {
        return collect_min_;
    }
    T max() const
    {
        return collect_max_;
    }
    T percentile(double p) const
    {
        uint64_t id = 1.0 * collected_nr_ * p;
        if (unlikely(less_than_min_ >= id))
        {
            return collect_min_;
        }
        id -= less_than_min_;
        for (size_t i = 0; i < buckets_.size(); ++i)
        {
            auto nr = buckets_[i];
            if (nr >= id)
            {
                return min_ + range_ * i;
            }
            id -= nr;
        }
        return collect_max_;
    }
    size_t data_nr() const
    {
        return collected_nr_;
    }

private:
    T min_;
    T max_;
    T range_;
    size_t range_nr_{0};
    T collect_min_{std::numeric_limits<T>::max()};
    T collect_max_{std::numeric_limits<T>::lowest()};
    uint64_t less_than_min_{0};
    uint64_t larger_than_max_{0};
    size_t collected_nr_{0};
    std::vector<uint64_t> buckets_;
};

template <typename T>
inline std::ostream &operator<<(std::ostream &os,
                                const OnePassBucketMonitor<T> &m)
{
    os << "{min: " << m.min() << ", max: " << m.max()
       << ", median: " << m.percentile(0.5) << ", p8: " << m.percentile(0.8)
       << ", p9: " << m.percentile(0.9) << ", p99: " << m.percentile(0.99)
       << ", p999: " << m.percentile(0.999) << ", max: " << m.max()
       << ". collected " << m.data_nr() << " data}";
    return os;
}

// std::list<std::array<T, kBatchSize>>;
template <typename T, size_t kBatchSize = 1000>
class Sequence
{
public:
    bool empty() const
    {
        return size_ == 0;
    }
    size_t size() const
    {
        return size_;
    }
    void push_back(const T &value)
    {
        auto sid = slot_id();
        if (unlikely(size_ % kBatchSize == 0))
        {
            sequence_.push_back({});
        }
        sequence_.back()[sid] = value;
        size_++;
    }
    void emplace_back(T &&value)
    {
        auto sid = slot_id();
        if (unlikely(size_ % kBatchSize == 0))
        {
            sequence_.push_back({});
            sequence_.back()[sid] = std::move(value);
        }
        size_++;
    }
    T &front()
    {
        return sequence_.front()[0];
    }
    T &back()
    {
        return sequence_.back()[slot_id() - 1];
    }
    std::vector<T> to_vector()
    {
        std::vector<T> ret;
        ret.reserve(size_);
        for (auto it = sequence_.cbegin(); it != sequence_.cend(); ++it)
        {
            for (size_t i = 0; i < kBatchSize; ++i)
            {
                ret.push_back((*it)[i]);
                if (ret.size() == size_)
                {
                    return ret;
                }
            }
        }
        return ret;
    }
    size_t batch_id() const
    {
        return size_ / kBatchSize;
    }
    size_t slot_id() const
    {
        return size_ % kBatchSize;
    }
    void clear()
    {
        size_ = 0;
        sequence_.clear();
    }

private:
    using BatchT = std::array<T, kBatchSize>;
    std::list<BatchT> sequence_;
    size_t size_{0};
};

#endif