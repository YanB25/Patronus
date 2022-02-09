#pragma once
#ifndef PERFORMANCE_REPORTER_H_
#define PERFORMANCE_REPORTER_H_

#include <atomic>
#include <chrono>
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
        CHECK_LE(thread_nr, MAX_APP_THREAD);
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

#endif