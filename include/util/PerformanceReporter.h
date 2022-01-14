#pragma once
#ifndef PERFORMANCE_REPORTER_H_
#define PERFORMANCE_REPORTER_H_

#include <chrono>

class PerformanceReporter
{
public:
    PerformanceReporter(const std::string &name,
                        std::atomic<uint64_t> &op,
                        std::atomic<bool> &finished)
        : name_(name), op_(op), finished_(finished)
    {
    }

    template <typename T>
    void start(const T &duration)
    {
        monitor_thread = std::thread([this, duration]() {
            while (likely(!finished_.load(std::memory_order_relaxed)))
            {
                size_t before = op_.load(std::memory_order_relaxed);
                auto before_time = std::chrono::steady_clock::now();

                std::this_thread::sleep_for(duration);

                auto after_time = std::chrono::steady_clock::now();
                size_t after = op_.load(std::memory_order_relaxed);

                size_t op = after - before;
                size_t ns =
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        after_time - before_time)
                        .count();
                double ops = 1.0 * op * 1e9 / ns;
                LOG(INFO) << "[performance] " << name_ << " op: " << op
                          << ", ns: " << ns << ", ops: " << ops;
            }
        });
    }
    ~PerformanceReporter()
    {
        monitor_thread.join();
    }

private:
    std::string name_;
    std::atomic<uint64_t> &op_;
    std::atomic<bool> &finished_;
    std::thread monitor_thread;
};

#endif