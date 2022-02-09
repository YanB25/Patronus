#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

#include "Common.h"
#include "glog/logging.h"
#include "util/PerformanceReporter.h"
using namespace std::chrono_literals;

constexpr static size_t kThreadNr = 8;
[[maybe_unused]] constexpr static double kScale = 1.0;

void bindCore(uint16_t core)
{
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core, &cpuset);
    int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0)
    {
        std::cerr << "can't bind core " << core << std::endl;
    }
}

void report_rate()
{
    std::vector<std::thread> threads;
    bindCore(0);
    std::array<double, kThreadNr> rdtsc_per_ns;
    std::array<double, kThreadNr> ns_per_rdtsc;

    for (size_t i = 0; i < kThreadNr; ++i)
    {
        threads.emplace_back([i, &rdtsc_per_ns, &ns_per_rdtsc]() {
            bindCore(i + 1);
            auto before_now = std::chrono::steady_clock::now();
            auto before_rdtsc = rdtsc();
            std::this_thread::sleep_for(1s);
            auto after_rdtsc = rdtsc();
            auto after_now = std::chrono::steady_clock::now();

            auto take_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                               after_now - before_now)
                               .count();
            auto take_rdtsc = after_rdtsc - before_rdtsc;
            rdtsc_per_ns[i] = 1.0 * take_rdtsc / take_ns;
            ns_per_rdtsc[i] = 1.0 * take_ns / take_rdtsc;
        });
    }
    for (auto &t : threads)
    {
        if (t.joinable())
        {
            t.join();
        }
    }
    for (size_t i = 0; i < kThreadNr; ++i)
    {
        LOG(INFO) << "Thread " << i << " rdtsc/ns: " << rdtsc_per_ns[i]
                  << ", ns/rdtsc: " << ns_per_rdtsc[i];
    }
    for (size_t i = 0; i < kThreadNr; ++i)
    {
        for (size_t j = 0; j < kThreadNr; ++j)
        {
            auto diff = rdtsc_per_ns[i] - rdtsc_per_ns[j];
            CHECK_LT(diff, 0.01) << i << "-th thread and " << j
                                 << "-th thread has different rdtsc rate.";
        }
    }
}

void check_stable_across_threads()
{
    bindCore(0);
    std::atomic<uint64_t> shared_rdtsc{0};
    std::atomic<bool> finished{false};
    std::atomic<bool> started{false};
    std::thread exposer([&shared_rdtsc, &finished, &started]() {
        bindCore(1);
        while (likely(!finished.load(std::memory_order_relaxed)))
        {
            shared_rdtsc.store(rdtsc(), std::memory_order_relaxed);
            if (unlikely(!started.load(std::memory_order_relaxed)))
            {
                started = true;
            }
        }
    });

    OnePassMonitor diff_rdtsc_m;
    auto begin_now = std::chrono::steady_clock::now();
    while (true)
    {
        while (!started)
        {
            continue;
        }
        auto now = std::chrono::steady_clock::now();
        auto my_rdtsc = rdtsc();
        int64_t rdtsc_diff =
            my_rdtsc - shared_rdtsc.load(std::memory_order_relaxed);
        diff_rdtsc_m.collect(rdtsc_diff);
        if (unlikely(now - begin_now >= 5s))
        {
            finished = true;
            break;
        }
    }

    LOG(INFO) << "[bench] rdtsc_diff_m: " << diff_rdtsc_m;

    exposer.join();

    CHECK_LT(diff_rdtsc_m.abs_average(), 2000)
        << "Hopefully, the diff of rdtsc should not be too high";
}

int main()
{
    check_stable_across_threads();
    report_rate();
    return 0;
}