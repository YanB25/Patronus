#include <algorithm>
#include <map>
#include <random>

#include "DSM.h"
#include "Timer.h"
#include "util/monitor.h"

std::map<std::chrono::time_point<std::chrono::steady_clock>, std::string>
    time_to_str;

constexpr static size_t kTestTime = 100 * define::M;

std::ostream &operator<<(
    std::ostream &os,
    const std::chrono::time_point<std::chrono::steady_clock> &time)
{
    auto ns_since_epoch = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              time.time_since_epoch())
                              .count();
    os << "{steady_clock Timepoint " << ns_since_epoch << " }";
    return os;
}

using namespace std::chrono_literals;

std::chrono::time_point<std::chrono::steady_clock> explain_now;
void task_map_of_ddl()
{
    // auto start = std::chrono::time_point<std::chrono::system_clock>();
    explain_now = std::chrono::steady_clock::now();
    auto time_since_epoch = explain_now.time_since_epoch();
    auto ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(time_since_epoch)
            .count();
    auto hours =
        std::chrono::duration_cast<std::chrono::hours>(time_since_epoch)
            .count();
    LOG(INFO) << "Since the epoch: ns : " << ns << ", hours: " << hours;

    explain_now = std::chrono::steady_clock::now();
    std::map<std::chrono::time_point<std::chrono::steady_clock>, size_t>
        task_map;
    // the diff_us
    size_t offset_us = 0;
    for (size_t i = 1; i < 10; ++i)
    {
        for (size_t times = 0; times < 10; ++times)
        {
            offset_us += i;
            task_map[explain_now + std::chrono::microseconds(offset_us)] = i;
        }
    }

    std::vector<std::pair<size_t, size_t>> diff_ns_set;
    while (!task_map.empty())
    {
        auto begin = task_map.begin();
        auto wait_now = std::chrono::steady_clock::now();
        while (wait_now < begin->first)
        {
            wait_now = std::chrono::steady_clock::now();
        }
        auto ns_diff = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           wait_now - begin->first)
                           .count();
        // diff_ns_set.push_back({begin->second, ns_diff});
        diff_ns_set.push_back({(size_t)(begin->second), (size_t) ns_diff});
        task_map.erase(begin);
    }
    for (const auto &[offset_us, diff_ns] : diff_ns_set)
    {
        LOG(INFO) << "[bench], diff_ns: " << diff_ns
                  << " for uffset_us: " << offset_us;
    }

    explain_now += 3s;

    auto cur = std::chrono::steady_clock::now();
    while (cur < explain_now)
    {
        LOG(INFO) << "current time " << cur << ", not till 3s";
        std::this_thread::sleep_for(500ms);
        cur = std::chrono::steady_clock::now();
    }
}

size_t bench_compare_times_per_sec()
{
    auto now = std::chrono::steady_clock::now();
    auto ddl = now + 1s;
    size_t times = 0;
    while (now < ddl)
    {
        now = std::chrono::steady_clock::now();
        times++;
    }
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(1s).count();
    LOG(INFO) << "[bench_compare_times_per_sec] op: " << times << ", ns: " << ns
              << ", ops: " << 1.0 * times * 1e9 / ns;
    return times;
}

size_t bench_time_point_modification()
{
    double max_tp = 0;
    std::atomic<size_t> times{0};
    std::atomic<bool> finished{false};

    std::thread t([&times, &finished]() {
        size_t magic = 0;
        auto now = std::chrono::steady_clock::now();
        for (size_t i = 0; i < kTestTime; ++i)
        {
            now += 1ns;
            times.fetch_add(1, std::memory_order_relaxed);
        }
        magic = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    now.time_since_epoch())
                    .count();
        LOG(INFO) << "[bench] ignore me. magic: " << magic;
        finished = true;
    });

    while (!finished)
    {
        auto before_time = std::chrono::steady_clock::now();
        auto before = times.load(std::memory_order_relaxed);
        usleep(100 * 1000);
        auto after = times.load(std::memory_order_relaxed);
        auto after_time = std::chrono::steady_clock::now();
        auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                      after_time - before_time)
                      .count();
        auto op = after - before;
        double ops = 1.0 * 1e9 * op / ns;
        LOG(INFO) << "[bench_time_point_modification] op: " << op
                  << ", ns: " << ns << ", ops: " << ops;
        max_tp = std::max(max_tp, ops);
    }
    t.join();

    return max_tp;
}

double bench_rdtsc()
{
    double max_tp = 0;
    std::atomic<size_t> times{0};
    std::atomic<bool> finished{false};

    std::thread t([&times, &finished]() {
        size_t magic = 0;
        for (size_t i = 0; i < kTestTime; ++i)
        {
            auto before = rdtsc();
            times.fetch_add(1, std::memory_order_relaxed);
            auto after = rdtsc();
            auto diff = after - before;
            magic += diff;
        }
        LOG(INFO) << "[bench] ignore me. magic: " << magic;
        finished = true;
    });

    while (!finished)
    {
        auto before_time = std::chrono::steady_clock::now();
        auto before = times.load(std::memory_order_relaxed);
        usleep(100 * 1000);
        auto after = times.load(std::memory_order_relaxed);
        auto after_time = std::chrono::steady_clock::now();
        auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                      after_time - before_time)
                      .count();
        auto op = after - before;
        double ops = 1.0 * 1e9 * op / ns;
        LOG(INFO) << "[bench] op: " << op << ", ns: " << ns << ", ops: " << ops;
        max_tp = std::max(max_tp, ops);
    }
    t.join();

    return max_tp;
}

double bench_chrono()
{
    double max_tp = 0;
    std::atomic<size_t> times{0};
    std::atomic<bool> finished{false};

    std::thread t([&times, &finished]() {
        size_t magic = 0;
        for (size_t i = 0; i < kTestTime; ++i)
        {
            auto before = std::chrono::steady_clock::now();
            times.fetch_add(1, std::memory_order_relaxed);
            auto after = std::chrono::steady_clock::now();
            auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                          after - before)
                          .count();
            magic += ns;
        }
        LOG(INFO) << "[bench] ignore me. magic: " << magic;
        finished = true;
    });

    while (!finished)
    {
        auto before_time = std::chrono::steady_clock::now();
        auto before = times.load(std::memory_order_relaxed);
        usleep(100 * 1000);
        auto after = times.load(std::memory_order_relaxed);
        auto after_time = std::chrono::steady_clock::now();
        auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                      after_time - before_time)
                      .count();
        auto op = after - before;
        double ops = 1.0 * 1e9 * op / ns;
        LOG(INFO) << "[bench] op: " << op << ", ns: " << ns << ", ops: " << ops;
        max_tp = std::max(max_tp, ops);
    }
    t.join();

    return max_tp;
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    task_map_of_ddl();
    bench_compare_times_per_sec();
    bench_time_point_modification();
    LOG(INFO) << "finished. ctrl+C to quit.";
}