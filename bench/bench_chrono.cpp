#include <algorithm>
#include <map>

#include "Common.h"
#include "DSM.h"
#include "Timer.h"
#include "gflags/gflags.h"
#include "util/PerformanceReporter.h"
#include "util/monitor.h"

using namespace define::literals;

DEFINE_string(exec_meta, "", "The meta data of this execution");

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

size_t bench_steady_clock_compare_times_per_sec()
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
    LOG(INFO) << "[bench_compare_times_per_sec] std::chrono::steady_clock op: "
              << times << ", ns: " << ns << ", ops: " << 1.0 * times * 1e9 / ns;
    return times;
}
size_t bench_system_clock_compare_times_per_sec()
{
    PerformanceReporter reporter("std::chrono::system_clock compare + ::now()");
    auto now = std::chrono::system_clock::now();
    auto ddl = now + 1s;
    reporter.bench_task(
        1, [ddl](size_t, std::atomic<uint64_t> &op, std::atomic<bool> &finish) {
            auto now = std::chrono::system_clock::now();
            size_t task = 0;
            while (now < ddl)
            {
                now = std::chrono::system_clock::now();
                task++;
                if (task >= 50)
                {
                    task -= 50;
                    op.fetch_add(50, std::memory_order_relaxed);
                }
            }
            finish = true;
        });
    reporter.start(100ms, true);
    reporter.wait();
    LOG(INFO) << reporter;
    return reporter.avg_ops();
}

double bench_steady_clock_now()
{
    PerformanceReporter reporter("std::chrono::steady_clock::now()");
    reporter.bench_task(
        1, [](size_t, std::atomic<uint64_t> &op, std::atomic<bool> &finish) {
            auto now = std::chrono::steady_clock::now();
            size_t remain_task = 50_M;
            while (remain_task > 0)
            {
                size_t task = std::min(remain_task, size_t(50));
                for (size_t i = 0; i < task; ++i)
                {
                    now = std::chrono::steady_clock::now();
                }
                op.fetch_add(task, std::memory_order_relaxed);
                remain_task -= task;
            }
            finish = true;
        });
    reporter.start(100ms, true);
    reporter.wait();
    LOG(WARNING) << reporter;
    return reporter.avg_ops();
}

double bench_system_clock_now()
{
    PerformanceReporter reporter("std::chrono::system_clock::now()");
    reporter.bench_task(
        1, [](size_t, std::atomic<uint64_t> &op, std::atomic<bool> &finish) {
            auto now = std::chrono::system_clock::now();
            size_t remain_task = 50_M;
            while (remain_task > 0)
            {
                size_t task = std::min(remain_task, size_t(50));
                for (size_t i = 0; i < task; ++i)
                {
                    now = std::chrono::system_clock::now();
                }
                op.fetch_add(task, std::memory_order_relaxed);
                remain_task -= task;
            }
            finish = true;
        });
    reporter.start(100ms, true);
    reporter.wait();
    LOG(WARNING) << reporter;
    return reporter.avg_ops();
}

double bench_steady_clock_time_point_modification()
{
    PerformanceReporter reporter(
        "std::chrono::steady_clock time_point modification");

    reporter.bench_task(
        1, [](size_t, std::atomic<uint64_t> &op, std::atomic<bool> &finished) {
            size_t magic = 0;
            auto now = std::chrono::steady_clock::now();
            auto remember = now;

            constexpr static size_t kThisTestTime = 100 * kTestTime;
            size_t remain_work = kThisTestTime;
            while (remain_work > 0)
            {
                size_t task = std::min(remain_work, size_t(50));
                for (size_t i = 0; i < task; ++i)
                {
                    now += 1ns;
                }
                op.fetch_add(task, std::memory_order_relaxed);
                remain_work -= task;
            }
            magic = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        now.time_since_epoch())
                        .count();
            LOG(INFO) << "[bench] ignore me. magic: " << magic;

            auto diff_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                               now - remember)
                               .count();
            CHECK_EQ(diff_ns, kThisTestTime);

            finished = true;
        });

    reporter.start(100ms, true);

    reporter.wait();
    LOG(WARNING) << reporter;

    return reporter.avg_ops();
}

double bench_system_clock_time_point_modification()
{
    PerformanceReporter reporter(
        "std::chrono::system_clock time_point modification");

    reporter.bench_task(
        1, [](size_t, std::atomic<uint64_t> &op, std::atomic<bool> &finished) {
            size_t magic = 0;
            auto now = std::chrono::system_clock::now();
            auto remember = now;
            constexpr static size_t this_test_time = 100 * kTestTime;
            size_t remain_task = this_test_time;
            while (remain_task > 0)
            {
                size_t task = std::min(remain_task, size_t(50));
                for (size_t i = 0; i < task; ++i)
                {
                    now += 1ns;
                }
                op.fetch_add(task, std::memory_order_relaxed);
                remain_task -= task;
            }
            magic = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        now.time_since_epoch())
                        .count();
            LOG(INFO) << "[bench] ignore me. magic: " << magic;
            auto diff_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                               now - remember)
                               .count();
            CHECK_EQ(diff_ns, this_test_time);
            finished = true;
        });

    reporter.start(100ms, true);

    reporter.wait();
    LOG(WARNING) << reporter;

    return reporter.avg_ops();
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
        LOG(INFO) << "[bench] rdtsc: op: " << op << ", ns: " << ns
                  << ", ops: " << ops;
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

void explain_system_clock()
{
    auto now = std::chrono::system_clock::now();
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                  now.time_since_epoch())
                  .count();
    auto us = ns / 1_K;
    auto ms = us / 1_K;
    auto s = ms / 1_K;
    auto hours =
        std::chrono::duration_cast<std::chrono::hours>(now.time_since_epoch())
            .count();
    auto days = hours / 24;
    auto month = days / 30;
    auto years = days / 365;
    LOG(INFO) << "Now: " << years << " years, " << month << " months, " << days
              << " days, " << hours << " hours, " << s << " s, " << ms
              << " ms, " << us << " us, " << ns << " ns";
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    explain_system_clock();
    bench_steady_clock_compare_times_per_sec();

    bench_system_clock_now();
    bench_steady_clock_now();

    bench_steady_clock_time_point_modification();
    bench_system_clock_time_point_modification();

    LOG(INFO) << "finished. ctrl+C to quit.";
}