#include <algorithm>
#include <random>

#include "DSM.h"
#include "Timer.h"
#include "util/monitor.h"

constexpr static size_t kTestTime = 100 * define::M;
DEFINE_string(exec_meta, "", "The meta data of this execution");

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    std::atomic<size_t> times{0};
    std::atomic<bool> finished{false};

    std::thread t([&times, &finished]() {
        size_t magic = 0;
        for (size_t i = 0; i < kTestTime; ++i)
        {
            auto before = rdtsc();
            times.fetch_add(1, std::memory_order_relaxed);
            auto after = rdtsc();
            magic += (after - before);
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
        LOG(INFO) << "[bench] op: " << op << ", ns: " << ns
                  << ", ops: " << 1.0 * 1e9 * op / ns;
    }
    t.join();

    LOG(INFO) << "finished. ctrl+C to quit.";
}