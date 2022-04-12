#include <algorithm>
#include <random>

#include "Timer.h"
#include "patronus/Patronus.h"
#include "util/BenchRand.h"
#include "util/PerformanceReporter.h"
#include "util/monitor.h"

// Two nodes
// one node issues cas operations

DEFINE_string(exec_meta, "", "The meta data of this execution");

[[maybe_unused]] constexpr uint16_t kServerNodeId = 1;

using namespace patronus;

constexpr static size_t kMaxKey = 1_M;
constexpr static size_t kMinKey = 10;

void show_distribution(IKVRandGenerator::pointer alloc, size_t test_size)
{
    OnePassBucketMonitor<uint64_t> m(kMinKey, kMaxKey, 1);
    uint64_t key;
    for (size_t i = 0; i < test_size; ++i)
    {
        alloc->gen_key((char *) &key, sizeof(key));
        m.collect(key);
        if (i < 20)
        {
            LOG(INFO) << "[bench] showing: " << key;
        }
        CHECK_GE(key, kMinKey);
        CHECK_LT(key, kMaxKey);
    }
    for (double p = 0; p <= 1; p += 0.1)
    {
        // auto percentile = m.percentile(p);
        // auto expect = kMaxKey * p;
        // auto error = abs(percentile - expect);
        // double error_rate = 1.0 * error / kMaxKey;
        // LOG(INFO) << "p " << p << " got percentile " << percentile << ",
        // error "
        //           << error << " (calculate from expect: " << expect
        //           << ", max_key: " << kMaxKey << ") "
        //           << ", rate: " << error_rate;
        // LOG(INFO) << "[debug] percentile(" << p << "): " << percentile;
    }
    LOG(INFO) << "[bench] got distribution: " << m;
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    auto ug = UniformRandGenerator::new_instance(kMinKey, kMaxKey);
    show_distribution(ug, 10_M);

    auto zg = MehcachedZipfianRandGenerator::new_instance(kMinKey, kMaxKey);
    show_distribution(zg, 10_M);

    LOG(INFO) << "finished. ctrl+C to quit.";
}