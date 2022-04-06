#include <algorithm>
#include <random>

#include "Common.h"
#include "util/PerformanceReporter.h"
#include "util/Rand.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

using namespace define::literals;

constexpr static double kEpsilon = 0.0001;

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    OnePassBucketMonitor<double> m(0, 1, 0.0001);
    for (size_t i = 0; i < 10_M; ++i)
    {
        m.collect(fast_pseudo_rand_dbl(1));
    }
    LOG(INFO) << m;
    for (double i = 0; i <= 1; i += 0.1)
    {
        auto pi = m.percentile(i);
        if (abs(pi - i) >= 3 * kEpsilon)
        {
            CHECK(false) << "m.percentile(" << i << ") got " << pi
                         << ", expect to be " << i << ", larger than allowed "
                         << 3 * kEpsilon;
        }
    }
    CHECK_DOUBLE_EQ(m.percentile(0.99), 0.99);
    CHECK_DOUBLE_EQ(m.percentile(0.999), 0.999);

    LOG(INFO) << "finished. ctrl+C to quit.";
}