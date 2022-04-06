#include <algorithm>
#include <random>

#include "Common.h"
#include "util/PerformanceReporter.h"
#include "util/Rand.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

using namespace define::literals;

constexpr static size_t kTestSize = 10_M;
constexpr static size_t kBatchSize = 1000;

void test(size_t size)
{
    std::vector<uint64_t> origin;
    origin.reserve(kTestSize);
    for (size_t i = 0; i < kTestSize; ++i)
    {
        origin.push_back(fast_pseudo_rand_int());
    }

    Sequence<uint64_t, kBatchSize> seq;
    CHECK(seq.empty());
    CHECK_EQ(seq.size(), 0);
    for (size_t i = 0; i < origin.size(); ++i)
    {
        seq.push_back(origin[i]);
    }
    auto got = seq.to_vector();
    CHECK_EQ(got.size(), origin.size());

    for (size_t i = 0; i < got.size(); ++i)
    {
        CHECK_EQ(got[i], origin[i]) << "unequal at " << i;
    }
    CHECK_EQ(got.front(), origin.front());
    CHECK_EQ(got.back(), origin.back());

    CHECK_EQ(seq.size(), kTestSize);
}
int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    test(10_M);
    test(0);
    test(1);
    test(9);
    test(kBatchSize - 5);
    test(kBatchSize - 1);
    test(kBatchSize + 1);
    test(kBatchSize + 5);

    LOG(INFO) << "finished. ctrl+C to quit.";
}