#include <algorithm>
#include <chrono>
#include <random>

#include "DSM.h"
#include "Timer.h"
#include "gflags/gflags.h"
#include "util/monitor.h"

using namespace std::chrono_literals;

// Two nodes
// one node issues cas operations

[[maybe_unused]] constexpr uint16_t kClientNodeId = 0;
[[maybe_unused]] constexpr uint16_t kServerNodeId = 1;

DEFINE_string(exec_meta, "", "The meta data of this execution");

void accept_duration(std::chrono::nanoseconds ns)
{
    LOG(INFO)
        << "the ns is "
        << std::chrono::duration_cast<std::chrono::nanoseconds>(ns).count();
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    VLOG(1) << "It is 1 vlog";
    VLOG(2) << "It is 2 vlog";
    VLOG(3) << "It is 3 vlog";
    LOG(INFO) << "Support color ? " << getenv("TERM");
    LOG(INFO) << "Hash of 100 is " << std::hash<int>{}(100);

    std::this_thread::sleep_for(5s);

    LOG(INFO) << "finished. ctrl+C to quit.";
}