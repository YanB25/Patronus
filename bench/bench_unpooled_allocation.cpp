#include <algorithm>
#include <random>
#include <set>

#include "Common.h"
#include "Rdma.h"
#include "gflags/gflags.h"
#include "glog/logging.h"

using namespace util::literals;

DEFINE_string(exec_meta, "", "The meta data of this execution");

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    RdmaContext rdma_context;
    CHECK(createContext(&rdma_context));
    void *addr = CHECK_NOTNULL(hugePageAlloc(8_GB));
    CHECK_NOTNULL(createMemoryRegion((uint64_t) addr, 8_GB, &rdma_context));
}