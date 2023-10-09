#include <algorithm>

#include "DSM.h"
#include "Timer.h"
#include "gflags/gflags.h"
#include "util/monitor.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

using namespace util::literals;

constexpr static size_t kTestTime = 1_M;
constexpr static size_t kCoroNr = 16;
constexpr static size_t kLargeStackSize = 16_KB;

class Scope
{
public:
    Scope()
    {
        DLOG(INFO) << "scope ctor";
    }
    ~Scope()
    {
        DLOG(INFO) << "scop ~ctor";
    }
};

struct Comm
{
    std::array<bool, kTestTime> finished{};
    CoroCall workers[kCoroNr];
    CoroCall master;
    bool all_finished{false};
    size_t finished_nr{0};
    size_t yielded_nr{0};
} comm;

void coro_large_stack_master(CoroYield &yield)
{
    CoroContext ctx(0, &yield, comm.workers);
    Timer timer;
    timer.begin();

    std::array<char, kLargeStackSize> large{};

    while (!comm.all_finished)
    {
        for (size_t i = 0; i < kCoroNr; ++i)
        {
            if (!comm.finished[i])
            {
                comm.yielded_nr++;
                large[i] = 0;
                ctx.yield_to_worker(i);
            }
        }
    }
    auto ns = timer.end();
    LOG(INFO) << "[bench] large stack: op: " << comm.yielded_nr
              << ", ns: " << ns
              << ", ops: " << 1.0 * 1e9 * comm.yielded_nr / ns;
}

void coro_large_stack_worker(coro_t coro_id, CoroYield &yield)
{
    CoroContext ctx(0, &yield, &comm.master, coro_id);

    Scope scope;

    for (size_t i = 0; i < kTestTime; ++i)
    {
        comm.yielded_nr++;
        ctx.yield_to_master();
    }
    comm.finished[coro_id] = true;
    comm.finished_nr++;
    if (comm.finished_nr == kCoroNr)
    {
        comm.all_finished = true;
    }
    ctx.yield_to_master();
}
void bench_coro_large_stack()
{
    for (size_t i = 0; i < kCoroNr; ++i)
    {
        comm.workers[i] = CoroCall(
            [i](CoroYield &yield) { coro_large_stack_worker(i, yield); });
    }
    comm.master =
        CoroCall([](CoroYield &yield) { coro_large_stack_master(yield); });

    comm.master();
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    bindCore(0);

    bench_coro_large_stack();

    LOG(INFO) << "finished. ctrl+C to quit.";
}