#include <algorithm>
#include <random>

#include "DSM.h"
#include "Timer.h"
#include "boost/coroutine2/all.hpp"
#include "gflags/gflags.h"
#include "util/monitor.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

constexpr static size_t kTestTime = 1 * define::M;
constexpr static size_t kCoroNr = 16;
constexpr static size_t kLargeStackSize = 16 * define::MB;

using coro2_t = boost::coroutines2::coroutine<void>;

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
    std::array<bool, kTestTime> finished;
    std::vector<coro2_t::push_type> workers;
    bool all_finished{false};
    size_t finished_nr{0};
    size_t yielded_nr{0};
} comm;

void coro_large_stack_worker(coro_t coro_id, coro2_t::pull_type &out)
{
    Scope scope;
    std::array<char, kLargeStackSize> buffer;
    for (size_t i = 0; i < kTestTime; ++i)
    {
        comm.yielded_nr++;
        buffer[i % kLargeStackSize] = 0;
        out();
    }
    comm.finished[coro_id] = true;
    comm.finished_nr++;
    if (comm.finished_nr == kCoroNr)
    {
        comm.all_finished = true;
    }
    out();
}
void bench_coro_large_stack()
{
    for (size_t i = 0; i < kCoroNr; ++i)
    {
        comm.workers.emplace_back(coro2_t::push_type(
            [i](coro2_t::pull_type &out) { coro_large_stack_worker(i, out); }));
    }

    Timer timer;
    timer.begin();
    while (!comm.all_finished)
    {
        for (size_t i = 0; i < kCoroNr; ++i)
        {
            if (!comm.finished[i])
            {
                comm.workers[i]();
            }
        }
    }
    auto ns = timer.end();
    auto op = comm.yielded_nr;
    LOG(INFO) << "[bench] op: " << op << ", ns " << ns
              << ", ops: " << 1.0 * 1e9 * op / ns;
}

CoroCall simple_coro_worker[8];
CoroCall simple_coro_master;
size_t count = 0;

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    bindCore(0);

    // bench_coro_large_stack();
    LOG(WARNING) << "not work, crash, but won't fix";

    LOG(INFO) << "finished. ctrl+C to quit.";
}