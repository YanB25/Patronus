#include <algorithm>
#include <random>

#include "DSM.h"
#include "Timer.h"
#include "util/monitor.h"

constexpr static size_t kTestTime = 1 * define::M;
constexpr static size_t kCoroNr = 16;
constexpr static size_t kLargeStackSize = 16 * define::MB;

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

    std::array<char, kLargeStackSize> large;

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
        comm.workers[i] = CoroCall([i](CoroYield &yield)
                                   { coro_large_stack_worker(i, yield); });
    }
    comm.master =
        CoroCall([](CoroYield &yield) { coro_large_stack_master(yield); });

    comm.master();
}

CoroCall simple_coro_worker[8];
CoroCall simple_coro_master;
size_t count = 0;

void simple_worker(CoroYield &yield, size_t data)
{
    count++;
    LOG(INFO) << "at worker, yield to master. data is " << data << ", count "
              << count;
    yield(simple_coro_master);
}
void simple_master(CoroYield &yield, size_t data)
{
    LOG(INFO) << "at master, yield to worker. data is " << data;
    for (size_t i = 0; i < 8; ++i)
    {
        count++;
        yield(simple_coro_worker[i]);
    }
    LOG(INFO) << "count is " << count;
}
void simple()
{
    size_t data = 10;
    for (size_t i = 0; i < 8; ++i)
    {
        simple_coro_worker[i] =
            CoroCall([data](CoroYield &yield) { simple_worker(yield, data); });
    }
    simple_coro_master =
        CoroCall([data](CoroYield &yield) { simple_master(yield, data); });
    simple_coro_master();
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    LOG(WARNING) << "correct_coro does not *correct*. boost::coroutine (and "
                    "boost::coroutine2) does not work with asan.";
    // auto t = std::thread([]() {
    //     bench_coro_large_stack();
    // });
    // t.join();
    bench_coro_large_stack();
    // simple();

    LOG(INFO) << "finished. ctrl+C to quit.";
}