#include <algorithm>
#include <random>

#include "DSM.h"
#include "Magick++.h"
#include "Timer.h"
#include "gflags/gflags.h"
#include "util/monitor.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

using namespace util::literals;

constexpr static size_t kTestTime = 10;
constexpr static size_t kCoroNr = 16;
constexpr static size_t kLargeStackSize = 16_KB;

[[maybe_unused]] constexpr static const char *img_name = "view.thumbnail.jpeg";

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

struct Prv
{
    size_t yielded_nr{0};
    std::unordered_map<uint64_t, Magick::Image> images;
};

void coro_large_stack_master(
    CoroYield &yield,
    [[maybe_unused]] CoroExecutionContextWith<kCoroNr, Prv> &comm)
{
    CoroContext ctx(0, &yield, comm.workers());
    Timer timer;
    timer.begin();

    std::array<char, kLargeStackSize> large{};

    while (!comm.is_finished_all())
    {
        for (size_t i = 0; i < kCoroNr; ++i)
        {
            if (!comm.is_finished(i))
            {
                comm.get_private_data().yielded_nr++;
                large[i] = 0;
                ctx.yield_to_worker(i);
            }
        }
    }
    auto ns = timer.end();
    LOG(INFO) << "[bench] large stack: op: "
              << comm.get_private_data().yielded_nr << ", ns: " << ns
              << ", ops: "
              << 1.0 * 1e9 * comm.get_private_data().yielded_nr / ns;
}

void coro_large_stack_worker(coro_t coro_id,
                             CoroYield &yield,
                             CoroExecutionContextWith<kCoroNr, Prv> &comm)
{
    CoroContext ctx(0, &yield, &comm.master(), coro_id);

    Scope scope;

    for (size_t i = 0; i < kTestTime; ++i)
    {
        Magick::Image img2 = comm.get_private_data().images[0];
        LOG(INFO) << "name: " << img2.fileName()
                  << ", size: " << img2.fileSize();
        img2.zoom({100, 100});
        Magick::Blob blob;
        img2.write(&blob);
        LOG(INFO) << "after zoom: name: " << img2.fileName()
                  << ", size: " << img2.fileSize()
                  << ", size: " << blob.length();

        comm.get_private_data().yielded_nr++;
        ctx.yield_to_master();
    }

    // LOG(INFO) << "Erasing with id: " << (int) coro_id;
    // comm.get_private_data().images.erase(coroid);

    // comm.finished[coro_id] = true;
    // comm.finished_nr++;
    // if (comm.finished_nr == kCoroNr)
    // {
    //     comm.all_finished = true;
    // }
    comm.worker_finished(coro_id);
    ctx.yield_to_master();
}
void bench_coro_large_stack()
{
    CoroExecutionContextWith<kCoroNr, Prv> comm;

    auto img_path = artifacts_directory() / img_name;
    CHECK_EQ(comm.get_private_data().images.count(0), 0);
    comm.get_private_data().images.emplace(0, img_path);

    for (size_t i = 0; i < kCoroNr; ++i)
    {
        auto &worker = comm.worker(i);
        worker = CoroCall([i, &comm](CoroYield &yield) {
            coro_large_stack_worker(i, yield, comm);
        });
    }

    auto &master = comm.master();
    master = CoroCall(
        [&comm](CoroYield &yield) { coro_large_stack_master(yield, comm); });

    master();

    comm.get_private_data().images.clear();
    CHECK(comm.get_private_data().images.empty());
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