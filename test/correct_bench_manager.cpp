#include <algorithm>
#include <random>

#include "Timer.h"
#include "boost/thread/barrier.hpp"
#include "gflags/gflags.h"
#include "patronus/Patronus.h"
#include "patronus/bench_manager.h"
#include "util/monitor.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

using namespace patronus;
constexpr static size_t kServerThreadNr = NR_DIRECTORY;
constexpr static size_t kClientThreadNr = 1;
// constexpr static size_t kClientThreadNr = 1;

static_assert(kClientThreadNr <= kMaxAppThread);
static_assert(kServerThreadNr <= NR_DIRECTORY);
constexpr static size_t kCoroCnt = 16;
// constexpr static size_t kCoroCnt = 1;

constexpr static uint64_t kMagic = 0xaabbccdd11223344;
constexpr static size_t kCoroStartKey = 1024;

// constexpr static size_t kTestTime =
//     Patronus::kMwPoolSizePerThread / kCoroCnt / NR_DIRECTORY / 2;
constexpr static size_t kTestTime = 100;

using namespace std::chrono_literals;

struct Object
{
    uint64_t target;
    uint64_t unused_1;
    uint64_t unused_2;
    uint64_t unused_3;
};

struct Config
{
    size_t wait_key{0};
};

uint64_t bench_locator(uint64_t key)
{
    return key * sizeof(Object);
}
inline size_t gen_coro_key(size_t thread_id, size_t coro_id)
{
    return kCoroStartKey + thread_id * kCoroCnt + coro_id;
}
inline uint64_t gen_magic(size_t thread_id, size_t coro_id)
{
    return kMagic + thread_id * kCoroCnt + coro_id;
}

void client_worker(Patronus::pointer p, CoroContext &ctx)
{
    auto coro_id = ctx.coro_id();
    auto tid = p->get_thread_id();
    auto server_nid = ::config::get_server_nids().front();

    auto dir_id = tid % kServerThreadNr;
    CHECK_LT(dir_id, kServerThreadNr);

    LOG(INFO) << "[bench] tid " << tid << ", coro: " << ctx;

    size_t coro_key = gen_coro_key(tid, coro_id);
    [[maybe_unused]] size_t coro_magic = gen_magic(tid, coro_id);

    for (size_t time = 0; time < kTestTime; ++time)
    {
        DVLOG(2) << "[bench] client coro " << ctx << " start to got lease ";
        auto locate_offset = bench_locator(coro_key);
        LOG(INFO) << "[debug] !!! before get lease";
        Lease lease = p->get_rlease(server_nid,
                                    dir_id,
                                    GlobalAddress(0, locate_offset),
                                    0 /* alloc_hint */,
                                    sizeof(Object),
                                    0ns,
                                    (flag_t) AcquireRequestFlag::kNoGc,
                                    &ctx);
        CHECK(lease.success());
        LOG(INFO) << "[debug] !!! after get lease";
        // if (unlikely(!lease.success()))
        // {
        //     CHECK_EQ(lease.ec(), AcquireRequestStatus::kMagicMwErr);
        //     // DLOG(ERROR) << "[bench] client coro " << ctx
        //     //             << " get_rlease failed. retry. Got: " << lease;
        //     continue;
        // }

        DVLOG(2) << "[bench] client coro " << ctx << " got lease " << lease;

        auto rdma_buf = p->get_rdma_buffer(sizeof(Object));
        CHECK_GE(rdma_buf.size, sizeof(Object));
        memset(rdma_buf.buffer, 0, sizeof(Object));

        DVLOG(2) << "[bench] client coro " << ctx << " start to read";
        CHECK_LE(sizeof(Object), rdma_buf.size);
        LOG(INFO) << "[debug] !!! before read";
        auto ec = p->read(lease,
                          rdma_buf.buffer,
                          sizeof(Object),
                          0 /* offset */,
                          0 /* flag */,
                          &ctx);
        CHECK_EQ(ec, RC::kOk)
            << "client READ failed. lease " << lease << ", ctx: " << ctx
            << " at " << time << "-th. Failure: " << ec;
        LOG(INFO) << "[debug] !!! after read";

        // DVLOG(2) << "[bench] client coro " << ctx << " read finished";
        // Object magic_object = *(Object *) rdma_buf.buffer;
        // CHECK_EQ(magic_object.target, coro_magic)
        //     << "coro_id " << ctx << ", Read at key " << coro_key
        //     << " expect magic " << coro_magic
        //     << ", lease.base: " << (void *) lease.base_addr()
        //     << ", actual offset: " << bench_locator(coro_key);

        DVLOG(2) << "[bench] client coro " << ctx
                 << " start to relinquish lease ";
        auto rel_flag = 0;
        LOG(INFO) << "[debug] !!! before relinquish";
        p->relinquish(lease, 0, rel_flag, &ctx);
        LOG(INFO) << "[debug] !!! after relinquish";

        p->put_rdma_buffer(std::move(rdma_buf));
    }
    LOG(WARNING) << "worker coro " << (int) coro_id << ", thread " << tid
                 << " finished ALL THE TASK. yield to master.";
}

using Context = Void;
using CoroComm = Void;

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    rdmaQueryDevice();

    PatronusConfig config;
    config.machine_nr = ::config::kMachineNr;

    auto p = Patronus::ins(config);
    auto nid = p->get_node_id();

    bool is_client = ::config::is_client(nid);

    patronus::bench::PatronusManager<Context, Config, CoroComm> manager(
        p, is_client ? kClientThreadNr : kServerThreadNr, 2);

    std::vector<Config> configs;
    for (size_t wait_key : {0, 1, 2})
    {
        configs.emplace_back(Config{wait_key});
    }

    if (is_client)
    {
        manager.register_task([](Patronus::pointer p,
                                 Context &,
                                 CoroComm &,
                                 const Config &,
                                 CoroContext &ctx,
                                 bool) { client_worker(p, ctx); });
    }
    else
    {
        manager.register_server_task(
            [](Patronus::pointer p, Context &, const Config &config) {
                p->server_serve(config.wait_key);
            });
    }

    manager.bench(configs);

    p->keeper_barrier("finished", 100ms);
    LOG(INFO) << "finished. ctrl+C to quit.";
}