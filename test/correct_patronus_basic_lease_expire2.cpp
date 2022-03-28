#include <algorithm>
#include <chrono>
#include <random>
#include <thread>

#include "Timer.h"
#include "patronus/Patronus.h"
#include "util/PerformanceReporter.h"
#include "util/monitor.h"
using namespace std::chrono_literals;
using namespace define::literals;

DEFINE_string(exec_meta, "", "The meta data of this execution");

// Two nodes
// one node issues cas operations

constexpr uint16_t kClientNodeId = 0;
[[maybe_unused]] constexpr uint16_t kServerNodeId = 1;
constexpr uint32_t kMachineNr = 2;
// dont be too large, because each test require QP recover, which may be slow
constexpr static size_t kTestTime = 200;

using namespace patronus;
constexpr static size_t kCoroCnt = 1;
thread_local CoroCall workers[kCoroCnt];
thread_local CoroCall master;

constexpr static uint64_t kMagic = 0xaabbccdd11223344;
constexpr static uint64_t kKey = 0;

constexpr static auto kInitialLeasePeriod = 100us;
constexpr static auto kExtendLeasePeriod = 10ms;

using namespace std::chrono_literals;

struct Object
{
    uint64_t target;
    uint64_t unused_1;
    uint64_t unused_2;
    uint64_t unused_3;
};

uint64_t bench_locator(key_t key)
{
    return key * sizeof(Object);
}

struct ClientCommunication
{
    bool still_has_work[kCoroCnt];
    bool finish_cur_task[kCoroCnt];
    bool finish_all_task[kCoroCnt];
} client_comm;

void client_worker(Patronus::pointer p, coro_t coro_id, CoroYield &yield)
{
    auto tid = p->get_thread_id();
    auto dir_id = tid;
    auto &syncer = p->time_syncer();

    CoroContext ctx(tid, &yield, &master, coro_id);

    client_comm.still_has_work[coro_id] = true;
    client_comm.finish_cur_task[coro_id] = false;
    client_comm.finish_all_task[coro_id] = false;

    OnePassMonitor fail_to_unbind_m;
    OnePassMonitor remain_ddl_m;
    OnePassMonitor extend_fail_m;
    OnePassMonitor extend_fail_to_work_m;

    auto dsm = p->get_dsm();

    for (size_t i = 0; i < kTestTime; ++i)
    {
        auto max_key = p->lease_buffer_size() / sizeof(Object);
        auto key = rand() % (max_key - 1);
        auto before_get_rlease = std::chrono::steady_clock::now();

        auto bench_offset = bench_locator(key);
        Lease lease = p->get_rlease(GlobalAddress(kServerNodeId, bench_offset),
                                    dir_id,
                                    sizeof(Object),
                                    kInitialLeasePeriod,
                                    0 /* no flag */,
                                    // (uint8_t) AcquireRequestFlag::kNoGc,
                                    &ctx);
        auto after_get_rlease = std::chrono::steady_clock::now();
        auto get_rlease_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                after_get_rlease - before_get_rlease)
                .count();
        CHECK(lease.success())
            << "Failed to successfully get lease. err: " << lease.ec()
            << ", key: " << key
            << ". dsm_buffer_size: " << p->lease_buffer_size()
            << ", object_size: " << sizeof(Object) << ", max_key: " << max_key;

        auto lease_ddl = lease.ddl_term();
        auto now = syncer.patronus_now();
        time::ns_t diff_ns = lease_ddl - now;
        DVLOG(6) << "The term of lease is " << lease_ddl << ", now is " << now
                 << ", DDL remains " << diff_ns
                 << " ns. Latency of get_rlease: " << get_rlease_ns
                 << " ns. for key " << key;

        // NOTE: time drift too high to satisfy this assert
        // CHECK_LT(
        //     diff_ns,
        //     std::chrono::duration_cast<std::chrono::nanoseconds>(1ms + 200us)
        //         .count())
        //     << "** The period of lease is even longer than requested.";

        remain_ddl_m.collect(diff_ns);

        // DVLOG(2) << "[bench] client coro " << ctx << " got lease " << lease
        //          << " for key " << key;

        auto rdma_buf = p->get_rdma_buffer(sizeof(Object));
        CHECK_GE(rdma_buf.size, sizeof(Object));
        memset(rdma_buf.buffer, 0, sizeof(Object));

        auto extend_ec = p->extend(lease, kExtendLeasePeriod, 0, &ctx);
        if (unlikely(extend_ec != ErrCode::kSuccess))
        {
            LOG(WARNING) << "[bench] extend failed for key: " << key
                         << ", ec: " << extend_ec << ", lease: " << lease;
            extend_fail_m.collect(1);
            p->relinquish(lease, 0, &ctx);
            continue;
        }
        else
        {
            extend_fail_m.collect(0);
        }

        // to make sure that the extend really work!
        std::this_thread::sleep_for(kInitialLeasePeriod);
        auto ec = p->read(lease, rdma_buf.buffer, sizeof(Object), 0, 0, &ctx);
        if (unlikely(ec != ErrCode::kSuccess))
        {
            DVLOG(3) << "[bench] extend_fail_to_work for key = " << key
                     << ", ec: " << ec << ", lease: " << lease;
            extend_fail_to_work_m.collect(1);
            p->relinquish(lease, 0, &ctx);
            continue;
        }
        else
        {
            extend_fail_to_work_m.collect(0);
        }

        // should be enough for the server to gc the mw
        std::this_thread::sleep_for(kExtendLeasePeriod);

        ec = p->read(lease,
                     rdma_buf.buffer,
                     sizeof(Object),
                     0,
                     (uint8_t) RWFlag::kNoLocalExpireCheck,
                     &ctx);
        if (unlikely(ec == ErrCode::kSuccess))
        {
            DVLOG(3) << "[bench] server failed to unbind for key = " << key
                     << ", ec: " << ec << ", lease: " << lease;
            fail_to_unbind_m.collect(1);
        }
        else
        {
            fail_to_unbind_m.collect(0);
        }

        DVLOG(2) << "[bench] client coro " << ctx
                 << " start to relinquish lease ";

        // make sure this will take no harm.
        p->relinquish(lease, 0, &ctx);

        p->put_rdma_buffer(rdma_buf);
    }

    LOG(INFO) << "[bench] remain_ddl: " << remain_ddl_m;
    LOG_IF(ERROR, fail_to_unbind_m.max() > 0)
        << "[bench] in some tests, server fails to expire the lease "
           "immediately. fail_to_unbind_nr: "
        << fail_to_unbind_m;
    LOG_IF(ERROR, extend_fail_m.max() > 0)
        << "[bench] p->extend(lease) fails in some cases. extend_fail_m: "
        << extend_fail_m << ". Fail case_nr: " << extend_fail_m.sum();
    LOG_IF(ERROR, extend_fail_to_work_m.max() > 0)
        << "[bench] p->extend(lease) succeeded but the lease does not live so "
           "long in some cases. extend_fail_to_work_m: "
        << extend_fail_to_work_m;
    CHECK_EQ(extend_fail_m.max(), 0)
        << "** extend can not failed to work. extend_fail_m: " << extend_fail_m;

    client_comm.still_has_work[coro_id] = false;
    client_comm.finish_cur_task[coro_id] = true;
    client_comm.finish_all_task[coro_id] = true;

    LOG(WARNING) << "worker coro " << (int) coro_id
                 << " finished ALL THE TASK. yield to master.";

    ctx.yield_to_master();
}

void client_master(Patronus::pointer p, CoroYield &yield)
{
    auto tid = p->get_thread_id();
    auto mid = tid;

    CoroContext mctx(tid, &yield, workers);
    CHECK(mctx.is_master());

    for (size_t i = 0; i < kCoroCnt; ++i)
    {
        mctx.yield_to_worker(i);
    }
    LOG(INFO) << "Return back to master. start to recv messages";
    coro_t coro_buf[2 * kCoroCnt];
    while (!std::all_of(std::begin(client_comm.finish_all_task),
                        std::end(client_comm.finish_all_task),
                        [](bool i) { return i; }))
    {
        // try to see if messages arrived

        auto nr = p->try_get_client_continue_coros(mid, coro_buf, 2 * kCoroCnt);
        for (size_t i = 0; i < nr; ++i)
        {
            auto coro_id = coro_buf[i];
            DVLOG(1) << "[bench] yielding due to CQE";
            mctx.yield_to_worker(coro_id);
        }

        for (size_t i = 0; i < kCoroCnt; ++i)
        {
            if (client_comm.finish_cur_task[i] &&
                !client_comm.finish_all_task[i])
            {
                DVLOG(1) << "[bench] yielding to coro " << (int) i
                         << " for new task";
                mctx.yield_to_worker(i);
            }
        }
    }

    p->finished();
    LOG(WARNING) << "[bench] all worker finish their work. exiting...";
}

void client(Patronus::pointer p)
{
    auto tid = p->get_thread_id();
    LOG(INFO) << "I am client. tid " << tid;
    for (size_t i = 0; i < kCoroCnt; ++i)
    {
        workers[i] =
            CoroCall([p, i](CoroYield &yield) { client_worker(p, i, yield); });
    }
    master = CoroCall([p](CoroYield &yield) { client_master(p, yield); });
    master();
}

void server(Patronus::pointer p)
{
    auto tid = p->get_thread_id();
    auto mid = tid;

    auto internal_buffer = p->get_server_internal_buffer();
    auto *buffer = internal_buffer.buffer;
    auto offset = bench_locator(kKey);
    auto &object = *(Object *) &buffer[offset];
    object.target = kMagic;

    LOG(INFO) << "[bench] server starts to serve";
    p->server_serve(mid);
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    bindCore(0);

    rdmaQueryDevice();

    PatronusConfig config;
    config.machine_nr = kMachineNr;

    auto patronus = Patronus::ins(config);

    // let client spining
    auto nid = patronus->get_node_id();
    if (nid == kClientNodeId)
    {
        patronus->registerClientThread();
        client(patronus);
        patronus->finished();
    }
    else
    {
        patronus->registerServerThread();
        patronus->finished();
        server(patronus);
    }

    LOG(INFO) << "finished. ctrl+C to quit.";
}