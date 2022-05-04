#include <algorithm>
#include <chrono>
#include <random>
#include <thread>

#include "Timer.h"
#include "gflags/gflags.h"
#include "patronus/Patronus.h"
#include "util/PerformanceReporter.h"
#include "util/monitor.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

using namespace std::chrono_literals;
using namespace define::literals;

// Two nodes
// one node issues cas operations

constexpr uint16_t kClientNodeId = 0;
[[maybe_unused]] constexpr uint16_t kServerNodeId = 1;
constexpr uint32_t kMachineNr = 2;
constexpr static size_t kTestTime = 100;

using namespace patronus;
constexpr static size_t kCoroCnt = 1;
thread_local CoroCall workers[kCoroCnt];
thread_local CoroCall master;

constexpr static uint64_t kMagic = 0xaabbccdd11223344;
constexpr static uint64_t kKey = 0;

constexpr static auto kExpectLeaseAliveTime = 10ms;
constexpr static uint64_t kWaitKey = 0;

using namespace std::chrono_literals;

struct Object
{
    uint64_t target;
    uint64_t unused_1;
    uint64_t unused_2;
    uint64_t unused_3;
};

uint64_t bench_locator(uint64_t key)
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
    auto key = kKey;

    CoroContext ctx(tid, &yield, &master, coro_id);

    client_comm.still_has_work[coro_id] = true;
    client_comm.finish_cur_task[coro_id] = false;
    client_comm.finish_all_task[coro_id] = false;

    OnePassMonitor read_loop_nr_m;
    OnePassMonitor read_loop_ns_m;

    for (size_t i = 0; i < kTestTime; ++i)
    {
        DVLOG(2) << "[bench] client coro " << ctx << " start to got lease ";
        auto locate_offset = bench_locator(key);
        Lease lease = p->get_rlease(kServerNodeId,
                                    dir_id,
                                    GlobalAddress(0, locate_offset),
                                    0 /* alloc_hint */,
                                    sizeof(Object),
                                    1ms,
                                    0 /* no flag */,
                                    &ctx);
        CHECK(lease.success());

        DVLOG(2) << "[bench] client coro " << ctx << " got lease " << lease;

        auto rdma_buf = p->get_rdma_buffer(sizeof(Object));
        CHECK_GE(rdma_buf.size, sizeof(Object));
        memset(rdma_buf.buffer, 0, sizeof(Object));

        VLOG(2) << "[bench] client coro " << ctx << " start to read";
        CHECK_LT(sizeof(Object), rdma_buf.size);

        auto before_read_loop = std::chrono::steady_clock::now();
        size_t read_loop_succ_cnt = 0;
        while (true)
        {
            auto ec = p->read(lease,
                              rdma_buf.buffer,
                              sizeof(Object),
                              0 /*offset*/,
                              (flag_t) RWFlag::kWithAutoExtend,
                              &ctx);
            if (ec == RetCode::kOk)
            {
                read_loop_succ_cnt++;
            }
            else
            {
                break;
            }
            auto now = std::chrono::steady_clock::now();
            if (now - before_read_loop > kExpectLeaseAliveTime)
            {
                // will not let it extend infiniately.
                break;
            }
        }
        auto after_read_loop = std::chrono::steady_clock::now();
        auto read_loop_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                after_read_loop - before_read_loop)
                .count();
        read_loop_ns_m.collect(read_loop_ns);
        read_loop_nr_m.collect(read_loop_succ_cnt);

        // make sure this will take no harm.
        p->relinquish(lease, 0 /* hint */, 0 /* flag */, &ctx);

        p->put_rdma_buffer(rdma_buf);
    }

    LOG(INFO) << "[bench] read_loop_succ_nr: " << read_loop_nr_m
              << ", read_loop_ns: " << read_loop_ns_m;
    // average should be very close to @kExpectLeaseAliveTime
    auto alive_avg = read_loop_ns_m.average();
    auto expect_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         kExpectLeaseAliveTime)
                         .count();
    double err = 1.0 * abs(expect_ns - alive_avg) / expect_ns;
    CHECK_LT(err, 0.01) << "** Expect alive " << expect_ns << " ns, actual "
                        << alive_avg << " ns. err: " << err
                        << " larger than threshold 0.01";

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

        auto nr = p->try_get_client_continue_coros(coro_buf, 2 * kCoroCnt);
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

    p->finished(kWaitKey);
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
    auto internal_buffer = p->get_server_internal_buffer();
    auto *buffer = internal_buffer.buffer;
    auto offset = bench_locator(kKey);
    auto &object = *(Object *) &buffer[offset];
    object.target = kMagic;

    p->server_serve(kWaitKey);
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

    sleep(1);

    // let client spining
    auto nid = patronus->get_node_id();
    if (nid == kClientNodeId)
    {
        patronus->registerClientThread();
        sleep(2);
        client(patronus);
        patronus->finished(kWaitKey);
    }
    else
    {
        patronus->registerServerThread();
        patronus->finished(kWaitKey);
        server(patronus);
    }

    LOG(INFO) << "finished. ctrl+C to quit.";
}