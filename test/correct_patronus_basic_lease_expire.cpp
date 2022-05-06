#include <algorithm>
#include <chrono>
#include <random>
#include <thread>

#include "Timer.h"
#include "gflags/gflags.h"
#include "patronus/Patronus.h"
#include "util/PerformanceReporter.h"
#include "util/monitor.h"
using namespace std::chrono_literals;
using namespace define::literals;

DEFINE_string(exec_meta, "", "The meta data of this execution");

constexpr static size_t kTestTime = 5_K;

using namespace patronus;
constexpr static size_t kCoroCnt = 1;
thread_local CoroCall workers[kCoroCnt];
thread_local CoroCall master;

constexpr static uint64_t kMagic = 0xaabbccdd11223344;
constexpr static uint64_t kKey = 0;
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
    auto dir_id = tid % NR_DIRECTORY;
    auto key = kKey;
    auto &syncer = p->time_syncer();

    auto server_nid = ::config::get_server_nids().front();

    CoroContext ctx(tid, &yield, &master, coro_id);

    client_comm.still_has_work[coro_id] = true;
    client_comm.finish_cur_task[coro_id] = false;
    client_comm.finish_all_task[coro_id] = false;

    OnePassMonitor read_nr_before_expire_m;
    OnePassMonitor fail_to_unbind_m;
    OnePassMonitor remain_ddl_m;

    for (size_t i = 0; i < kTestTime; ++i)
    {
        DVLOG(2) << "[bench] client coro " << ctx << " start to got lease ";
        auto before_get_rlease = std::chrono::steady_clock::now();
        auto locate_offset = bench_locator(key);
        Lease lease = p->get_rlease(server_nid,
                                    dir_id,
                                    GlobalAddress(0, locate_offset),
                                    0 /* alloc_hint */,
                                    sizeof(Object),
                                    100us,
                                    0 /* no flag */,
                                    &ctx);
        auto after_get_rlease = std::chrono::steady_clock::now();
        auto get_rlease_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                after_get_rlease - before_get_rlease)
                .count();
        CHECK(lease.success());

        auto lease_ddl = lease.ddl_term();
        auto now = syncer.patronus_now();
        time::ns_t diff_ns = lease_ddl - now;
        VLOG(2) << "The term of lease is " << lease_ddl << ", now is " << now
                << ", DDL remains " << diff_ns
                << " ns. Latency of get_rlease: " << get_rlease_ns << " ns";
        remain_ddl_m.collect(diff_ns);

        DVLOG(2) << "[bench] client coro " << ctx << " got lease " << lease;

        auto rdma_buf = p->get_rdma_buffer(sizeof(Object));
        CHECK_GE(rdma_buf.size, sizeof(Object));
        memset(rdma_buf.buffer, 0, sizeof(Object));

        VLOG(2) << "[bench] client coro " << ctx << " start to read";
        CHECK_LT(sizeof(Object), rdma_buf.size);

        size_t read_nr = 0;
        for (size_t t = 0; t < 100; ++t)
        {
            auto ec = p->read(lease,
                              rdma_buf.buffer,
                              sizeof(Object),
                              0 /* offset */,
                              0 /* flag */,
                              &ctx);
            if (ec != RetCode::kOk)
            {
                auto patronus_now = syncer.patronus_now();
                time::ns_t ns_diff = lease.ddl_term() - patronus_now;
                if (ns_diff <= syncer.epsilon())
                {
                    VLOG(2) << "[bench] read fails because of lease expired";
                }
                else
                {
                    CHECK_EQ(ec, RetCode::kOk)
                        << "[bench] " << i << "-th, read " << t
                        << "-th failed. lease until DDL: " << ns_diff
                        << ", patronus_now: " << patronus_now;
                }
            }
            else
            {
                read_nr++;
                DVLOG(2) << "[bench] client coro " << ctx << " read finished";
                Object magic_object = *(Object *) rdma_buf.buffer;
                CHECK_EQ(magic_object.target, kMagic)
                    << "coro_id " << ctx << ", Read at key " << key
                    << ", lease.base: " << (void *) lease.base_addr();
            }
        }
        read_nr_before_expire_m.collect(read_nr);

        // to make sure the server really gc the lea se
        std::this_thread::sleep_for(100us +
                                    std::chrono::nanoseconds(syncer.epsilon()));
        // the lease is expired (maybe by client's decision)
        // force to send here
        // if actually issue read, QP should fails
        VLOG(3) << "[bench] before read, patronus_time: "
                << syncer.patronus_now();
        auto ec = p->read(lease,
                          rdma_buf.buffer,
                          sizeof(Object),
                          0,
                          (flag_t) RWFlag::kNoLocalExpireCheck,
                          &ctx);

        if (ec == RetCode::kOk)
        {
            fail_to_unbind_m.collect(1);
        }
        else
        {
            fail_to_unbind_m.collect(0);
        }

        DVLOG(2) << "[bench] client coro " << ctx
                 << " start to relinquish lease ";

        // make sure this will take no harm.
        p->relinquish(lease, 0 /* hint */, 0 /* flag */, &ctx);

        p->put_rdma_buffer(rdma_buf);
    }

    LOG(INFO) << "[bench] remain_ddl: " << remain_ddl_m;
    LOG(INFO) << "[bench] read_nr before lease expire: "
              << read_nr_before_expire_m;

    LOG_IF(ERROR, fail_to_unbind_m.max() > 0)
        << "[bench] in some tests, server fails to expire the lease "
           "immediately. fail_to_unbind_nr: "
        << fail_to_unbind_m << ". failed_nr: " << fail_to_unbind_m.sum();

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
    config.machine_nr = ::config::kMachineNr;

    auto patronus = Patronus::ins(config);

    auto nid = patronus->get_node_id();
    if (::config::is_client(nid))
    {
        patronus->registerClientThread();
        patronus->keeper_barrier("begin", 100ms);
        client(patronus);
        patronus->finished(kWaitKey);
    }
    else
    {
        patronus->registerServerThread();
        patronus->finished(kWaitKey);
        patronus->keeper_barrier("begin", 100ms);
        server(patronus);
    }

    LOG(INFO) << "finished. ctrl+C to quit.";
}