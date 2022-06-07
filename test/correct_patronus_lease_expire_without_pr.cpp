#include <algorithm>
#include <random>

#include "Timer.h"
#include "boost/thread/barrier.hpp"
#include "gflags/gflags.h"
#include "patronus/Patronus.h"
#include "util/PerformanceReporter.h"
#include "util/Pre.h"
#include "util/monitor.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

using namespace patronus;
constexpr static size_t kServerThreadNr = NR_DIRECTORY;
constexpr static size_t kClientThreadNr = kServerThreadNr;

constexpr static size_t kCoroCnt = 1;

constexpr static size_t kTestTime = 100_K;
constexpr static auto kLeaseTime = 1ms;

constexpr static size_t kWaitKey = 0;

using namespace std::chrono_literals;

struct Object
{
    uint64_t target;
    uint64_t unused_1;
    uint64_t unused_2;
    uint64_t unused_3;
};

struct ClientCoro
{
    CoroCall workers[kCoroCnt];
    CoroCall master;
};
thread_local ClientCoro client_coro;

uint64_t locate_key(size_t tid, size_t coro_id)
{
    std::ignore = tid;
    std::ignore = coro_id;
    return 0;
}

std::atomic<size_t> bench_succ_nr{0};
std::atomic<size_t> bench_failed_nr{0};
std::atomic<size_t> bench_total_nr{0};

void client_worker(Patronus::pointer p,
                   coro_t coro_id,
                   CoroYield &yield,
                   CoroExecutionContext<kCoroCnt> &ex)
{
    auto tid = p->get_thread_id();
    auto dir_id = tid % kServerThreadNr;
    auto server_nid = ::config::get_server_nids().front();

    CoroContext ctx(tid, &yield, &client_coro.master, coro_id);

    OnePassMonitor lease_success_m;
    size_t lease_success_nr{0};

    OnePassMonitor latency_m;
    ChronoTimer timer;
    for (size_t time = 0; time < kTestTime; ++time)
    {
        auto key = locate_key(tid, coro_id);

        DVLOG(2) << "[bench] client coro " << ctx << " start to got lease ";
        auto flag = (flag_t) AcquireRequestFlag::kNoBindPR;
        auto before = p->patronus_now();
        ChronoTimer timer;
        Lease lease = p->get_rlease(server_nid,
                                    dir_id,
                                    GlobalAddress(0, key),
                                    0 /* alloc_hint */,
                                    sizeof(Object),
                                    kLeaseTime,
                                    flag,
                                    &ctx);
        auto end = p->patronus_now();
        auto elaps_ns = timer.pin();
        if (time >= 5)
        {
            latency_m.collect(elaps_ns);
        }

        bench_total_nr.fetch_add(1);
        if (unlikely(!lease.success()))
        {
            CHECK_EQ(lease.ec(), AcquireRequestStatus::kMagicMwErr)
                << "Unexpected lease failure: " << lease.ec()
                << ". Lease: " << lease;
            lease_success_m.collect(0);
            bench_failed_nr.fetch_add(1);
            continue;
        }
        auto rdma_buf = p->get_rdma_buffer(sizeof(Object));
        CHECK_GE(rdma_buf.size, sizeof(Object));
        memset(rdma_buf.buffer, 0, sizeof(Object));

        CHECK_LT(sizeof(Object), rdma_buf.size);
        // auto r_flag = (flag_t) RWFlag::kNoLocalExpireCheck;
        auto r_flag = 0;
        auto ec = p->read(lease,
                          rdma_buf.buffer,
                          sizeof(Object),
                          0 /* offset */,
                          r_flag /* flag */,
                          &ctx);
        auto lease_ddl = lease.ddl_term();
        CHECK_EQ(ec, RC::kOk)
            << "[bench] client coro " << ctx
            << " read FAILED. This should not happen, because we "
               "filter out the invalid mws. failed at "
            << time << " -th test. Before: " << before << ", end: " << end
            << ", elaps: " << (end - before)
            << ", (ddl - before): " << (lease_ddl - before)
            << " (ddl - end): " << (lease_ddl - end)
            << ". Acquire takes: " << elaps_ns << ", history: " << latency_m;
        auto rel_flag = (flag_t) LeaseModifyFlag::kWaitUntilSuccess;
        p->relinquish(lease, 0, rel_flag, &ctx);
        p->put_rdma_buffer(rdma_buf);
    }
    auto ns = timer.pin();
    ex.worker_finished(coro_id);

    LOG(INFO) << "[bench] coro: " << ctx
              << ", lease_success_m: " << lease_success_m
              << ", lease_success_nr: " << lease_success_nr << ". Take " << ns
              << " ns. History Latency: " << latency_m;

    ctx.yield_to_master();
}
void client_master(Patronus::pointer p,
                   CoroYield &yield,
                   CoroExecutionContext<kCoroCnt> &ex)
{
    auto tid = p->get_thread_id();

    CoroContext mctx(tid, &yield, client_coro.workers);
    CHECK(mctx.is_master());

    for (size_t i = 0; i < kCoroCnt; ++i)
    {
        mctx.yield_to_worker(i);
    }
    LOG(INFO) << "Return back to master. start to recv messages";
    coro_t coro_buf[2 * kCoroCnt];
    while (!ex.is_finished_all())
    {
        auto nr = p->try_get_client_continue_coros(coro_buf, 2 * kCoroCnt);
        for (size_t i = 0; i < nr; ++i)
        {
            auto coro_id = coro_buf[i];
            DVLOG(1) << "[bench] yielding due to CQE";
            mctx.yield_to_worker(coro_id);
        }
    }

    LOG(WARNING) << "[bench] all worker finish their work. at tid " << tid
                 << " thread exiting...";
}

void client(Patronus::pointer p)
{
    auto tid = p->get_thread_id();
    LOG(INFO) << "I am client. tid " << tid;

    CoroExecutionContext<kCoroCnt> ex;
    for (size_t i = 0; i < kCoroCnt; ++i)
    {
        client_coro.workers[i] = CoroCall(
            [p, i, &ex](CoroYield &yield) { client_worker(p, i, yield, ex); });
    }
    client_coro.master =
        CoroCall([p, &ex](CoroYield &yield) { client_master(p, yield, ex); });
    client_coro.master();
}

void server(Patronus::pointer p)
{
    auto tid = p->get_thread_id();

    LOG(INFO) << "I am server. tid " << tid;
    p->server_serve(kWaitKey);
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    rdmaQueryDevice();

    PatronusConfig config;
    config.machine_nr = ::config::kMachineNr;

    auto patronus = Patronus::ins(config);

    sleep(1);

    std::vector<std::thread> threads;
    // let client spining
    auto nid = patronus->get_node_id();

    boost::barrier client_bar(kClientThreadNr);
    if (::config::is_client(nid))
    {
        for (size_t i = 0; i < kClientThreadNr - 1; ++i)
        {
            threads.emplace_back([patronus, &bar = client_bar]() {
                patronus->registerClientThread();
                auto tid = patronus->get_thread_id();
                client(patronus);
                LOG(INFO) << "[bench] thread " << tid << " finish it work";
                bar.wait();
            });
        }
        patronus->registerClientThread();
        auto tid = patronus->get_thread_id();
        client(patronus);
        client_bar.wait();
        LOG(INFO) << "[bench] joined. thread " << tid << " call p->finished()";
        patronus->finished(kWaitKey);
    }
    else
    {
        for (size_t i = 0; i < kServerThreadNr - 1; ++i)
        {
            threads.emplace_back([patronus]() {
                patronus->registerServerThread();
                server(patronus);
                patronus->thread_explain();
            });
        }
        patronus->registerServerThread();
        patronus->finished(kWaitKey);
        server(patronus);
    }

    for (auto &t : threads)
    {
        t.join();
    }

    LOG(INFO) << "finished. ctrl+C to quit.";
}