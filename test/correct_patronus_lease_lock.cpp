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

constexpr static size_t kClientNodeId = 1;

constexpr static size_t kCoroCnt = 1;
constexpr static size_t kKeyLimit = 1;

constexpr static size_t kTestTime = 500;
constexpr static auto kLeaseTime = 10ms;

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

uint64_t locate_key(size_t nid, size_t tid, size_t coro_id)
{
    std::ignore = nid;
    std::ignore = tid;
    std::ignore = coro_id;
    return 0;
}

std::atomic<size_t> bench_succ_nr{0};
std::atomic<size_t> bench_failed_nr{0};
std::atomic<size_t> bench_total_nr{0};

std::mutex seq_mu;
Sequence<uint64_t, 10> seq;

void client_worker(Patronus::pointer p,
                   coro_t coro_id,
                   CoroYield &yield,
                   CoroExecutionContext<kCoroCnt> &ex)
{
    auto nid = p->get_node_id();
    auto tid = p->get_thread_id();
    auto server_nid = ::config::get_server_nids().front();

    auto dir_id = tid % NR_DIRECTORY;

    CoroContext ctx(tid, &yield, &client_coro.master, coro_id);

    OnePassMonitor lease_success_m;
    size_t lease_success_nr{0};

    ChronoTimer timer;
    for (size_t time = 0; time < kTestTime; ++time)
    {
        auto key = locate_key(nid, tid, coro_id);

        DVLOG(2) << "[bench] client coro " << ctx << " start to got lease ";
        auto flag = (flag_t) AcquireRequestFlag::kWithConflictDetect;
        Lease lease = p->get_rlease(server_nid,
                                    dir_id,
                                    GlobalAddress(0, key),
                                    0 /* alloc_hint */,
                                    sizeof(Object),
                                    kLeaseTime,
                                    flag,
                                    &ctx);
        bench_total_nr.fetch_add(1);
        if (unlikely(!lease.success()))
        {
            CHECK(lease.ec() == AcquireRequestStatus::kMagicMwErr ||
                  lease.ec() == AcquireRequestStatus::kLockedErr)
                << "Unexpected lease failure: " << lease.ec()
                << ". Lease: " << lease;
            lease_success_m.collect(0);
            std::this_thread::sleep_for(kLeaseTime / 10);
            bench_failed_nr.fetch_add(1);
            continue;
        }
        else
        {
            bench_succ_nr.fetch_add(1);
            lease_success_m.collect(1);
            lease_success_nr++;
            // success
            std::lock_guard<std::mutex> lk(seq_mu);
            auto now = std::chrono::system_clock::now();
            auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                          now.time_since_epoch())
                          .count();
            seq.push_back(ns);
        }

        std::this_thread::sleep_for(kLeaseTime / 10);
    }
    auto ns = timer.pin();
    ex.worker_finished(coro_id);

    LOG(INFO) << "[bench] coro: " << ctx
              << ", lease_success_m: " << lease_success_m
              << ", lease_success_nr: " << lease_success_nr << ". Take " << ns
              << " ns";

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
        // try to see if messages arrived

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
    auto nid = p->get_node_id();
    auto tid = p->get_thread_id();
    LOG(INFO) << "I am client. tid " << tid;

    if (likely(nid == kClientNodeId))
    {
        CoroExecutionContext<kCoroCnt> ex;
        for (size_t i = 0; i < kCoroCnt; ++i)
        {
            client_coro.workers[i] = CoroCall([p, i, &ex](CoroYield &yield) {
                client_worker(p, i, yield, ex);
            });
        }
        client_coro.master = CoroCall(
            [p, &ex](CoroYield &yield) { client_master(p, yield, ex); });
        client_coro.master();
    }
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

    double concurrency =
        ::config::get_client_nids().size() * kClientThreadNr * kCoroCnt;
    double key_nr = kKeyLimit;
    LOG(INFO) << "[bench] reference: concurrency: " << concurrency
              << ", key_range: " << key_nr
              << ", concurrency/key_range = " << 1.0 * concurrency / key_nr;
    LOG(INFO) << "[bench] total acquire nr: " << bench_total_nr
              << ", succeess nr: " << bench_succ_nr
              << ", failed nr: " << bench_failed_nr;

    uint64_t limit_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(kLeaseTime)
            .count();
    auto s = seq.to_vector();

    LOG(INFO) << "[bench] acquire_time: " << util::pre_vec(s);

    for (size_t i = 0; i < s.size(); ++i)
    {
        auto ns = s[i];
        if (i > 0)
        {
            auto last_ns = s[i - 1];
            auto diff = ns - last_ns;
            CHECK_GT(diff, limit_ns)
                << "The " << i - 1 << "-th and " << i << "-th acquire has "
                << diff << " ns as internal, less than expected " << limit_ns;
            CHECK_LE(diff, 1.5 * limit_ns)
                << "The " << i - 1 << "-th and " << i << "-th acquire has "
                << diff << " ns as internal, >= 1.5 * limit ns.";
        }
    }

    patronus->keeper_barrier("finished", 100ms);

    LOG(INFO) << "finished. ctrl+C to quit.";
}