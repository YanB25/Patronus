#include <algorithm>
#include <random>

#include "Timer.h"
#include "boost/thread/barrier.hpp"
#include "gflags/gflags.h"
#include "patronus/Patronus.h"
#include "util/PerformanceReporter.h"
#include "util/monitor.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

using namespace patronus;
constexpr static size_t kServerThreadNr = NR_DIRECTORY;
constexpr static size_t kClientThreadNr = kServerThreadNr;

constexpr static size_t kCoroCnt = 8;
constexpr static size_t kKeyLimit = 100;

constexpr static size_t kTestTime =
    Patronus::kMwPoolSizePerThread / kCoroCnt / NR_DIRECTORY / 2;

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
struct ClientCommunication
{
    bool still_has_work[kCoroCnt];
    bool finish_cur_task[kCoroCnt];
    bool finish_all_task[kCoroCnt];
};
thread_local ClientCommunication client_comm;

void client_worker(Patronus::pointer p, coro_t coro_id, CoroYield &yield)
{
    auto tid = p->get_thread_id();
    auto server_nid = ::config::get_server_nids().front();

    auto dir_id = tid % NR_DIRECTORY;

    CoroContext ctx(tid, &yield, &client_coro.master, coro_id);

    OnePassMonitor lease_success_m;
    size_t lease_success_nr{0};

    for (size_t time = 0; time < kTestTime; ++time)
    {
        client_comm.still_has_work[coro_id] = true;
        client_comm.finish_cur_task[coro_id] = false;
        client_comm.finish_all_task[coro_id] = false;

        auto key = rand() % kKeyLimit;

        DVLOG(2) << "[bench] client coro " << ctx << " start to got lease ";
        auto flag = (flag_t) AcquireRequestFlag::kNoGc |
                    (flag_t) AcquireRequestFlag::kWithConflictDetect;
        Lease lease = p->get_rlease(server_nid,
                                    dir_id,
                                    GlobalAddress(0, key),
                                    0 /* alloc_hint */,
                                    sizeof(Object),
                                    0ns,
                                    flag,
                                    &ctx);
        if (unlikely(!lease.success()))
        {
            // LOG(WARNING) << "[bench] client coro " << ctx
            //              << " get_rlease failed. retry. ec: " << lease.ec();
            lease_success_m.collect(0);
            continue;
        }
        else
        {
            lease_success_m.collect(1);
            lease_success_nr++;
        }

        DVLOG(2) << "[bench] client coro " << ctx << " got lease " << lease;

        DVLOG(2) << "[bench] client coro " << ctx
                 << " start to relinquish lease ";
        p->relinquish(lease, 0, 0, &ctx);

        DVLOG(2) << "[bench] client coro " << ctx << " finished current task.";
        client_comm.still_has_work[coro_id] = true;
        client_comm.finish_cur_task[coro_id] = true;
        client_comm.finish_all_task[coro_id] = false;
        ctx.yield_to_master();
    }
    client_comm.still_has_work[coro_id] = false;
    client_comm.finish_cur_task[coro_id] = true;
    client_comm.finish_all_task[coro_id] = true;
    LOG(INFO) << "[bench] coro: " << ctx
              << ", lease_success_m: " << lease_success_m
              << ", lease_success_nr: " << lease_success_nr;

    LOG(WARNING) << "worker coro " << (int) coro_id << ", thread " << tid
                 << " finished ALL THE TASK. yield to master.";

    ctx.yield_to_master();
}
void client_master(Patronus::pointer p, CoroYield &yield)
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

    LOG(WARNING) << "[bench] all worker finish their work. at tid " << tid
                 << " thread exiting...";
}

void client(Patronus::pointer p)
{
    auto tid = p->get_thread_id();
    LOG(INFO) << "I am client. tid " << tid;
    for (size_t i = 0; i < kCoroCnt; ++i)
    {
        client_coro.workers[i] =
            CoroCall([p, i](CoroYield &yield) { client_worker(p, i, yield); });
    }
    client_coro.master =
        CoroCall([p](CoroYield &yield) { client_master(p, yield); });
    client_coro.master();
    LOG(INFO) << "[bench] thread " << tid << " going to leave client()";
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
        LOG(INFO) << "[bench] thread " << tid << " finish its work";
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

    LOG(INFO) << "finished. ctrl+C to quit.";
}