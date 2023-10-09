#include <algorithm>
#include <chrono>
#include <iostream>
#include <queue>
#include <set>

#include "Common.h"
#include "PerThread.h"
#include "Rdma.h"
#include "boost/thread/barrier.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "patronus/All.h"
#include "umsg/Config.h"
#include "util/DataFrameF.h"
#include "util/PerformanceReporter.h"
#include "util/Rand.h"
#include "util/TimeConv.h"

using namespace util::literals;
using namespace patronus;
using namespace std::chrono_literals;

constexpr static size_t kClientThreadNr = kMaxAppThread - 1;
constexpr static size_t kServerThreadNr = NR_DIRECTORY;
// static_assert(kServerThreadNr <= NR_DIRECTORY);
static_assert(kClientThreadNr <= kMaxAppThread);

DEFINE_string(exec_meta, "", "The meta data of this execution");

constexpr static size_t kCoroCnt = 32;  // max
struct CoroCommunication
{
    CoroCall workers[kCoroCnt];
    CoroCall master;
    ssize_t thread_remain_task;
    std::vector<bool> finish_all;
};

struct BenchConfig
{
    size_t thread_nr;
    size_t coro_nr;
    size_t task_nr;
    std::string name;

    static BenchConfig get_empty_conf(const std::string &name,
                                      size_t thread_nr,
                                      size_t coro_nr,
                                      size_t task_nr)
    {
        BenchConfig ret;
        ret.name = name;
        ret.thread_nr = thread_nr;
        ret.coro_nr = coro_nr;
        ret.task_nr = task_nr;
        return ret;
    }
};

class BenchConfigFactory
{
public:
    static std::vector<BenchConfig> get_default(const std::string &name,
                                                size_t thread_nr,
                                                size_t coro_nr,
                                                size_t task_nr)
    {
        auto conf =
            BenchConfig::get_empty_conf(name, thread_nr, coro_nr, task_nr);
        return {conf};
    }

private:
};

struct ReqMessage
{
    size_t from_tid;
    size_t from_nid;
    size_t from_coro_id;
    char padding[sizeof(patronus::AcquireRequest) - sizeof(size_t) * 3];
};

struct RespMessage
{
    size_t from_tid;
    size_t from_nid;
    size_t from_coro_id;
    char padding[sizeof(patronus::AcquireResponse) - sizeof(size_t) * 3];
};

void bench_alloc_thread_coro_master(Patronus::pointer patronus,
                                    CoroYield &yield,
                                    CoroCommunication &coro_comm,
                                    size_t coro_nr)
{
    auto tid = patronus->get_thread_id();
    auto nid = patronus->get_node_id();

    CoroContext mctx(tid, &yield, coro_comm.workers);
    CHECK(mctx.is_master());

    for (size_t i = 0; i < coro_nr; ++i)
    {
        mctx.yield_to_worker(i);
    }

    auto dsm = patronus->get_dsm();
    std::vector<char> buffer;
    buffer.resize(config::umsg::kMaxRecvBuffer);
    char *recv_buffer = buffer.data();
    while (true)
    {
        auto nr =
            dsm->unreliable_try_recv(recv_buffer, config::umsg::kRecvLimit);
        for (size_t i = 0; i < nr; ++i)
        {
            auto *resp_msg =
                (RespMessage *) (recv_buffer +
                                 i * config::umsg::kUserMessageSize);
            auto from_tid = resp_msg->from_tid;
            auto from_nid = resp_msg->from_nid;
            auto from_coro_id = resp_msg->from_coro_id;
            CHECK_EQ(from_nid, nid);
            CHECK_EQ(from_tid, tid);
            CHECK_LT(from_coro_id, kCoroCnt);
            mctx.yield_to_worker(from_coro_id);
        }
    }

    LOG(WARNING) << "[coro] all worker finish their work. exiting...";
}

constexpr static ssize_t kBatch = 100;
void bench_alloc_thread_coro_worker(Patronus::pointer patronus,
                                    size_t coro_id,
                                    CoroYield &yield,
                                    CoroCommunication &coro_comm,
                                    std::atomic<uint64_t> &finished_nr)
{
    auto tid = patronus->get_thread_id();
    auto nid = patronus->get_node_id();

    auto dir_id = tid % kServerThreadNr;
    auto server_nid = ::config::get_server_nids().front();

    CoroContext ctx(tid, &yield, &coro_comm.master, coro_id);

    auto min = util::time::to_ns(0ns);
    auto max = util::time::to_ns(10ms);
    auto range = util::time::to_ns(1us);
    OnePassBucketMonitor lat_m(min, max, range);

    auto dsm = patronus->get_dsm();
    auto rdma_buffer = patronus->get_rdma_buffer(sizeof(ReqMessage));
    auto *req_msg = (ReqMessage *) rdma_buffer.buffer;
    req_msg->from_tid = tid;
    req_msg->from_coro_id = coro_id;
    req_msg->from_nid = nid;
    ssize_t finished_nr_coro{0};
    LOG(INFO) << "[bench] client: tid: " << tid << ", coro_id: " << coro_id
              << ", nid: " << nid;
    while (true)
    {
        dsm->unreliable_send(
            (const char *) req_msg, sizeof(ReqMessage), server_nid, dir_id);
        ctx.yield_to_master();
        finished_nr_coro++;
        if (finished_nr_coro >= kBatch)
        {
            finished_nr_coro -= kBatch;
            finished_nr.fetch_add(kBatch, std::memory_order_relaxed);
        }
    }
    patronus->put_rdma_buffer(std::move(rdma_buffer));

    ctx.yield_to_master();
    CHECK(false) << "yield back to me.";
}

void bench_alloc_thread_coro(Patronus::pointer patronus,
                             std::atomic<uint64_t> &finished_nr,
                             size_t coro_nr)
{
    auto tid = patronus->get_thread_id();
    auto dir_id = tid % kServerThreadNr;

    VLOG(1) << "[coro] client tid " << tid << " bind to core " << tid
            << ", using dir_id " << dir_id;
    CoroCommunication coro_comm;
    coro_comm.finish_all.resize(coro_nr);

    for (size_t i = 0; i < coro_nr; ++i)
    {
        coro_comm.workers[i] =
            CoroCall([patronus, coro_id = i, &coro_comm, &finished_nr](
                         CoroYield &yield) {
                bench_alloc_thread_coro_worker(
                    patronus, coro_id, yield, coro_comm, finished_nr);
            });
    }

    coro_comm.master =
        CoroCall([patronus, &coro_comm, coro_nr](CoroYield &yield) {
            bench_alloc_thread_coro_master(patronus, yield, coro_comm, coro_nr);
        });

    coro_comm.master();
}

void run_benchmark_server(Patronus::pointer patronus)
{
    // auto tid = patronus->get_thread_id();
    auto dsm = patronus->get_dsm();

    std::vector<char> buffer;
    buffer.resize(config::umsg::kMaxRecvBuffer);
    const char *recv_buffer = buffer.data();
    while (true)
    {
        auto nr = dsm->unreliable_try_recv((char *) recv_buffer,
                                           config::umsg::kRecvLimit);
        for (size_t i = 0; i < nr; ++i)
        {
            auto send_buffer =
                patronus->get_rdma_message_buffer(sizeof(RespMessage));
            DCHECK_GE(send_buffer.size, sizeof(RespMessage));
            auto *resp_msg = (RespMessage *) send_buffer.buffer;
            auto *req_msg = (ReqMessage *) (recv_buffer +
                                            i * config::umsg::kUserMessageSize);
            auto from_tid = req_msg->from_tid;
            auto from_nid = req_msg->from_nid;
            auto from_coro_id = req_msg->from_coro_id;
            resp_msg->from_tid = from_tid;
            resp_msg->from_nid = from_nid;
            resp_msg->from_coro_id = from_coro_id;
            dsm->unreliable_send((const char *) resp_msg,
                                 sizeof(RespMessage),
                                 from_nid,
                                 from_tid);
            patronus->put_rdma_message_buffer(std::move(send_buffer));
        }
    }
}

void run_benchmark_client(Patronus::pointer patronus,
                          boost::barrier &bar,
                          std::atomic<uint64_t> &finished_nr,
                          size_t coro_nr)
{
    bar.wait();

    bench_alloc_thread_coro(patronus, finished_nr, coro_nr);

    bar.wait();
}

void benchmark(Patronus::pointer patronus,
               boost::barrier &bar,
               std::atomic<uint64_t> &finished_nr,
               bool is_client)
{
    constexpr static size_t kBenchUseCoroNr = 32;
    if (is_client)
    {
        run_benchmark_client(patronus, bar, finished_nr, kBenchUseCoroNr);
    }
    else
    {
        run_benchmark_server(patronus);
    }
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    PatronusConfig config;
    config.machine_nr = ::config::kMachineNr;
    config.block_class = {2_MB};
    config.block_ratio = {1};

    auto patronus = Patronus::ins(config);
    Perthread<std::atomic<uint64_t>> finished_nrs;

    auto nid = patronus->get_node_id();
    bool is_client = ::config::is_client(nid);

    std::thread monitor_thread([&finished_nrs, is_client]() {
        if (!is_client)
        {
            return;
        }
        ChronoTimer timer;
        double last_sum = 0;
        while (true)
        {
            timer.pin();
            std::this_thread::sleep_for(1s);
            auto ns = timer.pin();
            double sum = 0;
            for (size_t i = 0; i < finished_nrs.size(); ++i)
            {
                sum += finished_nrs[i].load(std::memory_order_relaxed);
            }

            auto op = sum - last_sum;
            double ops = op * 1e9 / ns;
            double cops = ops * ::config::get_client_nids().size();
            LOG(INFO) << "[bench] ops: " << ops << ", cluster ops: " << cops
                      << ", From op " << op << ", ns: " << ns;
            last_sum = sum;
        }
    });

    if (is_client)
    {
        std::vector<std::thread> threads;
        boost::barrier bar(kClientThreadNr);
        for (size_t i = 0; i < kClientThreadNr - 1; ++i)
        {
            // used by all the threads to synchronize their works
            threads.emplace_back(
                [patronus, &bar, &finished_nr = finished_nrs[i + 1]]() {
                    patronus->registerClientThread();
                    bar.wait();
                    benchmark(patronus, bar, finished_nr, true /* is_client */);
                });
        }
        patronus->registerClientThread();
        bar.wait();
        patronus->keeper_barrier("begin", 100ms);
        benchmark(patronus, bar, finished_nrs[0], true);

        for (auto &t : threads)
        {
            t.join();
        }

        auto tid = patronus->get_thread_id();
        LOG(INFO) << "Client calling finished with tid " << tid;
    }
    else
    {
        boost::barrier bar(kServerThreadNr);
        std::vector<std::thread> threads;
        for (size_t i = 0; i < kServerThreadNr - 1; ++i)
        {
            threads.emplace_back(
                [patronus, &bar, &finished_nr = finished_nrs[0]]() {
                    patronus->registerServerThread();
                    bar.wait();
                    benchmark(
                        patronus, bar, finished_nr, false /* is_client */);
                });
        }
        patronus->registerServerThread();
        bar.wait();
        patronus->keeper_barrier("begin", 100ms);
        benchmark(patronus, bar, finished_nrs[0], false /* is_client */);

        for (auto &t : threads)
        {
            t.join();
        }
    }

    monitor_thread.join();

    patronus->keeper_barrier("finished", 100ms);
    LOG(INFO) << "Exiting...";
}
