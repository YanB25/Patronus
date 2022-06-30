#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdlib>
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
#include "thirdparty/serverless/serverless.h"
#include "util/DataFrameF.h"
#include "util/PerformanceReporter.h"
#include "util/Rand.h"
#include "util/TimeConv.h"

using namespace util::literals;
using namespace patronus;
using namespace std::chrono_literals;

constexpr static size_t kClientThreadNr = kMaxAppThread;
constexpr static size_t kServerThreadNr = NR_DIRECTORY;

constexpr static size_t kTestTimePerThread = 1_K;
// constexpr static size_t kTestTimePerThread = 100;

std::vector<std::string> col_idx;
std::vector<size_t> col_x_alloc_size;
std::vector<size_t> col_x_thread_nr;
std::vector<size_t> col_x_coro_nr;
std::vector<size_t> col_x_lease_time_ns;
std::vector<size_t> col_x_with_reuse_mw_opt;
std::vector<size_t> col_alloc_nr;
std::vector<size_t> col_alloc_ns;

std::vector<std::string> col_lat_idx;
std::vector<uint64_t> col_lat_patronus;
std::vector<uint64_t> col_lat_MR;
std::vector<uint64_t> col_lat_unprotected;

using namespace hmdf;

DEFINE_string(exec_meta, "", "The meta data of this execution");

constexpr static size_t kCoroCnt = 32;  // max
struct CoroCommunication
{
    CoroCall workers[kCoroCnt];
    CoroCall master;
    ssize_t thread_remain_task;
    std::vector<bool> finish_all;
};

inline size_t gen_coro_key(size_t node_id, size_t thread_id, size_t coro_id)
{
    return node_id * kMaxAppThread * kCoroCnt + thread_id * kCoroCnt + coro_id;
}
uint64_t bench_locator(uint64_t key)
{
    // the 4 is not needed
    return key * 4 * sizeof(uint64_t);
}

struct BenchConfig
{
    size_t thread_nr;
    size_t coro_nr;
    size_t block_size;
    size_t task_nr;
    flag_t acquire_flag;
    flag_t relinquish_flag;
    bool do_not_call_relinquish{false};
    std::chrono::nanoseconds acquire_ns;
    std::string name;
    bool report;
    bool reuse_mw_opt{true};

    static BenchConfig get_empty_conf(const std::string &name,
                                      size_t thread_nr,
                                      size_t coro_nr,
                                      size_t block_size,
                                      size_t task_nr)
    {
        BenchConfig ret;
        ret.name = name;
        ret.thread_nr = thread_nr;
        ret.coro_nr = coro_nr;
        ret.block_size = block_size;
        ret.task_nr = task_nr;
        return ret;
    }
};

class BenchConfigFactory
{
public:
    static std::vector<BenchConfig> get_basic(const std::string &name,
                                              size_t thread_nr,
                                              size_t coro_nr,
                                              size_t block_size,
                                              size_t task_nr)
    {
        flag_t acquire_flag = (flag_t) AcquireRequestFlag::kNoGc |
                              (flag_t) AcquireRequestFlag::kNoBindAny;
        flag_t relinquish_flag =
            (flag_t) LeaseModifyFlag::kNoRelinquishUnbindAny;
        auto acquire_ns = 0ns;

        auto conf = BenchConfig::get_empty_conf(
            name, thread_nr, coro_nr, block_size, task_nr);
        conf.acquire_flag = acquire_flag;
        conf.relinquish_flag = relinquish_flag;
        conf.acquire_ns = acquire_ns;
        return pipeline({conf});
    }

private:
    static std::vector<BenchConfig> pipeline(std::vector<BenchConfig> &&confs)
    {
        for (auto &conf : confs)
        {
            conf.report = false;
        }
        confs.back().report = true;
        return confs;
    }
};

void reg_result(size_t total_ns, const BenchConfig &conf)
{
    col_idx.push_back(conf.name);
    col_x_alloc_size.push_back(conf.block_size);
    col_x_thread_nr.push_back(conf.thread_nr);
    col_x_coro_nr.push_back(conf.coro_nr);
    auto ns = util::time::to_ns(conf.acquire_ns);
    col_x_lease_time_ns.push_back(ns);
    col_x_with_reuse_mw_opt.push_back(conf.reuse_mw_opt);
    col_alloc_nr.push_back(conf.task_nr);
    col_alloc_ns.push_back(total_ns);
}

std::unordered_map<std::string, std::vector<uint64_t>> lat_data_;
void reg_latency(const std::string &name, OnePassBucketMonitor<uint64_t> &m)
{
    if (unlikely(col_lat_idx.empty()))
    {
        col_lat_idx.push_back("lat_min");
        col_lat_idx.push_back("lat_p5");
        col_lat_idx.push_back("lat_p9");
        col_lat_idx.push_back("lat_p99");
    }

    lat_data_[name].push_back(m.min());
    lat_data_[name].push_back(m.percentile(0.5));
    lat_data_[name].push_back(m.percentile(0.9));
    lat_data_[name].push_back(m.percentile(0.99));
}

void bench_alloc_thread_coro_master(Patronus::pointer patronus,
                                    CoroYield &yield,
                                    size_t test_times,
                                    std::atomic<ssize_t> &work_nr,
                                    CoroCommunication &coro_comm,
                                    size_t coro_nr)
{
    auto tid = patronus->get_thread_id();

    CoroContext mctx(tid, &yield, coro_comm.workers);
    CHECK(mctx.is_master());

    LOG_IF(WARNING, test_times % 1000 != 0)
        << "test_times % 1000 != 0. Will introduce 1/1000 performance error "
           "per thread";

    ssize_t task_per_sync = test_times / 1000;
    LOG_IF(WARNING, task_per_sync <= (ssize_t) coro_nr)
        << "test_times < coro_nr.";

    task_per_sync =
        std::max(task_per_sync, ssize_t(coro_nr));  // at least coro_nr

    coro_comm.thread_remain_task = task_per_sync;
    ssize_t remain =
        work_nr.fetch_sub(task_per_sync, std::memory_order_relaxed) -
        task_per_sync;

    VLOG(1) << "[bench] thread_remain_task init to " << task_per_sync
            << ", task_per_sync: " << task_per_sync
            << ", remain (after fetched): " << remain;

    for (size_t i = 0; i < coro_nr; ++i)
    {
        mctx.yield_to_worker(i);
    }

    static thread_local coro_t coro_buf[2 * kCoroCnt];
    while (true)
    {
        if (coro_comm.thread_remain_task <= 2 * ssize_t(coro_nr))
        {
            auto cur_task_nr = std::min(remain, task_per_sync);
            // VLOG(1) << "[coro] before cur_task_nr: " << cur_task_nr
            //         << ", remain: " << remain
            //         << ", thread_remain_task:" <<
            //         coro_comm.thread_remain_task
            //         << ", task_per_sync: " << task_per_sync;
            if (cur_task_nr >= 0)
            {
                remain =
                    work_nr.fetch_sub(cur_task_nr, std::memory_order_relaxed) -
                    cur_task_nr;
                if (remain >= 0)
                {
                    coro_comm.thread_remain_task += cur_task_nr;
                }
                // NOTE:
                // The worker may do slightly more task, because here, the
                // remain may be below zeros
            }
            // VLOG(1) << "[coro] after remain: " << remain
            //         << ", remain: " << remain
            //         << ", thread_remain_task: " <<
            //         coro_comm.thread_remain_task
            //         << ", task_per_sync: " << task_per_sync;
        }

        auto nr =
            patronus->try_get_client_continue_coros(coro_buf, 2 * kCoroCnt);

        for (size_t i = 0; i < nr; ++i)
        {
            auto coro_id = coro_buf[i];
            DVLOG(1) << "[coro] yielding due to CQE";
            mctx.yield_to_worker(coro_id);
        }

        if (remain <= 0)
        {
            bool finish_all = std::all_of(std::begin(coro_comm.finish_all),
                                          std::end(coro_comm.finish_all),
                                          [](bool i) { return i; });
            if (finish_all)
            {
                CHECK_LE(remain, 0);
                break;
            }
        }
    }

    // LOG(WARNING) << "[coro] all worker finish their work. exiting...";
}

void bench_alloc_thread_coro_worker(Patronus::pointer patronus,
                                    size_t coro_id,
                                    CoroYield &yield,
                                    CoroCommunication &coro_comm,
                                    OnePassBucketMonitor<uint64_t> &lat_m,
                                    bool is_master,
                                    const BenchConfig &conf)
{
    auto alloc_size = conf.block_size;
    auto acquire_ns = conf.acquire_ns;
    auto acquire_flag = conf.acquire_flag;
    auto relinquish_flag = conf.relinquish_flag;
    auto do_not_call_relinquish = conf.do_not_call_relinquish;

    auto tid = patronus->get_thread_id();
    auto dir_id = tid % kServerThreadNr;
    auto server_nid = ::config::get_server_nids().front();

    CoroContext ctx(tid, &yield, &coro_comm.master, coro_id);

    size_t fail_nr = 0;
    size_t succ_nr = 0;

    ChronoTimer timer;
    ChronoTimer op_timer;
    while (coro_comm.thread_remain_task > 0)
    {
        bool succ = true;
        // VLOG(4) << "[coro] tid " << tid << " get_rlease. coro: " << ctx;
        if (is_master && coro_id == 0)
        {
            op_timer.pin();
        }
        auto lease = patronus->get_rlease(server_nid,
                                          dir_id,
                                          nullgaddr,
                                          0 /* alloc_hint */,
                                          alloc_size,
                                          acquire_ns,
                                          acquire_flag,
                                          &ctx);
        succ = lease.success();

        if (succ)
        {
            succ_nr++;
            if (!do_not_call_relinquish)
            {
                patronus->relinquish(
                    lease, 0 /* hint */, relinquish_flag, &ctx);
            }
            coro_comm.thread_remain_task--;
            // LOG_IF(INFO, succ_nr % 10_K == 0)
            //     << "[detail] finished 10K succeess. total: " << succ_nr
            //     << ". coro: " << ctx;
        }
        else
        {
            fail_nr++;
            // LOG_IF(INFO, fail_nr % 1_K == 0)
            //     << "[detail] finished 1K failed. total: " << fail_nr
            //     << ". coro: " << ctx;
            CHECK_EQ(lease.ec(), AcquireRequestStatus::kMagicMwErr)
                << "** only this kind of error is possible.";
        }
        if (is_master && coro_id == 0)
        {
            auto ns = op_timer.pin();
            lat_m.collect(ns);
        }
    }
    auto total_ns = timer.pin();

    coro_comm.finish_all[coro_id] = true;

    VLOG(1) << "[bench] tid " << tid << " got " << fail_nr
            << " failed lease. succ lease: " << succ_nr << " within "
            << total_ns << " ns. coro: " << ctx;
    ctx.yield_to_master();
    CHECK(false) << "yield back to me.";
}

struct BenchResult
{
    double lat_min;
    double lat_p5;
    double lat_p9;
    double lat_p99;
};

using Parameters = serverless::CoroLauncher::Parameters;
using lambda_t = serverless::CoroLauncher::lambda_t;
struct Comm
{
    uint64_t magic;
};

size_t worker_do_nr_{0};
RetCode worker_do(Patronus::pointer patronus,
                  const Parameters &input,
                  Parameters &output,
                  bool root,
                  bool tail,
                  CoroContext *ctx)
{
    worker_do_nr_++;
    auto tid = patronus->get_thread_id();
    auto dir_id = tid % kServerThreadNr;
    auto server_nid = ::config::get_server_nids().front();

    if (input.find("addr") == input.end())
    {
        LOG(FATAL) << "** Failed to locate input `addr`. is_root: " << root
                   << ", is_tail: " << tail << ". ctx: " << pre_coro_ctx(ctx);
    }

    const auto &r = input.find("addr")->second;
    Comm *comm;
    if (root)
    {
        comm = new Comm;
    }
    else
    {
        comm = (Comm *) r.prv;
    }

    if (root)
    {
        auto ac_flag = (flag_t) AcquireRequestFlag::kNoGc;
        auto wlease = patronus->get_wlease(server_nid,
                                           dir_id,
                                           r.gaddr,
                                           0 /* alloc hint */,
                                           r.size,
                                           0ns,
                                           ac_flag,
                                           ctx);
        CHECK(wlease.success());
        auto rdma_buf = patronus->get_rdma_buffer(sizeof(uint64_t));
        auto magic = fast_pseudo_rand_int();
        // LOG(INFO) << "[debug] !!! init magic to " << (void *) magic << " at "
        //           << r.gaddr;
        *(uint64_t *) rdma_buf.buffer = magic;
        patronus
            ->write(wlease,
                    rdma_buf.buffer,
                    sizeof(uint64_t),
                    0 /* offset */,
                    0 /* flag */,
                    ctx)
            .expect(RC::kOk);
        comm->magic = magic;

        patronus->put_rdma_buffer(rdma_buf);
        patronus->relinquish(wlease, 0 /* alloc hint */, 0 /* flag */, ctx);
    }

    // read, validate same
    {
        auto ac_flag = (flag_t) AcquireRequestFlag::kNoGc;
        auto rlease = patronus->get_rlease(server_nid,
                                           dir_id,
                                           r.gaddr,
                                           0 /* alloc hint */,
                                           r.size,
                                           0ns,
                                           ac_flag,
                                           ctx);
        CHECK(rlease.success());
        auto rdma_buf = patronus->get_rdma_buffer(sizeof(uint64_t));
        memset(rdma_buf.buffer, 0, sizeof(uint64_t));
        patronus
            ->read(rlease,
                   rdma_buf.buffer,
                   sizeof(uint64_t),
                   0 /* offset */,
                   0 /* flag */,
                   ctx)
            .expect(RC::kOk);
        std::atomic<uint64_t> *atomic_got =
            (std::atomic<uint64_t> *) rdma_buf.buffer;
        uint64_t got = atomic_got->load();
        CHECK_EQ(got, comm->magic);
        patronus->put_rdma_buffer(rdma_buf);
        patronus->relinquish(rlease, 0 /* hint */, 0 /* flag */, ctx);
    }

    // write to magic + 1
    {
        auto ac_flag = (flag_t) AcquireRequestFlag::kNoGc;
        auto wlease = patronus->get_wlease(server_nid,
                                           dir_id,
                                           r.gaddr,
                                           0 /* hint */,
                                           r.size,
                                           0ns,
                                           ac_flag,
                                           ctx);
        CHECK(wlease.success());
        auto rdma_buf = patronus->get_rdma_buffer(sizeof(uint64_t));
        comm->magic = comm->magic + 1;
        *(uint64_t *) rdma_buf.buffer = comm->magic;
        patronus
            ->write(wlease,
                    rdma_buf.buffer,
                    sizeof(uint64_t),
                    0 /* offset */,
                    0 /* flag */,
                    ctx)
            .expect(RC::kOk);
        patronus->put_rdma_buffer(rdma_buf);
        patronus->relinquish(wlease, 0 /* hint */, 0 /* flag */, ctx);
        // LOG(INFO) << "[debug] !!! updating magic to " << (void *)
        // (comm->magic)
        //           << " at " << r.gaddr;
    }

    // debug: reread, make sure we really write it successfully
    {
        auto ac_flag = (flag_t) AcquireRequestFlag::kNoGc;
        auto rlease = patronus->get_rlease(server_nid,
                                           dir_id,
                                           r.gaddr,
                                           0 /* alloc hint */,
                                           r.size,
                                           0ns,
                                           ac_flag,
                                           ctx);
        CHECK(rlease.success());
        auto rdma_buf = patronus->get_rdma_buffer(sizeof(uint64_t));
        memset(rdma_buf.buffer, 0, sizeof(uint64_t));
        patronus
            ->read(rlease,
                   rdma_buf.buffer,
                   sizeof(uint64_t),
                   0 /* offset */,
                   0 /* flag */,
                   ctx)
            .expect(RC::kOk);
        std::atomic<uint64_t> *atomic_got =
            (std::atomic<uint64_t> *) rdma_buf.buffer;
        uint64_t got = atomic_got->load();
        CHECK_EQ(got, comm->magic);
        patronus->put_rdma_buffer(rdma_buf);
        patronus->relinquish(rlease, 0 /* hint */, 0 /* flag */, ctx);
    }

    // tell the next lambda the magic
    {
        output["addr"] = input.find("addr")->second;
        output["addr"].prv = comm;
    }

    if (tail)
    {
        delete (Comm *) r.prv;
    }
    return RC::kOk;
}

void bench_alloc_thread_coro(
    Patronus::pointer patronus,
    [[maybe_unused]] OnePassBucketMonitor<uint64_t> &lat_m,
    [[maybe_unused]] bool is_master,
    std::atomic<ssize_t> &work_nr,
    const BenchConfig &conf)
{
    auto test_times = conf.task_nr;

    auto tid = patronus->get_thread_id();
    auto nid = patronus->get_node_id();

    serverless::CoroLauncher launcher(patronus, test_times, work_nr);

    auto root = [patronus](const Parameters &input,
                           Parameters &output,
                           CoroContext *ctx) -> RetCode {
        return worker_do(patronus,
                         input,
                         output,
                         true /* is root */,
                         false /* is tail */,
                         ctx);
    };
    auto tail = [patronus](const Parameters &input,
                           Parameters &output,
                           CoroContext *ctx) -> RetCode {
        return worker_do(patronus,
                         input,
                         output,
                         false /* is root */,
                         true /* is tail */,
                         ctx);
    };
    auto middle = [patronus](const Parameters &input,
                             Parameters &output,
                             CoroContext *ctx) -> RetCode {
        return worker_do(patronus,
                         input,
                         output,
                         false /* is root */,
                         false /* is tail */,
                         ctx);
    };

    {
        // one chain
        Parameters init_para;
        auto &addr = init_para["addr"];
        auto key = gen_coro_key(nid, tid, 0);
        auto offset = bench_locator(key);
        addr.gaddr = GlobalAddress(0, offset);
        addr.size = sizeof(uint64_t);

        LOG(INFO) << "[debug] !! init_para: " << init_para;

        lambda_t last_id{};
        lambda_t root_id{};
        size_t expect_lambda_nr = kCoroCnt / 2;
        for (size_t i = 0; i < expect_lambda_nr; ++i)
        {
            if (i == 0)
            {
                last_id = launcher.add_lambda(root, init_para, {}, {});
                root_id = last_id;
            }
            else
            {
                bool is_last = i + 1 == expect_lambda_nr;
                if (is_last)
                {
                    auto id = launcher.add_lambda(tail, {}, last_id, root_id);
                    last_id = id;
                }
                else
                {
                    auto id = launcher.add_lambda(middle, {}, last_id, {});
                    last_id = id;
                }
            }
        }
    }

    {
        // another chain

        Parameters init_para;
        auto &addr = init_para["addr"];
        auto key = gen_coro_key(nid, tid, 1);
        auto offset = bench_locator(key);
        addr.gaddr = GlobalAddress(0, offset);
        addr.size = sizeof(uint64_t);

        lambda_t last_id{};
        lambda_t root_id{};
        size_t expect_lambda_nr = kCoroCnt / 2;
        for (size_t i = 0; i < expect_lambda_nr; ++i)
        {
            if (i == 0)
            {
                last_id = launcher.add_lambda(root, init_para, {}, {});
                root_id = last_id;
            }
            else
            {
                bool is_last = i + 1 == expect_lambda_nr;
                if (is_last)
                {
                    auto id = launcher.add_lambda(tail, {}, last_id, root_id);
                    last_id = id;
                }
                else
                {
                    auto id = launcher.add_lambda(middle, {}, last_id, {});
                    last_id = id;
                }
            }
        }
    }

    launcher.launch();

    LOG(INFO) << "[debug] !! worker_do_nr: " << worker_do_nr_ << " from tid "
              << tid;
    return;
}

void bench_template(const std::string &name,
                    Patronus::pointer patronus,
                    boost::barrier &bar,
                    std::atomic<ssize_t> &work_nr,
                    bool is_master,
                    const BenchConfig &conf)

{
    auto test_times = conf.task_nr;
    auto thread_nr = conf.thread_nr;
    auto coro_nr = conf.coro_nr;
    auto report = conf.report;
    auto alloc_size = conf.block_size;

    if (is_master)
    {
        work_nr = test_times;
        LOG(INFO) << "[bench] BENCH: " << name << ", thread_nr: " << thread_nr
                  << ", alloc_size: " << alloc_size << ", coro_nr: " << coro_nr
                  << ", report: " << report;
    }

    bar.wait();
    ChronoTimer timer;

    auto tid = patronus->get_thread_id();

    auto min = util::time::to_ns(0ns);
    auto max = util::time::to_ns(10ms);
    auto range = util::time::to_ns(1us);
    OnePassBucketMonitor lat_m(min, max, range);

    if (tid < thread_nr)
    {
        bench_alloc_thread_coro(patronus, lat_m, is_master, work_nr, conf);
    }

    bar.wait();
    auto total_ns = timer.pin();
    if (is_master && report)
    {
        reg_result(total_ns, conf);
        reg_latency(name, lat_m);
    }
    bar.wait();
}

void run_benchmark_server(Patronus::pointer patronus,
                          const BenchConfig &conf,
                          bool is_master,
                          uint64_t key)
{
    std::ignore = conf;
    if (is_master)
    {
        patronus->finished(key);
    }

    auto tid = patronus->get_thread_id();
    LOG(INFO) << "[coro] server thread tid " << tid;

    LOG(INFO) << "[bench] BENCH: " << conf.name
              << ", thread_nr: " << conf.thread_nr
              << ", alloc_size: " << conf.block_size
              << ", coro_nr: " << conf.coro_nr << ", report: " << conf.report;

    patronus->server_serve(key);
}
std::atomic<ssize_t> shared_task_nr;

void run_benchmark_client(Patronus::pointer patronus,
                          const BenchConfig &conf,
                          boost::barrier &bar,
                          bool is_master,
                          uint64_t &key)
{
    if (is_master)
    {
        shared_task_nr = conf.task_nr;
    }
    bar.wait();

    bench_template(conf.name, patronus, bar, shared_task_nr, is_master, conf);
    bar.wait();
    if (is_master)
    {
        patronus->finished(key);
    }
}

void run_benchmark(Patronus::pointer patronus,
                   const std::vector<BenchConfig> &configs,
                   boost::barrier &bar,
                   bool is_client,
                   bool is_master,
                   uint64_t &key)
{
    for (const auto &conf : configs)
    {
        key++;

        LOG_IF(INFO, is_master)
            << "[bench] setting reuse_mw_opt to " << conf.reuse_mw_opt;
        patronus->set_configure_reuse_mw_opt(conf.reuse_mw_opt);

        if (is_client)
        {
            run_benchmark_client(patronus, conf, bar, is_master, key);
        }
        else
        {
            run_benchmark_server(patronus, conf, is_master, key);
        }
        if (is_master)
        {
            patronus->keeper_barrier(std::to_string(key), 100ms);
        }
    }
}

void benchmark(Patronus::pointer patronus,
               boost::barrier &bar,
               bool is_client,
               bool is_master)
{
    bar.wait();

    size_t key = 0;

    // for (size_t thread_nr : {32})
    for (size_t thread_nr : {1})
    {
        CHECK_LE(thread_nr, kMaxAppThread);
        for (size_t block_size : {64ul})
        {
            // for (size_t coro_nr : {2, 4, 8, 16, 32})
            // for (size_t coro_nr : {1, 32})
            for (size_t coro_nr : {32})
            {
                auto total_test_times = kTestTimePerThread * 4;
                {
                    auto configs =
                        BenchConfigFactory::get_basic("two-chain",
                                                      thread_nr,
                                                      coro_nr,
                                                      block_size,
                                                      total_test_times);
                    run_benchmark(
                        patronus, configs, bar, is_client, is_master, key);
                }
            }
        }
    }

    bar.wait();
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

    auto nid = patronus->get_node_id();
    if (::config::is_client(nid))
    {
        std::vector<std::thread> threads;
        boost::barrier bar(kClientThreadNr);
        for (size_t i = 0; i < kClientThreadNr - 1; ++i)
        {
            // used by all the threads to synchronize their works
            threads.emplace_back([patronus, &bar]() {
                patronus->registerClientThread();
                bar.wait();
                benchmark(
                    patronus, bar, true /* is_client */, false /* is_master */);
            });
        }
        patronus->registerClientThread();
        bar.wait();
        patronus->keeper_barrier("begin", 100ms);
        benchmark(patronus, bar, true /* is_client */, true /* is_master */);

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
            threads.emplace_back([patronus, &bar]() {
                patronus->registerServerThread();
                bar.wait();
                benchmark(patronus,
                          bar,
                          false /* is_client */,
                          false /* is_master */);
            });
        }
        patronus->registerServerThread();
        bar.wait();
        patronus->keeper_barrier("begin", 100ms);
        benchmark(patronus, bar, false /* is_client */, true /* is_master */);

        for (auto &t : threads)
        {
            t.join();
        }
    }

    patronus->keeper_barrier("finished", 100ms);
    LOG(INFO) << "Finished.";
}
