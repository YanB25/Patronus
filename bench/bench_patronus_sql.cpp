#include <algorithm>
#include <random>
#include <set>

#include "Common.h"
#include "boost/thread/barrier.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "patronus/Patronus.h"
#include "patronus/RdmaAdaptor.h"
#include "patronus/memory/direct_allocator.h"
#include "patronus/memory/mr_allocator.h"
#include "thirdparty/racehashing/hashtable.h"
#include "thirdparty/racehashing/hashtable_handle.h"
#include "thirdparty/racehashing/utils.h"
#include "util/BenchRand.h"
#include "util/DataFrameF.h"
#include "util/Rand.h"

using namespace patronus::hash;
using namespace util::literals;
using namespace patronus;
using namespace hmdf;

constexpr static size_t kServerThreadNr = NR_DIRECTORY;
constexpr static size_t kClientThreadNr = kMaxAppThread;
constexpr static size_t kMaxCoroNr = 16;

DEFINE_string(exec_meta, "", "The meta data of this execution");

std::vector<std::string> col_idx;
std::vector<size_t> col_x_thread_nr;
std::vector<size_t> col_x_coro_nr;
std::vector<size_t> col_x_record_size;
std::vector<size_t> col_x_record_nr;
std::vector<size_t> col_x_record_buffer_size;
std::vector<size_t> col_x_io_times;
std::vector<size_t> col_x_io_record_nr;
std::vector<size_t> col_test_op_nr;
std::vector<size_t> col_ns;

std::vector<size_t> col_rdma_protection_err;

std::vector<std::string> col_lat_idx;
std::vector<uint64_t> col_lat_min;
std::vector<uint64_t> col_lat_p5;
std::vector<uint64_t> col_lat_p9;
std::vector<uint64_t> col_lat_p99;
std::unordered_map<std::string, std::vector<uint64_t>> lat_data;

struct Record
{
    uint64_t id;
    uint32_t age;
    uint8_t sex;
};

struct BenchConfig;

struct BenchConfig
{
    using InitF = std::function<void(char *buffer, const BenchConfig &)>;
    using SqlF =
        std::function<void(char *, size_t record_size, size_t record_nr)>;
    size_t thread_nr{1};
    size_t coro_nr{1};
    size_t test_nr{0};
    bool first_enter{true};
    bool server_should_leave{true};
    bool should_report{false};
    std::string name;

    size_t record_nr{0};
    size_t record_size{64};
    size_t buffer_size{0};

    flag_t acquire_flag{0};
    uint64_t alloc_hint{0};
    flag_t rw_flag{0};
    flag_t relinquish_flag{0};

    InitF init_func;
    SqlF sql_func;

    size_t io_times{0};
    size_t io_record_nr{0};

    static BenchConfig get_empty_conf(const std::string &name,
                                      size_t thread_nr,
                                      size_t coro_nr,
                                      size_t test_nr,
                                      size_t record_nr,
                                      size_t record_size)
    {
        BenchConfig ret;
        ret.name = name;
        ret.thread_nr = thread_nr;
        ret.coro_nr = coro_nr;
        ret.test_nr = test_nr;
        ret.record_nr = record_nr;
        ret.buffer_size = record_nr * record_size;
        ret.init_func = [](char *buffer, const BenchConfig &c) {
            auto record_nr = c.record_nr;
            auto record_size = c.record_size;
            for (size_t i = 0; i < record_nr; ++i)
            {
                auto &record = *(Record *) (buffer + record_size * i);
                memset(&record, 0, record_size);
                record.id = i;
                record.age = fast_pseudo_rand_int(0, 80 + 1);
                record.sex = fast_pseudo_rand_int(0, 1);
            }
        };
        return ret;
    }

    static BenchConfig get_record_conf_unprotected(const std::string &name,
                                                   size_t thread_nr,
                                                   size_t coro_nr,
                                                   size_t test_nr,
                                                   size_t record_nr,
                                                   size_t record_size)
    {
        BenchConfig ret = get_empty_conf(
            name, thread_nr, coro_nr, test_nr, record_nr, record_size);

        ret.acquire_flag = (flag_t) AcquireRequestFlag::kNoRpc |
                           (flag_t) AcquireRequestFlag::kNoGc;
        ret.alloc_hint = 0;
        ret.rw_flag = (flag_t) RWFlag::kUseUniversalRkey |
                      (flag_t) RWFlag::kNoLocalExpireCheck;
        ret.relinquish_flag = (flag_t) LeaseModifyFlag::kNoRpc;
        return ret;
    }
    static BenchConfig get_record_conf_mw_protected(const std::string &name,
                                                    size_t thread_nr,
                                                    size_t coro_nr,
                                                    size_t test_nr,
                                                    size_t record_nr,
                                                    size_t record_size)
    {
        BenchConfig ret = get_empty_conf(
            name, thread_nr, coro_nr, test_nr, record_nr, record_size);

        ret.acquire_flag = (flag_t) AcquireRequestFlag::kNoGc |
                           (flag_t) AcquireRequestFlag::kNoBindPR;
        ret.alloc_hint = 0;
        ret.rw_flag = (flag_t) RWFlag::kNoLocalExpireCheck;
        // TODO: disable unbind on relinquish
        // does it make sense?
        ret.relinquish_flag = (flag_t) LeaseModifyFlag::kNoRelinquishUnbindAny;
        return ret;
    }
    static BenchConfig get_record_conf_mr_protected(const std::string &name,
                                                    size_t thread_nr,
                                                    size_t coro_nr,
                                                    size_t test_nr,
                                                    size_t record_nr,
                                                    size_t record_size)
    {
        BenchConfig ret = get_record_conf_mw_protected(
            name, thread_nr, coro_nr, test_nr, record_nr, record_size);

        ret.acquire_flag |= (flag_t) AcquireRequestFlag::kUseMR;
        ret.relinquish_flag |= (flag_t) LeaseModifyFlag::kUseMR;
        return ret;
    }
};

struct AdditionalCoroCtx
{
    ssize_t thread_remain_task{0};
    size_t rdma_protection_nr{0};
};

void test_basic_client_worker(
    Patronus::pointer p,
    size_t coro_id,
    CoroYield &yield,
    const BenchConfig &conf,
    Buffer rdma_buffer,
    CoroExecutionContextWith<kMaxCoroNr, AdditionalCoroCtx> &ex)
{
    auto tid = p->get_thread_id();
    auto server_nid = ::config::get_server_nids().front();
    auto dir_id = tid % kServerThreadNr;
    CoroContext ctx(tid, &yield, &ex.master(), coro_id);

    size_t rdma_protection_nr = 0;
    size_t executed_nr = 0;

    size_t acquire_nr = 0;
    size_t rel_nr = 0;

    std::string key;
    std::string value;

    ChronoTimer timer;

    size_t io_size = conf.io_record_nr * conf.record_size;
    LOG_IF(WARNING, conf.buffer_size >= 2_GB)
        << "** mw may not work well with >= 2GB buffer. Run at your own risk";
    CHECK_GE(rdma_buffer.size, io_size);

    bool should_report_lat = tid == 0 && coro_id == 0;
    ChronoTimer bootstrap_timer;
    while (ex.get_private_data().thread_remain_task > 0)
    {
        if (should_report_lat)
        {
            bootstrap_timer.pin();
        }
        auto lease = p->get_rlease(server_nid,
                                   dir_id,
                                   GlobalAddress(0),
                                   conf.alloc_hint,
                                   conf.buffer_size,
                                   0ns,
                                   conf.acquire_flag,
                                   &ctx);
        if (should_report_lat)
        {
            // lat_m.collect(bootstrap_timer.pin());
        }
        acquire_nr++;
        if (unlikely(!lease.success()))
        {
            CHECK_EQ(lease.ec(), AcquireRequestStatus::kMagicMwErr);
            continue;
        }

        for (size_t i = 0; i < conf.io_times; ++i)
        {
            auto begin_idx = 0;
            auto end_idx = conf.record_nr - conf.io_record_nr;
            auto start_idx = fast_pseudo_rand_int(begin_idx, end_idx);
            auto offset = start_idx * conf.record_size;
            auto ec = p->read(
                lease, rdma_buffer.buffer, io_size, offset, conf.rw_flag, &ctx);
            CHECK_EQ(ec, kOk);
            conf.sql_func(
                rdma_buffer.buffer, conf.record_size, conf.io_record_nr);
        }

        p->relinquish(lease, conf.alloc_hint, conf.relinquish_flag, &ctx);
        rel_nr++;
        executed_nr++;
        ex.get_private_data().thread_remain_task--;
    }

    auto ns = timer.pin();

    auto &comm = ex.get_private_data();
    comm.rdma_protection_nr += rdma_protection_nr;

    VLOG(1) << "[bench] rdma_protection_nr: " << rdma_protection_nr
            << ". executed: " << executed_nr << ", take: " << ns << " ns. "
            << ", ops: " << 1e9 * executed_nr / ns
            << " . acquire_nr: " << acquire_nr << ", rel_nr: " << rel_nr
            << ". ctx: " << ctx;

    ex.worker_finished(coro_id);
    ctx.yield_to_master();
}

void test_basic_client_master(
    Patronus::pointer p,
    CoroYield &yield,
    size_t test_nr,
    std::atomic<ssize_t> &atm_task_nr,
    size_t coro_nr,
    CoroExecutionContextWith<kMaxCoroNr, AdditionalCoroCtx> &ex)
{
    auto tid = p->get_thread_id();

    CoroContext mctx(tid, &yield, ex.workers());
    CHECK(mctx.is_master());

    ssize_t task_per_sync = test_nr / 20;
    LOG_IF(WARNING, tid == 0 && task_per_sync <= (ssize_t) coro_nr)
        << "task_per_sync " << task_per_sync
        << " less than coro_nr: " << coro_nr;
    task_per_sync = std::max(task_per_sync, ssize_t(coro_nr));
    ssize_t remain =
        atm_task_nr.fetch_sub(task_per_sync, std::memory_order_relaxed) -
        task_per_sync;
    ex.get_private_data().thread_remain_task = task_per_sync;

    for (size_t i = 0; i < coro_nr; ++i)
    {
        mctx.yield_to_worker(i);
    }

    coro_t coro_buf[2 * kMaxCoroNr];
    while (true)
    {
        if ((ssize_t) ex.get_private_data().thread_remain_task <=
            2 * ssize_t(coro_nr))
        {
            // refill the thread_remain_task
            auto cur_task_nr = std::min(remain, task_per_sync);
            if (cur_task_nr >= 0)
            {
                remain = atm_task_nr.fetch_sub(cur_task_nr,
                                               std::memory_order_relaxed) -
                         cur_task_nr;
                if (remain >= 0)
                {
                    ex.get_private_data().thread_remain_task += cur_task_nr;
                }
            }
        }
        auto nr = p->try_get_client_continue_coros(coro_buf, 2 * kMaxCoroNr);
        for (size_t i = 0; i < nr; ++i)
        {
            auto coro_id = coro_buf[i];
            mctx.yield_to_worker(coro_id);
        }

        if (remain <= 0)
        {
            if (ex.is_finished_all())
            {
                CHECK_LE(remain, 0);
                break;
            }
        }
    }
}

std::ostream &operator<<(std::ostream &os, const BenchConfig &conf)
{
    os << "{conf: name: " << conf.name << ", thread: " << conf.thread_nr
       << ", coro: " << conf.coro_nr << ", test: " << conf.test_nr
       << ", enter: " << conf.first_enter
       << ", leave: " << conf.server_should_leave
       << ", report: " << conf.should_report << "}";
    return os;
}

class ConfigFactory
{
public:
    static std::vector<BenchConfig> get_unprotected(const std::string &name,
                                                    size_t thread_nr,
                                                    size_t coro_nr,
                                                    size_t test_nr,
                                                    size_t record_nr,
                                                    size_t record_size,
                                                    size_t io_times,
                                                    size_t io_record_nr,
                                                    BenchConfig::SqlF sql_func)
    {
        auto warmup_conf = BenchConfig::get_record_conf_unprotected(
            name, thread_nr, coro_nr, test_nr, record_nr, record_size);
        warmup_conf.should_report = false;
        warmup_conf.sql_func = sql_func;
        warmup_conf.io_times = io_times;
        warmup_conf.io_record_nr = io_record_nr;

        auto query_conf = warmup_conf;
        query_conf.should_report = true;
        return pipeline({warmup_conf, query_conf});
    }

    static std::vector<BenchConfig> get_mw(const std::string &name,
                                           size_t thread_nr,
                                           size_t coro_nr,
                                           size_t test_nr,
                                           size_t record_nr,
                                           size_t record_size,
                                           size_t io_times,
                                           size_t io_record_nr,
                                           BenchConfig::SqlF sql_func)
    {
        auto warmup_conf = BenchConfig::get_record_conf_mw_protected(
            name, thread_nr, coro_nr, test_nr, record_nr, record_size);
        warmup_conf.should_report = false;
        warmup_conf.sql_func = sql_func;
        warmup_conf.io_times = io_times;
        warmup_conf.io_record_nr = io_record_nr;

        auto query_conf = warmup_conf;
        query_conf.should_report = true;
        return pipeline({warmup_conf, query_conf});
    }

    static std::vector<BenchConfig> get_mr(const std::string &name,
                                           size_t thread_nr,
                                           size_t coro_nr,
                                           size_t test_nr,
                                           size_t record_nr,
                                           size_t record_size,
                                           size_t io_times,
                                           size_t io_record_nr,
                                           BenchConfig::SqlF sql_func)
    {
        auto warmup_conf = BenchConfig::get_record_conf_mr_protected(
            name, thread_nr, coro_nr, test_nr, record_nr, record_size);
        warmup_conf.should_report = false;
        warmup_conf.sql_func = sql_func;
        warmup_conf.io_times = io_times;
        warmup_conf.io_record_nr = io_record_nr;

        auto query_conf = warmup_conf;
        query_conf.should_report = true;
        return pipeline({warmup_conf, query_conf});
    }

private:
    static std::vector<BenchConfig> pipeline(std::vector<BenchConfig> &&confs)
    {
        for (auto &conf : confs)
        {
            conf.first_enter = false;
            conf.server_should_leave = false;
        }
        if (confs.empty())
        {
            return confs;
        }
        confs.front().first_enter = true;
        confs.back().server_should_leave = true;
        return confs;
    }
};

std::atomic<ssize_t> g_total_test_nr;
void benchmark_client(Patronus::pointer p,
                      boost::barrier &bar,
                      const BenchConfig &conf,
                      bool is_master,
                      uint64_t key)
{
    auto coro_nr = conf.coro_nr;
    auto thread_nr = conf.thread_nr;
    bool first_enter = conf.first_enter;
    bool server_should_leave = conf.server_should_leave;
    CHECK_LE(coro_nr, kMaxCoroNr);

    auto tid = p->get_thread_id();
    if (is_master)
    {
        // init here by master
        g_total_test_nr = conf.test_nr;
        if (first_enter)
        {
            p->keeper_barrier("server_ready-" + std::to_string(key), 100ms);
        }
    }

    // auto min = util::time::to_ns(0ns);
    // auto max = util::time::to_ns(10us);
    // auto rng = util::time::to_ns(1us);
    // OnePassBucketMonitor<uint64_t> lat_m(min, max, rng);

    bar.wait();

    ChronoTimer timer;
    CoroExecutionContextWith<kMaxCoroNr, AdditionalCoroCtx> ex;
    if (tid < thread_nr)
    {
        auto self_managed_rdma_buffer = p->get_self_managed_rdma_buffer();
        auto coro_buffer_size = self_managed_rdma_buffer.size / coro_nr;

        ex.get_private_data().thread_remain_task = 0;
        for (size_t i = coro_nr; i < kMaxCoroNr; ++i)
        {
            // no that coro, so directly finished.
            ex.worker_finished(i);
        }
        for (size_t i = 0; i < coro_nr; ++i)
        {
            ex.worker(i) = CoroCall([p,
                                     coro_id = i,
                                     &conf,
                                     &ex,
                                     self_managed_rdma_buffer,
                                     coro_buffer_size](CoroYield &yield) {
                auto *coro_buffer = self_managed_rdma_buffer.buffer +
                                    coro_buffer_size * coro_id;
                test_basic_client_worker(p,
                                         coro_id,
                                         yield,
                                         conf,
                                         Buffer(coro_buffer, coro_buffer_size),
                                         ex);
            });
        }
        auto &master = ex.master();
        master = CoroCall(
            [p, &ex, test_nr = conf.test_nr, coro_nr](CoroYield &yield) {
                test_basic_client_master(
                    p, yield, test_nr, g_total_test_nr, coro_nr, ex);
            });

        master();

        p->put_self_managed_rdma_buffer(self_managed_rdma_buffer);
    }
    bar.wait();
    auto ns = timer.pin();

    double ops = 1e9 * conf.test_nr / ns;
    double avg_ns = 1.0 * ns / conf.test_nr;
    LOG_IF(INFO, is_master)
        << "[bench] total op: " << conf.test_nr << ", ns: " << ns
        << ", ops: " << ops << ", avg " << avg_ns << " ns";

    if (is_master && server_should_leave)
    {
        LOG(INFO) << "p->finished(" << key << ")";
        p->finished(key);
    }

    if (is_master && conf.should_report)
    {
        col_idx.push_back(conf.name);
        col_x_thread_nr.push_back(conf.thread_nr);
        col_x_coro_nr.push_back(conf.coro_nr);
        col_x_record_size.push_back(conf.record_nr);
        col_x_record_nr.push_back(conf.record_nr);
        col_x_record_buffer_size.push_back(conf.buffer_size);
        col_x_io_times.push_back(conf.io_times);
        col_x_io_record_nr.push_back(conf.io_record_nr);
        col_test_op_nr.push_back(conf.test_nr);
        col_ns.push_back(ns);

        const auto &prv = ex.get_private_data();
        col_rdma_protection_err.push_back(prv.rdma_protection_nr);

        // if (unlikely(col_lat_idx.empty()))
        // {
        //     col_lat_idx.push_back("lat_min");
        //     col_lat_idx.push_back("lat_p5");
        //     col_lat_idx.push_back("lat_p9");
        //     col_lat_idx.push_back("lat_p99");
        // }
        // lat_data[conf.name].push_back(lat_m.min());
        // lat_data[conf.name].push_back(lat_m.percentile(0.5));
        // lat_data[conf.name].push_back(lat_m.percentile(0.9));
        // lat_data[conf.name].push_back(lat_m.percentile(0.99));
    }

    bar.wait();
}

void server_init(Patronus::pointer p, const BenchConfig &conf)
{
    auto buffer = p->get_server_internal_buffer();
    conf.init_func(buffer.buffer, conf);
}

void benchmark_server(Patronus::pointer p,
                      boost::barrier &bar,
                      const std::vector<BenchConfig> &confs,
                      bool is_master,
                      uint64_t key)
{
    auto thread_nr = confs[0].thread_nr;
    for (const auto &conf : confs)
    {
        CHECK_EQ(conf.thread_nr, thread_nr);
    }
    if (is_master)
    {
        server_init(p, confs[0]);
        p->finished(key);
    }

    bar.wait();
    if (is_master)
    {
        p->keeper_barrier("server_ready-" + std::to_string(key), 100ms);
    }

    p->server_serve(key);
    bar.wait();
}

void benchmark(Patronus::pointer p, boost::barrier &bar, bool is_client)
{
    uint64_t key = 0;
    bool is_master = p->get_thread_id() == 0;
    bar.wait();

    std::map<std::string, BenchConfig::SqlF> sqls;
    sqls["sum_age"] = [](char *buffer, size_t record_size, size_t record_nr) {
        uint64_t age = 0;
        volatile uint64_t magic_age = 0;
        for (size_t i = 0; i < record_nr; ++i)
        {
            auto &record = *(Record *) (buffer + i * record_size);
            age += record.age;
        }
        magic_age = age;
    };

    // very easy to saturate, no need to test many threads
    // for (size_t thread_nr : {1, 2, 4, 8, 16, 32})
    for (size_t thread_nr : {32})
    {
        for (size_t io_times : {64})
        {
            for (size_t record_size : {64})
            {
                for (size_t io_record_nr : {1})
                {
                    // for (size_t record_buffer_size : {4_KB, 16_MB})
                    for (size_t record_buffer_size : {16_MB})
                    // for (size_t record_buffer_size :
                    //      {4_KB, 256_KB, 16_MB, 1_GB})
                    {
                        for (const auto &[name, sql] : sqls)
                        {
                            size_t coro_nr = 1;

                            CHECK_EQ(record_buffer_size % record_size, 0);
                            size_t record_nr = record_buffer_size / record_size;
                            {
                                key++;
                                auto confs = ConfigFactory::get_mw(name + ".mw",
                                                                   thread_nr,
                                                                   coro_nr,
                                                                   20_K,
                                                                   record_nr,
                                                                   record_size,
                                                                   io_times,
                                                                   io_record_nr,
                                                                   sql);
                                if (is_client)
                                {
                                    for (const auto &conf : confs)
                                    {
                                        LOG_IF(INFO, is_master)
                                            << "[conf] " << conf;
                                        benchmark_client(
                                            p, bar, conf, is_master, key);
                                    }
                                }
                                else
                                {
                                    benchmark_server(
                                        p, bar, confs, is_master, key);
                                }
                            }

                            {
                                key++;
                                auto confs = ConfigFactory::get_unprotected(
                                    name + ".unprot",
                                    thread_nr,
                                    coro_nr,
                                    20_K,
                                    record_nr,
                                    record_size,
                                    io_times,
                                    io_record_nr,
                                    sql);
                                if (is_client)
                                {
                                    for (const auto &conf : confs)
                                    {
                                        LOG_IF(INFO, is_master)
                                            << "[conf] " << conf;
                                        benchmark_client(
                                            p, bar, conf, is_master, key);
                                    }
                                }
                                else
                                {
                                    benchmark_server(
                                        p, bar, confs, is_master, key);
                                }
                            }
                            {
                                key++;
                                auto confs = ConfigFactory::get_mr(name + ".mr",
                                                                   thread_nr,
                                                                   coro_nr,
                                                                   500,
                                                                   record_nr,
                                                                   record_size,
                                                                   io_times,
                                                                   io_record_nr,
                                                                   sql);
                                if (is_client)
                                {
                                    for (const auto &conf : confs)
                                    {
                                        LOG_IF(INFO, is_master)
                                            << "[conf] " << conf;
                                        benchmark_client(
                                            p, bar, conf, is_master, key);
                                    }
                                }
                                else
                                {
                                    benchmark_server(
                                        p, bar, confs, is_master, key);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // for (size_t thread_nr : {32})
    // {
    //     // for (size_t coro_nr : {2, 4, 8, 16})
    //     for (size_t coro_nr : {2, 4, 8, 16})
    //     {
    //         for (size_t io_times : {64})
    //         {
    //             for (size_t record_size : {64})
    //             {
    //                 for (size_t io_record_nr : {1})
    //                 {
    //                     // for (size_t record_buffer_size : {4_KB, 16_MB})
    //                     for (size_t record_buffer_size : {16_MB})
    //                     // for (size_t record_buffer_size :
    //                     //      {4_KB, 256_KB, 16_MB, 1_GB})
    //                     {
    //                         for (const auto &[name, sql] : sqls)
    //                         {
    //                             CHECK_EQ(record_buffer_size % record_size,
    //                             0); size_t record_nr =
    //                                 record_buffer_size / record_size;
    //                             {
    //                                 key++;
    //                                 auto confs =
    //                                     ConfigFactory::get_mw(name + ".mw",
    //                                                           thread_nr,
    //                                                           coro_nr,
    //                                                           20_K,
    //                                                           record_nr,
    //                                                           record_size,
    //                                                           io_times,
    //                                                           io_record_nr,
    //                                                           sql);
    //                                 if (is_client)
    //                                 {
    //                                     for (const auto &conf : confs)
    //                                     {
    //                                         LOG_IF(INFO, is_master)
    //                                             << "[conf] " << conf;
    //                                         benchmark_client(
    //                                             p, bar, conf, is_master,
    //                                             key);
    //                                     }
    //                                 }
    //                                 else
    //                                 {
    //                                     benchmark_server(
    //                                         p, bar, confs, is_master, key);
    //                                 }
    //                             }

    //                             {
    //                                 key++;
    //                                 auto confs =
    //                                 ConfigFactory::get_unprotected(
    //                                     name + ".unprot",
    //                                     thread_nr,
    //                                     coro_nr,
    //                                     20_K,
    //                                     record_nr,
    //                                     record_size,
    //                                     io_times,
    //                                     io_record_nr,
    //                                     sql);
    //                                 if (is_client)
    //                                 {
    //                                     for (const auto &conf : confs)
    //                                     {
    //                                         LOG_IF(INFO, is_master)
    //                                             << "[conf] " << conf;
    //                                         benchmark_client(
    //                                             p, bar, conf, is_master,
    //                                             key);
    //                                     }
    //                                 }
    //                                 else
    //                                 {
    //                                     benchmark_server(
    //                                         p, bar, confs, is_master, key);
    //                                 }
    //                             }
    //                             {
    //                                 key++;
    //                                 auto confs =
    //                                     ConfigFactory::get_mr(name + ".mr",
    //                                                           thread_nr,
    //                                                           coro_nr,
    //                                                           500,
    //                                                           record_nr,
    //                                                           record_size,
    //                                                           io_times,
    //                                                           io_record_nr,
    //                                                           sql);
    //                                 if (is_client)
    //                                 {
    //                                     for (const auto &conf : confs)
    //                                     {
    //                                         LOG_IF(INFO, is_master)
    //                                             << "[conf] " << conf;
    //                                         benchmark_client(
    //                                             p, bar, conf, is_master,
    //                                             key);
    //                                     }
    //                                 }
    //                                 else
    //                                 {
    //                                     benchmark_server(
    //                                         p, bar, confs, is_master, key);
    //                                 }
    //                             }
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //     }
    // }
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    PatronusConfig pconfig;
    pconfig.machine_nr = ::config::kMachineNr;
    pconfig.block_class = {2_MB, 8_KB};
    pconfig.block_ratio = {0.5, 0.5};
    pconfig.reserved_buffer_size = 2_GB;
    pconfig.lease_buffer_size = (kDSMCacheSize - 2_GB) / 2;
    pconfig.alloc_buffer_size = (kDSMCacheSize - 2_GB) / 2;

    auto patronus = Patronus::ins(pconfig);

    std::vector<std::thread> threads;
    auto nid = patronus->get_node_id();

    bool is_client = ::config::is_client(nid);

    if (is_client)
    {
        patronus->registerClientThread();
    }
    else
    {
        patronus->registerServerThread();
    }

    if (is_client)
    {
        boost::barrier bar(kClientThreadNr);
        for (size_t i = 0; i < kClientThreadNr - 1; ++i)
        {
            threads.emplace_back([patronus, &bar]() {
                patronus->registerClientThread();
                bar.wait();
                benchmark(patronus, bar, true);
            });
        }
        bar.wait();
        patronus->keeper_barrier("begin", 100ms);
        benchmark(patronus, bar, true);

        for (auto &t : threads)
        {
            t.join();
        }
    }
    else
    {
        boost::barrier bar(kServerThreadNr);
        for (size_t i = 0; i < kServerThreadNr - 1; ++i)
        {
            threads.emplace_back([patronus, &bar]() {
                patronus->registerServerThread();
                bar.wait();
                benchmark(patronus, bar, false);
            });
        }
        bar.wait();
        patronus->keeper_barrier("begin", 100ms);
        benchmark(patronus, bar, false);

        for (auto &t : threads)
        {
            t.join();
        }
    }

    {
        StrDataFrame df;
        df.load_index(std::move(col_idx));
        df.load_column<size_t>("x_thread_nr", std::move(col_x_thread_nr));
        df.load_column<size_t>("x_coro_nr", std::move(col_x_coro_nr));
        df.load_column<size_t>("x_record_size(B)",
                               std::move(col_x_record_size));
        df.load_column<size_t>("x_record_nr", std::move(col_x_record_nr));
        df.load_column<size_t>("x_record_buffer_size(B)",
                               std::move(col_x_record_buffer_size));
        df.load_column<size_t>("x_io_times", std::move(col_x_io_times));
        df.load_column<size_t>("x_io_record_nr", std::move(col_x_io_record_nr));

        df.load_column<size_t>("test_nr(total)", std::move(col_test_op_nr));
        df.load_column<size_t>("test_ns(total)", std::move(col_ns));
        df.load_column<size_t>("rdma_prot_err_nr",
                               std::move(col_rdma_protection_err));

        auto div_f = gen_F_div<size_t, size_t, double>();
        auto div_f2 = gen_F_div<double, size_t, double>();
        auto ops_f = gen_F_ops<size_t, size_t, double>();
        auto mul_f = gen_F_mul<double, size_t, double>();
        auto mul_f2 = gen_F_mul<size_t, size_t, size_t>();

        auto client_nr = ::config::get_client_nids().size();
        auto replace_mul_cluster = gen_replace_F_mul<size_t>(client_nr);
        df.consolidate<size_t, size_t, size_t>(
            "x_thread_nr", "x_coro_nr", "client_nr(effective)", mul_f2, false);
        df.replace<size_t>("client_nr(effective)", replace_mul_cluster);

        df.consolidate<size_t, size_t, size_t>("test_ns(total)",
                                               "client_nr(effective)",
                                               "test_ns(effective)",
                                               mul_f2,
                                               false);
        df.consolidate<size_t, size_t, double>(
            "test_ns(total)", "test_nr(total)", "lat(div)", div_f, false);
        df.consolidate<size_t, size_t, double>(
            "test_ns(effective)", "test_nr(total)", "lat(avg)", div_f, false);
        df.consolidate<size_t, size_t, double>(
            "test_nr(total)", "test_ns(total)", "ops(total)", ops_f, false);
        df.consolidate<double, size_t, double>(
            "ops(total)", "x_thread_nr", "ops(thread)", div_f2, false);

        auto replace_mul_f = gen_replace_F_mul<double>(client_nr);
        df.load_column<double>("ops(cluster)",
                               df.get_column<double>("ops(total)"));
        df.replace<double>("ops(cluster)", replace_mul_f);

        df.consolidate<double, size_t, double>(
            "ops(thread)", "x_coro_nr", "ops(coro)", div_f2, false);

        auto filename = binary_to_csv_filename(argv[0], FLAGS_exec_meta);
        df.write<std::ostream, std::string, size_t, double>(std::cout,
                                                            io_format::csv2);
        df.write<std::string, size_t, double>(filename.c_str(),
                                              io_format::csv2);
    }

    {
        StrDataFrame df;
        df.load_index(std::move(col_lat_idx));
        for (auto &[name, vec] : lat_data)
        {
            df.load_column<uint64_t>(name.c_str(), std::move(vec));
        }
        std::map<std::string, std::string> info;
        info.emplace("kind", "lat");
        auto filename = binary_to_csv_filename(argv[0], FLAGS_exec_meta, info);
        df.write<std::ostream, std::string, size_t, double>(std::cout,
                                                            io_format::csv2);
        df.write<std::string, size_t, double>(filename.c_str(),
                                              io_format::csv2);
    }

    patronus->keeper_barrier("finished", 100ms);

    LOG(WARNING) << "TODO: this benchmark has a problem. when thread_nr >= 32 "
                    "&& coro_nr >= 2, performance drops";
    LOG(WARNING) << "TODO: fxxking, can not add lat_m. Strange segment fault "
                    "at dctor of shared_ptr of patronus";

    LOG(INFO) << "finished. ctrl+C to quit.";
}