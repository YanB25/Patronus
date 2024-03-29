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
#include "thirdparty/linked-list/conf.h"
#include "thirdparty/linked-list/list.h"
#include "thirdparty/linked-list/list_handle.h"
#include "util/BenchRand.h"
#include "util/DataFrameF.h"
#include "util/Pre.h"
#include "util/Rand.h"
#include "util/TimeConv.h"

using namespace patronus::list;
using namespace util::literals;
using namespace patronus;
using namespace hmdf;

constexpr static size_t kServerThreadNr = NR_DIRECTORY;
constexpr static size_t kClientThreadNr = kMaxAppThread;
constexpr static size_t kMaxCoroNr = 1;

DEFINE_string(exec_meta, "", "The meta data of this execution");

std::vector<std::string> col_idx;
std::vector<size_t> col_x_thread_nr;
std::vector<size_t> col_x_coro_nr;
std::vector<size_t> col_x_producer_nr;
std::vector<size_t> col_x_consumer_nr;
std::vector<size_t> col_test_nr;
std::vector<size_t> col_ns;

std::vector<double> col_producer_succ_rate;
std::vector<double> col_consumer_succ_rate;
std::vector<size_t> col_rdma_protection_err;

std::vector<std::string> col_lat_idx;
std::vector<uint64_t> col_lat_min;
std::vector<uint64_t> col_lat_p5;
std::vector<uint64_t> col_lat_p9;
std::vector<uint64_t> col_lat_p99;
std::unordered_map<std::string, std::vector<uint64_t>> lat_data;

struct ListItem
{
    uint64_t nid;
    uint64_t tid;
    uint64_t coro_id;
    uint64_t magic_number;
};

inline std::ostream &operator<<(std::ostream &os, const ListItem &item)
{
    os << "{ListItem nid: " << item.nid << ", tid: " << item.tid
       << ", coro_id: " << item.coro_id
       << ", magic_number: " << (void *) item.magic_number << "}";
    return os;
}

struct BenchConfig
{
    std::string name;
    size_t thread_nr{1};
    size_t producer_nr{0};
    size_t consumer_nr{0};
    size_t coro_nr{1};
    size_t test_nr{0};
    bool should_report{false};

    bool should_report_lat() const
    {
        return should_report && thread_nr == 32 && coro_nr == 1;
    }

    bool should_enter(size_t tid) const
    {
        return tid < thread_nr;
    }

    void validate() const
    {
        CHECK_EQ(thread_nr, producer_nr + consumer_nr);
    }
    size_t cluster_client_nr() const
    {
        return local_client_nr() * ::config::get_client_nids().size();
    }
    size_t local_client_nr() const
    {
        return thread_nr * coro_nr;
    }
    std::string conf_name() const
    {
        return name;
    }
    bool is_consumer(size_t tid) const
    {
        return should_enter(tid) && (!is_producer(tid));
    }
    bool is_producer(size_t tid) const
    {
        return should_enter(tid) && tid < producer_nr;
    }

    BenchConfig clone() const
    {
        BenchConfig ret;
        ret.name = name;
        ret.thread_nr = thread_nr;
        ret.coro_nr = coro_nr;
        ret.test_nr = test_nr;
        ret.should_report = should_report;
        return ret;
    }
    static BenchConfig get_conf(const std::string &name,
                                size_t test_nr,
                                size_t producer_nr,
                                size_t consumer_nr,
                                size_t coro_nr)
    {
        BenchConfig conf;
        conf.name = name;
        conf.test_nr = test_nr;
        conf.producer_nr = producer_nr;
        conf.consumer_nr = consumer_nr;
        conf.thread_nr = producer_nr + consumer_nr;
        conf.coro_nr = coro_nr;
        conf.test_nr = test_nr;
        conf.should_report = true;

        conf.validate();
        return conf;
    }
};
std::ostream &operator<<(std::ostream &os, const BenchConfig &conf)
{
    os << "{conf: name: " << conf.name << ", thread_nr: " << conf.thread_nr
       << ", coro_nr: " << conf.coro_nr
       << "(local_client_nr: " << conf.local_client_nr()
       << ", cluster: " << conf.cluster_client_nr()
       << "), test_nr: " << conf.test_nr
       << ", should_report: " << conf.should_report << "}";
    return os;
}

template <typename T>
typename ListHandle<T>::pointer gen_handle(Patronus::pointer p,
                                           size_t dir_id,
                                           const ListHandleConfig &conf,
                                           GlobalAddress meta_gaddr,
                                           CoroContext *ctx)
{
    auto server_nid = ::config::get_server_nids().front();

    DVLOG(1) << "Getting from race:meta_gaddr got " << meta_gaddr;

    auto rdma_adpt = patronus::RdmaAdaptor::new_instance(
        server_nid, dir_id, p, conf.bypass_prot, false /* two sided */, ctx);

    auto handle =
        ListHandle<T>::new_instance(server_nid, meta_gaddr, rdma_adpt, conf);

    return handle;
}

struct AdditionalCoroCtx
{
    ssize_t thread_remain_task{0};
    size_t put_nr{0};
    size_t put_succ_nr{0};
    size_t get_nr{0};
    size_t get_succ_nr{0};
    size_t rdma_protection_nr{0};
};

void reg_result(const std::string &name,
                const BenchConfig &bench_conf,
                const AdditionalCoroCtx &prv,
                uint64_t ns)
{
    col_idx.push_back(name);
    col_x_thread_nr.push_back(bench_conf.thread_nr);
    col_x_coro_nr.push_back(bench_conf.coro_nr);
    col_x_producer_nr.push_back(bench_conf.producer_nr);
    col_x_consumer_nr.push_back(bench_conf.consumer_nr);
    col_ns.push_back(ns);
    col_test_nr.push_back(bench_conf.test_nr);

    col_consumer_succ_rate.push_back(1.0 * prv.get_succ_nr / prv.get_nr);
    col_producer_succ_rate.push_back(1.0 * prv.put_succ_nr / prv.put_nr);

    col_rdma_protection_err.push_back(prv.rdma_protection_nr);
}

void reg_latency(const std::string &name,
                 const OnePassBucketMonitor<uint64_t> &lat_m)
{
    if (col_lat_idx.empty())
    {
        col_lat_idx.push_back("lat_min");
        col_lat_idx.push_back("lat_p5");
        col_lat_idx.push_back("lat_p9");
        col_lat_idx.push_back("lat_p99");
    }
    lat_data[name].push_back(lat_m.min());
    lat_data[name].push_back(lat_m.percentile(0.5));
    lat_data[name].push_back(lat_m.percentile(0.9));
    lat_data[name].push_back(lat_m.percentile(0.99));

    CHECK_EQ(lat_data[name].size(), col_lat_idx.size())
        << "** duplicated test detected.";
}
template <typename T>
typename List<T>::pointer gen_list(Patronus::pointer p,
                                   const ListConfig &config)
{
    auto allocator = p->get_allocator(0 /* default hint */);

    auto server_rdma_adpt = patronus::RdmaAdaptor::new_instance(p);
    auto list = List<T>::new_instance(server_rdma_adpt, allocator, config);

    auto meta_gaddr = list->meta_gaddr();
    p->put("race:meta_gaddr", meta_gaddr, 0ns);
    LOG(INFO) << "Puting to race:meta_gaddr with " << meta_gaddr;

    return list;
}

void test_basic_client_worker(
    Patronus::pointer p,
    size_t coro_id,
    CoroYield &yield,
    const BenchConfig &bench_conf,
    const ListHandleConfig &handle_conf,
    GlobalAddress meta_gaddr,
    CoroExecutionContextWith<kMaxCoroNr, AdditionalCoroCtx> &ex,
    OnePassBucketMonitor<uint64_t> &lat_m)
{
    std::ignore = bench_conf;

    auto nid = p->get_node_id();
    auto tid = p->get_thread_id();
    auto dir_id = tid % kServerThreadNr;
    CoroContext ctx(tid, &yield, &ex.master(), coro_id);

    auto handle =
        gen_handle<ListItem>(p, dir_id, handle_conf, meta_gaddr, &ctx);

    size_t put_succ_nr = 0;
    size_t put_nomem_nr = 0;
    size_t put_retry_nr = 0;
    size_t get_succ_nr = 0;
    size_t get_nomem_nr = 0;
    size_t rdma_protection_nr = 0;
    size_t executed_nr = 0;

    bool is_producer = bench_conf.is_producer(tid);
    bool is_consumer = bench_conf.is_consumer(tid);
    CHECK_EQ(is_producer + is_consumer, 1);

    ChronoTimer op_timer;
    // util::TraceManager tm(0.1);
    while (ex.get_private_data().thread_remain_task > 0)
    {
        if (is_consumer)
        {
            // pop
            // auto trace = tm.trace("pop");
            ListItem item;
            auto rc = handle->pop_front(&item);
            if (rc == kOk)
            {
                get_succ_nr++;
                if (bench_conf.should_report_lat())
                {
                    lat_m.collect(op_timer.pin());
                }
                ex.get_private_data().thread_remain_task--;
                // if (trace.enabled())
                // {
                //     LOG(INFO) << trace;
                // }
            }
            else
            {
                get_nomem_nr++;
            }
        }
        else
        {
            // push
            // auto trace = tm.trace("push");
            ListItem item;
            item.nid = nid;
            item.tid = tid;
            item.coro_id = coro_id;
            item.magic_number = 0;
            auto rc = handle->push_back(item);
            if (rc == kOk)
            {
                put_succ_nr++;
                if (bench_conf.should_report_lat())
                {
                    lat_m.collect(op_timer.pin());
                }
                ex.get_private_data().thread_remain_task--;
                // if (trace.enabled())
                // {
                //     LOG(INFO) << trace;
                // }
            }
            else
            {
                CHECK_EQ(rc, RC::kRetry);
                put_retry_nr++;
            }
        }
    }

    auto &comm = ex.get_private_data();
    comm.put_succ_nr += put_succ_nr;
    comm.put_nr += put_succ_nr + put_nomem_nr + put_retry_nr;
    comm.get_succ_nr += get_succ_nr;
    comm.get_nr += get_succ_nr + get_nomem_nr;
    comm.rdma_protection_nr += rdma_protection_nr;

    LOG(INFO) << "[bench] put: succ: " << put_succ_nr
              << ", nomem: " << put_nomem_nr << ", retry: " << put_retry_nr
              << ", get: succ: " << get_succ_nr
              << ", not found: " << get_nomem_nr
              << ", rdma_protection_nr: " << rdma_protection_nr
              << ". executed: " << executed_nr << ". ctx: " << ctx;

    handle.reset();

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

    ssize_t task_per_sync = test_nr / 100;
    LOG_IF(WARNING, task_per_sync <= (ssize_t) coro_nr);
    task_per_sync = std::max(task_per_sync, ssize_t(coro_nr));
    ssize_t remain =
        atm_task_nr.fetch_sub(task_per_sync, std::memory_order_relaxed) -
        task_per_sync;
    ex.get_private_data().thread_remain_task = task_per_sync;

    for (size_t i = coro_nr; i < kMaxCoroNr; ++i)
    {
        ex.worker_finished(i);
    }

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
            // DVLOG(1) << "[bench] yielding due to CQE: " << (int) coro_id;
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

std::atomic<ssize_t> g_total_test_nr;
GlobalAddress g_meta_gaddr;
void benchmark_client(Patronus::pointer p,
                      boost::barrier &bar,
                      bool is_master,
                      const BenchConfig &bench_conf,
                      const ListHandleConfig &handle_conf,
                      uint64_t key)
{
    auto coro_nr = bench_conf.coro_nr;
    // auto thread_nr = bench_conf.thread_nr;
    bool first_enter = true;
    bool server_should_leave = true;
    CHECK_LE(coro_nr, kMaxCoroNr);
    size_t actual_test_nr = bench_conf.test_nr;

    auto tid = p->get_thread_id();
    // auto nid = p->get_node_id();
    if (is_master)
    {
        // init here by master
        g_total_test_nr = actual_test_nr;
        if (first_enter)
        {
            p->keeper_barrier("server_ready-" + std::to_string(key), 100ms);
            // fetch meta_gaddr here by master thread
            // because it may be slow
            g_meta_gaddr = p->get_object<GlobalAddress>("race:meta_gaddr", 1ms);
        }
    }
    bar.wait();

    auto min = util::time::to_ns(0ns);
    auto max = util::time::to_ns(1ms);
    auto rng = util::time::to_ns(1us);
    OnePassBucketMonitor lat_m(min, max, rng);

    ChronoTimer timer;
    CoroExecutionContextWith<kMaxCoroNr, AdditionalCoroCtx> ex;
    bool should_enter = bench_conf.should_enter(tid);

    if (should_enter)
    {
        ex.get_private_data().thread_remain_task = 0;
        for (size_t i = coro_nr; i < kMaxCoroNr; ++i)
        {
            // no that coro, so directly finished.
            ex.worker_finished(i);
        }
        for (size_t i = 0; i < coro_nr; ++i)
        {
            ex.worker(i) =
                CoroCall([p,
                          coro_id = i,
                          &bench_conf,
                          &handle_conf,
                          &ex,
                          &lat_m,
                          meta_gaddr = g_meta_gaddr](CoroYield &yield) {
                    test_basic_client_worker(p,
                                             coro_id,
                                             yield,
                                             bench_conf,
                                             handle_conf,
                                             meta_gaddr,
                                             ex,
                                             lat_m);
                });
        }
        auto &master = ex.master();
        master = CoroCall([p, &ex, actual_test_nr = actual_test_nr, coro_nr](
                              CoroYield &yield) {
            test_basic_client_master(
                p, yield, actual_test_nr, g_total_test_nr, coro_nr, ex);
        });

        master();
    }

    bar.wait();
    auto ns = timer.pin();

    double ops = 1e9 * actual_test_nr / ns;
    double avg_ns = 1.0 * ns / actual_test_nr;
    LOG_IF(INFO, is_master)
        << "[bench] total op: " << actual_test_nr << ", ns: " << ns
        << ", ops: " << ops << ", avg " << avg_ns << " ns";

    if (is_master && server_should_leave)
    {
        LOG(INFO) << "p->finished(" << key << ")";
        p->finished(key);
    }

    auto report_name = bench_conf.conf_name();
    if (is_master && bench_conf.should_report)
    {
        reg_result(report_name, bench_conf, ex.get_private_data(), ns);

        if (bench_conf.should_report_lat())
        {
            reg_latency(report_name, lat_m);
        }
    }
}

void benchmark_server(Patronus::pointer p,
                      boost::barrier &bar,
                      bool is_master,
                      const std::vector<BenchConfig> &confs,
                      const ListConfig &list_config,
                      uint64_t key)
{
    auto thread_nr = confs[0].thread_nr;
    for (const auto &conf : confs)
    {
        thread_nr = std::max(thread_nr, conf.thread_nr);
    }

    typename List<ListItem>::pointer list;

    if (is_master)
    {
        p->finished(key);
        list = gen_list<ListItem>(p, list_config);
    }
    // wait for everybody to finish preparing
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

    std::vector<ListHandleConfig> handle_configs;
    handle_configs.emplace_back(ListHandleConfig().unprot().use_lock_free());
    handle_configs.emplace_back(ListHandleConfig().use_mw().use_lock_free());

    for (const auto &handle_conf : handle_configs)
    {
        // for (size_t producer_nr : {1, 2, 8, 16, 32})
        // for (size_t producer_nr : {1, 8, 32})
        for (size_t producer_nr : {1, 2, 4})
        {
            for (size_t consumer_nr : {0})
            {
                constexpr static size_t kCoroNr = 1;
                LOG_IF(INFO, is_master)
                    << "[bench] benching multiple threads for " << handle_conf;
                key++;
                // auto conf = BenchConfig::get_conf(
                //     "default", 10_K, producer_nr, consumer_nr, kCoroNr);
                auto conf = BenchConfig::get_conf(
                    "default", 10_K, producer_nr, consumer_nr, kCoroNr);
                if (is_client)
                {
                    conf.validate();
                    LOG_IF(INFO, is_master)
                        << "[sub-conf] running conf: " << conf;
                    benchmark_client(p, bar, is_master, conf, handle_conf, key);
                }
                else
                {
                    // TODO: let list config be real.
                    benchmark_server(
                        p, bar, is_master, {conf}, ListConfig{}, key);
                }
            }
        }
    }
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    PatronusConfig pconfig;
    pconfig.machine_nr = ::config::kMachineNr;
    pconfig.block_class = {32_MB, 2_MB, 4_KB};
    pconfig.block_ratio = {0.1, 0.5, 0.4};
    pconfig.reserved_buffer_size = 2_GB;
    pconfig.lease_buffer_size = (::config::kDefaultDSMSize - 2_GB) / 2;
    pconfig.alloc_buffer_size = (::config::kDefaultDSMSize - 2_GB) / 2;

    auto patronus = Patronus::ins(pconfig);

    std::vector<std::thread> threads;
    // boost::barrier bar(kThreadNr);
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

    LOG(INFO) << "[bench] " << pre_patronus_explain(*patronus);

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
        patronus->keeper_barrier("begin", 10ms);
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
        df.load_column<size_t>("x_producer_nr", std::move(col_x_producer_nr));
        df.load_column<size_t>("x_consumer_nr", std::move(col_x_consumer_nr));
        df.load_column<size_t>("test_nr(total)", std::move(col_test_nr));
        df.load_column<size_t>("test_ns(total)", std::move(col_ns));

        df.load_column<size_t>("rdma_prot_err_nr",
                               std::move(col_rdma_protection_err));
        df.load_column<double>("produce_succ_rate",
                               std::move(col_producer_succ_rate));
        df.load_column<double>("consume_succ_rate",
                               std::move(col_consumer_succ_rate));

        auto div_f = gen_F_div<size_t, size_t, double>();
        auto div_f2 = gen_F_div<double, size_t, double>();
        auto ops_f = gen_F_ops<size_t, size_t, double>();
        auto mul_f = gen_F_mul<double, size_t, double>();
        auto mul_f2 = gen_F_mul<size_t, size_t, size_t>();
        df.consolidate<size_t, size_t, size_t>(
            "x_thread_nr", "x_coro_nr", "client_nr(effective)", mul_f2, false);
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

        auto client_nr = ::config::get_client_nids().size();
        auto replace_mul_f = gen_replace_F_mul<double>(client_nr);
        df.load_column<double>("ops(cluster)",
                               df.get_column<double>("ops(total)"));
        df.replace<double>("ops(cluster)", replace_mul_f);

        df.consolidate<double, size_t, double>(
            "ops(total)", "x_thread_nr", "ops(thread)", div_f2, false);
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

    LOG(INFO) << "finished. ctrl+C to quit.";
}