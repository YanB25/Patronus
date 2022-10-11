#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <queue>
#include <set>

#include "Common.h"
#include "Magick++.h"
#include "PerThread.h"
#include "Rdma.h"
#include "boost/thread/barrier.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "patronus/All.h"
#include "thirdparty/serverless/serverless.h"
#include "util/DataFrameF.h"
#include "util/Hexdump.hpp"
#include "util/IRdmaAdaptor.h"
#include "util/PerformanceReporter.h"
#include "util/Rand.h"
#include "util/TimeConv.h"
#include "util/Util.h"

using namespace util::literals;
using namespace patronus;
using namespace std::chrono_literals;
using namespace hmdf;
using namespace util::pre;

constexpr static size_t kClientThreadNr = kMaxAppThread;
constexpr static size_t kServerThreadNr = NR_DIRECTORY;
constexpr static size_t kTestTimePerThread = 20_K;

[[maybe_unused]] constexpr static size_t V1 =
    ::config::verbose::kBenchReserve_1;
[[maybe_unused]] constexpr static size_t V2 =
    ::config::verbose::kBenchReserve_2;

std::vector<std::string> col_idx;
std::vector<size_t> col_x_thread_nr;
std::vector<size_t> col_x_coro_nr;
std::vector<size_t> col_test_op_nr;
std::vector<size_t> col_ns;

std::vector<std::string> col_lat_idx;
std::map<std::string, std::vector<uint64_t>> lat_data;

DEFINE_string(exec_meta, "", "The meta data of this execution");

[[maybe_unused]] constexpr static size_t kCoroCnt = 32;  // max

struct BenchConfig
{
    size_t thread_nr;
    size_t coro_nr;
    size_t task_nr;
    std::string name;
    bool report;

    bool should_report_lat() const
    {
        return thread_nr == kMaxAppThread && coro_nr == 1;
    }

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
    static std::vector<BenchConfig> get_basic(const std::string &name,
                                              size_t thread_nr,
                                              size_t coro_nr,
                                              size_t task_nr)
    {
        auto conf =
            BenchConfig::get_empty_conf(name, thread_nr, coro_nr, task_nr);
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

struct BenchResult
{
    double lat_min;
    double lat_p5;
    double lat_p9;
    double lat_p99;
};

using Parameters = serverless::Parameters;
using ParameterMap = serverless::Parameters::ParameterMap;
using lambda_t = serverless::CoroLauncher::lambda_t;

struct DataAnalyseMeta
{
    size_t item_nr;
    double salary_average;
};
struct Person
{
    char name[32];
    uint32_t birthdate;
    uint32_t salary;
    uint8_t gender;
};
constexpr static size_t kPersonNr = 512;
inline std::ostream &operator<<(std::ostream &os, const DataAnalyseMeta &meta)
{
    os << "{DataAnalyseMeta item_nr: " << meta.item_nr
       << ", salary_avg: " << meta.salary_average << "}";
    return os;
}

void init_allocator(Patronus::pointer p,
                    size_t dir_id,
                    size_t thread_nr,
                    size_t expect_size)
{
    auto rh_buffer = p->get_user_reserved_buffer();
    auto thread_pool_size = rh_buffer.size / thread_nr;
    void *thread_pool_addr = rh_buffer.buffer + dir_id * thread_pool_size;

    mem::SlabAllocatorConfig slab_config;
    slab_config.block_class = {expect_size};
    slab_config.block_ratio = {1.0};
    slab_config.enable_recycle = true;
    auto allocator = mem::SlabAllocator::new_instance(
        thread_pool_addr, thread_pool_size, slab_config);
    p->reg_allocator(config::serverless::kAllocHint, allocator);
}

void register_lambdas(serverless::CoroLauncher &launcher,
                      size_t coro_nr,
                      GlobalAddress person_db_gaddr,
                      GlobalAddress meta_gaddr)
{
    auto check = [](Parameters &parameter,
                    CoroContext *ctx,
                    util::TraceView trace) -> RetCode {
        auto [meta_buf, meta_size] = parameter.get_param("meta", ctx, trace);
        DCHECK_EQ(meta_size, sizeof(DataAnalyseMeta));
        const auto &meta = *(DataAnalyseMeta *) meta_buf.buffer;
        if (unlikely(meta.item_nr < 0))
        {
            LOG(WARNING) << "** Check failed. meta: " << meta;
        }
        if (unlikely(meta.item_nr >= 10 * kPersonNr))
        {
            LOG(WARNING) << "** Check failed. meta: " << meta;
        }

        auto update_meta_buf = parameter.get_buffer(sizeof(DataAnalyseMeta));
        memcpy(
            update_meta_buf.buffer, meta_buf.buffer, sizeof(DataAnalyseMeta));
        auto &update_meta = *(DataAnalyseMeta *) update_meta_buf.buffer;
        update_meta.item_nr = update_meta.item_nr;
        parameter.set_param("meta",
                            std::move(update_meta_buf),
                            sizeof(DataAnalyseMeta),
                            ctx,
                            trace);
        return RC::kOk;
    };
    auto alloc_record_to_db = [](Parameters &parameter,
                                 CoroContext *ctx,
                                 util::TraceView trace) -> RetCode {
        parameter.alloc("insert record", sizeof(Person), ctx, trace);
        return RC::kOk;
    };
    auto write_to_alloc_record = [](Parameters &parameter,
                                    CoroContext *ctx,
                                    util::TraceView trace) -> RetCode {
        auto person_buf = parameter.get_buffer(sizeof(Person));
        auto &person = *(Person *) person_buf.buffer;
        person.birthdate = 20220710;
        person.gender = 1;
        const char *name = "Alan Mathison Turing";
        auto length = sizeof(name);
        memcpy(person.name, name, length);
        person.salary = 300000;
        parameter
            .write("insert record",
                   std::move(person_buf),
                   sizeof(Person),
                   ctx,
                   trace)
            .expect(RC::kOk);
        return RC::kOk;
    };
    auto data_analyse = [](Parameters &parameter,
                           CoroContext *ctx,
                           util::TraceView trace) -> RetCode {
        auto [meta_buf, meta_size] = parameter.get_param("meta", ctx, trace);
        DCHECK_EQ(meta_size, sizeof(DataAnalyseMeta));
        const auto &meta = *(DataAnalyseMeta *) meta_buf.buffer;
        auto person_nr = meta.item_nr;

        auto [db_buf, db_size] = parameter.read("db", ctx, trace);
        const auto *person_db = (const Person *) db_buf.buffer;
        DCHECK_EQ(db_size, person_nr * sizeof(Person));
        double sum_salary = 0;
        for (size_t i = 0; i < person_nr; ++i)
        {
            const auto &person_ith = person_db[i];
            sum_salary += person_ith.salary;
        }
        sum_salary /= person_nr;

        auto update_meta_buf = parameter.get_buffer(sizeof(DataAnalyseMeta));
        memcpy(
            update_meta_buf.buffer, meta_buf.buffer, sizeof(DataAnalyseMeta));
        auto &update_meta = *(DataAnalyseMeta *) update_meta_buf.buffer;
        update_meta.salary_average = sum_salary;
        parameter.set_param("meta",
                            std::move(update_meta_buf),
                            sizeof(DataAnalyseMeta),
                            ctx,
                            trace);

        parameter.put_rdma_buffer(std::move(db_buf));
        return RC::kOk;
    };
    auto response = [](Parameters &parameter,
                       CoroContext *ctx,
                       util::TraceView trace) -> RetCode {
        auto [meta_buf, meta_size] = parameter.get_param("meta", ctx, trace);
        auto response_buffer = parameter.get_buffer(sizeof(DataAnalyseMeta));
        memcpy(
            response_buffer.buffer, meta_buf.buffer, sizeof(DataAnalyseMeta));
        parameter.set_param("response",
                            std::move(response_buffer),
                            sizeof(DataAnalyseMeta),
                            ctx,
                            trace);
        return RC::kOk;
    };

    ParameterMap init_param;
    init_param["meta"].gaddr = meta_gaddr;
    init_param["meta"].size = sizeof(DataAnalyseMeta);
    init_param["db"].gaddr = person_db_gaddr;
    init_param["db"].size = sizeof(Person) * kPersonNr;

    size_t lambda_nr = 0;
    constexpr static size_t kLambdaNrPerChain = 5;
    while (lambda_nr + kLambdaNrPerChain <= coro_nr)
    {
        auto check_id = launcher.add_lambda(check,
                                            init_param,
                                            {} /* recv from */,
                                            {} /* depend on */,
                                            {} /* reloop to */);
        auto alloc_record_id = launcher.add_lambda(alloc_record_to_db,
                                                   {} /* init param */,
                                                   check_id,
                                                   {check_id},
                                                   {} /* reloop to */);
        auto write_to_alloc_record_id =
            launcher.add_lambda(write_to_alloc_record,
                                {} /* init param */,
                                alloc_record_id,
                                {alloc_record_id},
                                {} /* reloop to */);
        auto data_analyse_id = launcher.add_lambda(data_analyse,
                                                   {} /* init param */,
                                                   write_to_alloc_record_id,
                                                   {write_to_alloc_record_id},
                                                   {} /* reloop to */);
        [[maybe_unused]] auto response_id =
            launcher.add_lambda(response,
                                {} /* init param */,
                                data_analyse_id,
                                {data_analyse_id},
                                check_id);
        lambda_nr += kLambdaNrPerChain;
    }
}

void reg_result(const std::string &name,
                const BenchConfig &bench_conf,
                uint64_t ns)
{
    col_idx.push_back(name);
    col_x_thread_nr.push_back(bench_conf.thread_nr);
    col_x_coro_nr.push_back(bench_conf.coro_nr);
    col_test_op_nr.push_back(bench_conf.task_nr);
    col_ns.push_back(ns);
}

void reg_lat_result(const std::string &name,
                    const OnePassBucketMonitor<uint64_t> &lat_m,
                    [[maybe_unused]] const BenchConfig &bench_conf)
{
    if (col_lat_idx.empty())
    {
        col_lat_idx.push_back("lat_min(avg)");
        col_lat_idx.push_back("lat_p5(avg)");
        col_lat_idx.push_back("lat_p9(avg)");
        col_lat_idx.push_back("lat_p99(avg)");
    }
    if (likely(lat_data[name].empty()))
    {
        lat_data[name].push_back(lat_m.min());
        lat_data[name].push_back(lat_m.percentile(0.5));
        lat_data[name].push_back(lat_m.percentile(0.9));
        lat_data[name].push_back(lat_m.percentile(0.99));
    }
    else
    {
        LOG(WARNING) << "** Recording latency for `" << name
                     << "` duplicated. Skip.";
    }
}

void bench_alloc_thread_coro(
    Patronus::pointer patronus,
    boost::barrier &bar,
    GlobalAddress person_db_gaddr,
    GlobalAddress meta_gaddr,
    [[maybe_unused]] OnePassBucketMonitor<uint64_t> &lat_m,
    [[maybe_unused]] bool is_master,
    std::atomic<ssize_t> &work_nr,
    const serverless::Config &serverless_config,
    const BenchConfig &conf)
{
    auto test_times = conf.task_nr;

    auto server_nid = ::config::get_server_nids().front();
    auto tid = patronus->get_thread_id();
    auto dir_id = tid % kServerThreadNr;
    size_t thread_nr = conf.thread_nr;

    std::unique_ptr<serverless::CoroLauncher> launcher;
    if (tid < thread_nr)
    {
        launcher = std::make_unique<serverless::CoroLauncher>(patronus,
                                                              server_nid,
                                                              dir_id,
                                                              serverless_config,
                                                              test_times,
                                                              work_nr,
                                                              0);
        // 20 * 1.0 / kTestTimePerThread);
        register_lambdas(*launcher, conf.coro_nr, person_db_gaddr, meta_gaddr);
    }

    bar.wait();
    ChronoTimer timer;
    if (launcher)
    {
        launcher->launch();
    }
    bar.wait();
    auto ns = timer.pin();

    if (is_master)
    {
        auto report_name = conf.name + "-" + serverless_config.name;
        reg_result(report_name, conf, ns);
        if (conf.should_report_lat())
        {
            reg_lat_result(report_name, lat_m, conf);
        }
    }

    return;
}

void bench_template(const std::string &name,
                    Patronus::pointer patronus,
                    GlobalAddress db_gaddr,
                    GlobalAddress meta_gaddr,
                    boost::barrier &bar,
                    std::atomic<ssize_t> &work_nr,
                    bool is_master,
                    const serverless::Config &serverless_config,
                    const BenchConfig &conf)

{
    auto test_times = conf.task_nr;
    auto thread_nr = conf.thread_nr;
    auto coro_nr = conf.coro_nr;
    auto report = conf.report;

    if (is_master)
    {
        work_nr = test_times;
        LOG(INFO) << "[bench] BENCH: " << name << ", thread_nr: " << thread_nr
                  << ", coro_nr: " << coro_nr << ", report: " << report;
    }

    bar.wait();
    ChronoTimer timer;

    // auto tid = patronus->get_thread_id();

    auto min = util::time::to_ns(0ns);
    auto max = util::time::to_ns(10ms);
    auto range = util::time::to_ns(1us);
    OnePassBucketMonitor lat_m(min, max, range);

    bench_alloc_thread_coro(patronus,
                            bar,
                            db_gaddr,
                            meta_gaddr,
                            lat_m,
                            is_master,
                            work_nr,
                            serverless_config,
                            conf);

    bar.wait();
}

void init_meta(Patronus::pointer patronus)
{
    // meta
    auto meta_addr =
        patronus->patronus_alloc(sizeof(DataAnalyseMeta), 0 /* hint */);
    auto &meta = *(DataAnalyseMeta *) meta_addr;
    meta.item_nr = kPersonNr;
    meta.salary_average = 0;
    auto meta_gaddr = patronus->to_exposed_gaddr(meta_addr);
    patronus->put("serverless:meta_gaddr", meta_gaddr, 0ns);

    // db
    auto db_addr =
        patronus->patronus_alloc(sizeof(Person) * kPersonNr, 0 /* hint */);
    Person *persons = (Person *) db_addr;
    for (size_t i = 0; i < kPersonNr; ++i)
    {
        persons[i].birthdate = fast_pseudo_rand_int(0, 1000);
        persons[i].gender = fast_pseudo_bool_with_prob(0.5);
        fast_pseudo_fill_buf(persons[i].name, 8);
        persons[i].salary = fast_pseudo_rand_dbl(0, 1000);
    }
    auto db_gaddr = patronus->to_exposed_gaddr(db_addr);
    patronus->put("serverless:db_gaddr", db_gaddr, 0ns);
}
void run_benchmark_server(Patronus::pointer patronus,
                          const BenchConfig &conf,
                          bool is_master,
                          boost::barrier &bar,
                          uint64_t key)
{
    auto tid = patronus->get_thread_id();
    auto dir_id = tid;
    std::ignore = conf;

    init_meta(patronus);

    // Make sure kExpectCompressedImgSize large enough
    init_allocator(patronus, dir_id, kServerThreadNr, sizeof(Person));

    if (is_master)
    {
        patronus->finished(key);
    }

    bar.wait();
    if (is_master)
    {
        patronus->keeper_barrier("server_ready-" + std::to_string(key), 100ms);
    }

    LOG(INFO) << "[coro] server thread tid " << tid;

    LOG(INFO) << "[bench] BENCH: " << conf.name
              << ", thread_nr: " << conf.thread_nr
              << ", coro_nr: " << conf.coro_nr << ", report: " << conf.report;

    patronus->server_serve(key);
}
std::atomic<ssize_t> shared_task_nr;

GlobalAddress g_meta_gaddr;
GlobalAddress g_db_gaddr;
void run_benchmark_client(Patronus::pointer patronus,
                          const serverless::Config &serverless_config,
                          const BenchConfig &conf,
                          boost::barrier &bar,
                          bool is_master,
                          uint64_t &key)
{
    if (is_master)
    {
        shared_task_nr = conf.task_nr;
        g_meta_gaddr =
            patronus->get_object<GlobalAddress>("serverless:meta_gaddr", 100ms);
        g_db_gaddr =
            patronus->get_object<GlobalAddress>("serverless:db_gaddr", 100ms);
        LOG(INFO) << "Getting serverless:meta_gaddr " << g_meta_gaddr
                  << ", serverless:db_gaddr: " << g_db_gaddr;

        patronus->keeper_barrier("server_ready-" + std::to_string(key), 100ms);
    }

    bar.wait();

    bench_template(conf.name,
                   patronus,
                   g_db_gaddr,
                   g_meta_gaddr,
                   bar,
                   shared_task_nr,
                   is_master,
                   serverless_config,
                   conf);
    bar.wait();
    if (is_master)
    {
        patronus->finished(key);
    }
}

void run_benchmark(Patronus::pointer patronus,
                   const serverless::Config &serverless_config,
                   const std::vector<BenchConfig> &configs,
                   boost::barrier &bar,
                   bool is_client,
                   bool is_master,
                   uint64_t &key)
{
    for (const auto &conf : configs)
    {
        key++;

        if (is_client)
        {
            run_benchmark_client(
                patronus, serverless_config, conf, bar, is_master, key);
        }
        else
        {
            run_benchmark_server(patronus, conf, is_master, bar, key);
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

    std::vector<serverless::Config> serverless_configs;
    serverless_configs.emplace_back(
        serverless::Config::get_unprot("unprot[step]", true));
    serverless_configs.emplace_back(
        serverless::Config::get_unprot("unprot[nested]", false));
    serverless_configs.emplace_back(
        serverless::Config::get_mw("mw[step]", true));
    serverless_configs.emplace_back(
        serverless::Config::get_mw("mw[nested]", false));
    serverless_configs.emplace_back(
        serverless::Config::get_mr("mr[step]", true));
    serverless_configs.emplace_back(
        serverless::Config::get_mr("mr[nested]", false));
    serverless_configs.emplace_back(
        serverless::Config::get_rpc("rpc[step]", true));
    serverless_configs.emplace_back(
        serverless::Config::get_rpc("rpc[nested]", false));

    for (size_t thread_nr : {1, 2, 4, 8, 16, 32})
    // for (size_t thread_nr : {32})
    {
        CHECK_LE(thread_nr, kMaxAppThread);
        // for (size_t coro_nr : {2, 4, 8, 16, 32})
        // for (size_t coro_nr : {1, 32})
        for (size_t coro_nr : {32})
        {
            auto total_test_times =
                kTestTimePerThread * std::min(size_t(4), thread_nr);
            for (const auto &serverless_config : serverless_configs)
            {
                auto configs = BenchConfigFactory::get_basic(
                    "img",
                    thread_nr,
                    coro_nr,
                    total_test_times * serverless_config.scale_factor);
                LOG_IF(INFO, is_master) << "[config] " << serverless_config;
                run_benchmark(patronus,
                              serverless_config,
                              configs,
                              bar,
                              is_client,
                              is_master,
                              key);
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
    config.reserved_buffer_size = 1_GB;
    config.lease_buffer_size = (::config::kDefaultDSMSize - 1_GB) / 2;
    config.alloc_buffer_size = (::config::kDefaultDSMSize - 1_GB) / 2;
    size_t db_expect_size = sizeof(Person) * kPersonNr;
    config.client_rdma_buffer.block_class = {db_expect_size, 4_KB, 8_B};
    config.client_rdma_buffer.block_ratio = {0.1, 1 - 0.1 - 0.001, 0.001};

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

    {
        StrDataFrame df;
        df.load_index(std::move(col_idx));
        df.load_column<size_t>("x_thread_nr", std::move(col_x_thread_nr));
        df.load_column<size_t>("x_coro_nr", std::move(col_x_coro_nr));
        df.load_column<size_t>("test_nr(total)", std::move(col_test_op_nr));
        df.load_column<size_t>("test_ns(total)", std::move(col_ns));

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
    LOG(INFO) << "Finished.";
}
