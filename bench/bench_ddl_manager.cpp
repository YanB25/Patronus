#include <algorithm>
#include <random>

#include "DSM.h"
#include "Timer.h"
#include "gflags/gflags.h"
#include "patronus/Patronus.h"
#include "util/DataFrameF.h"
#include "util/Rand.h"
#include "util/TimeConv.h"
#include "util/monitor.h"

using namespace patronus;

using namespace hmdf;

DEFINE_string(exec_meta, "", "The meta data of this execution");

std::vector<std::string> col_idx;
std::vector<size_t> col_x_test_time;
std::vector<size_t> col_x_active_size;
std::vector<uint64_t> col_ns;
// std::vector<uint64_t> col_lat_min;
// std::vector<uint64_t> col_lat_p5;
// std::vector<uint64_t> col_lat_p9;
// std::vector<uint64_t> col_lat_p99;

constexpr static size_t kTestNr = 10_M;

template <size_t kSize>
struct DumpData
{
    mutable volatile char buffer[kSize];
    void f() const
    {
        buffer[0] = 1;
        buffer[kSize - 1] = 1;
    }
};

void do_benchmark(Patronus::pointer p,
                  size_t active_size,
                  std::chrono::nanoseconds later_time,
                  size_t test_nr)
{
    LOG(INFO) << "[bench] active_size: " << active_size
              << ", later_time: " << util::time::to_ns(later_time)
              << ", test_nr: " << test_nr;
    DDLManager ddl_manager;
    DumpData<40> data;
    // load
    auto task = [data](CoroContext *) {
        // do nothing
        data.f();
    };
    // LOG(INFO) << "[debug] task size is " << sizeof(task);

    auto &time_syncer = p->time_syncer();

    auto max = std::numeric_limits<patronus::time::term_t>::max();

    // load
    for (size_t i = 0; i < active_size; ++i)
    {
        auto key =
            time_syncer.patronus_later(util::time::to_ns(later_time)).term();
        ddl_manager.push(key, task);
    }

    // auto lat_min = util::time::to_ns(0ns);
    // auto lat_max = util::time::to_ns(10us);
    // auto lat_rng = util::time::to_ns(10ns);
    // OnePassBucketMonitor pop_lat_m(lat_min, lat_max, lat_rng);
    // OnePassBucketMonitor push_lat_m(lat_min, lat_max, lat_rng);
    // bench
    ChronoTimer timer;
    for (size_t i = 0; i < test_nr; ++i)
    {
        // do one and push one
        auto key =
            time_syncer.patronus_later(util::time::to_ns(later_time)).term();
        // ddl_manager.push(key, task);
        ddl_manager.push(key, task);

        CHECK_EQ(ddl_manager.do_task(max, nullptr, 1), 1)
            << "got no task at " << i << "-test. detail: " << ddl_manager
            << ". max is : " << max;
    }
    auto ns = timer.pin();

    col_idx.push_back("ddl");
    col_x_test_time.push_back(test_nr);
    col_x_active_size.push_back(active_size);
    col_ns.push_back(ns);
}

void benchmark(Patronus::pointer patronus)
{
    for (size_t active_size : {0_B, 1_K, 10_K, 100_K, 1_M, 10_M, 100_M})
    {
        for (auto later_time : {1ns, 10ns, 100ns})
        {
            do_benchmark(patronus, active_size, later_time, kTestNr);
        }
    }
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    PatronusConfig config;
    config.machine_nr = ::config::kMachineNr;
    config.block_class = {4_KB};
    config.block_ratio = {1};
    config.alloc_buffer_size = 1_MB;
    config.lease_buffer_size = 1_MB;
    config.reserved_buffer_size = 1_MB;

    auto patronus = Patronus::ins(config);

    benchmark(patronus);

    StrDataFrame df;
    df.load_index(std::move(col_idx));
    df.load_column<size_t>("x_test_nr", std::move(col_x_test_time));
    df.load_column<size_t>("x_active_nr", std::move(col_x_active_size));
    df.load_column<size_t>("test_ns", std::move(col_ns));

    auto ops_f = gen_F_ops<size_t, size_t, double>();
    auto div_f = gen_F_div<size_t, size_t, double>();
    df.consolidate<size_t, size_t, double>(
        "x_test_nr", "test_ns", "ops(total)", ops_f, false);
    df.consolidate<size_t, size_t, double>(
        "test_ns", "x_test_nr", "lat_ns(avg)", div_f, false);
    // df.load_column<uint64_t>("lat_min(ns)", std::move(col_lat_min));
    // df.load_column<uint64_t>("lat_p5(ns)", std::move(col_lat_p5));
    // df.load_column<uint64_t>("lat_p9(ns)", std::move(col_lat_p9));
    // df.load_column<uint64_t>("lat_p99(ns)", std::move(col_lat_p99));

    auto filename = binary_to_csv_filename(argv[0], FLAGS_exec_meta);
    df.write<std::ostream, std::string, size_t, double>(std::cout,
                                                        io_format::csv2);
    df.write<std::string, size_t, double>(filename.c_str(), io_format::csv2);

    LOG(INFO) << "finished. ctrl+C to quit.";
}