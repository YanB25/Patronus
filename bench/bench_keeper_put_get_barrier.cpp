#include <algorithm>
#include <chrono>
#include <random>
#include <thread>

#include "Timer.h"
#include "patronus/Patronus.h"
#include "util/DataFrameF.h"
#include "util/monitor.h"

using namespace std::chrono_literals;

DEFINE_string(exec_meta, "", "The meta data of this execution");

using namespace hmdf;
std::vector<std::string> col_idx;
std::vector<size_t> col_x_task_nr;
std::vector<size_t> col_x_ns;

constexpr uint16_t kClientNodeId = 1;
[[maybe_unused]] constexpr uint16_t kServerNodeId = 0;
constexpr uint32_t kMachineNr = 2;

using namespace patronus;

using namespace std::chrono_literals;

constexpr static uint64_t kWaitKey = 0;

void bench_barrier(Patronus::pointer p)
{
    ChronoTimer timer;
    constexpr size_t kTestNr = 1_K;
    for (size_t i = 0; i < kTestNr; ++i)
    {
        p->keeper_barrier(std::to_string(i), 0ns);
    }
    auto ns = timer.pin();
    col_idx.push_back("barrier");
    col_x_task_nr.push_back(kTestNr);
    col_x_ns.push_back(ns);
}
void bench_bulk_put(Patronus::pointer p)
{
    ChronoTimer timer;
    constexpr size_t kTestNr = 1_K;
    for (size_t i = 0; i < kTestNr; ++i)
    {
        p->put(std::to_string(i), std::to_string(i), 0ns);
    }
    auto ns = timer.pin();
    col_idx.push_back("bulk_put_numbers");
    col_x_task_nr.push_back(kTestNr);
    col_x_ns.push_back(ns);
}
void bench_bulk_get(Patronus::pointer p)
{
    ChronoTimer timer;
    constexpr size_t kTestNr = 1_K;
    for (size_t i = 0; i < kTestNr; ++i)
    {
        p->get(std::to_string(i), 0ns);
    }
    auto ns = timer.pin();
    col_idx.push_back("bulk_get_numbers");
    col_x_task_nr.push_back(kTestNr);
    col_x_ns.push_back(ns);
}

void client(Patronus::pointer p)
{
    bench_bulk_put(p);
    bench_bulk_get(p);
    p->keeper_barrier("begin-barrier-test", 1s);
    bench_barrier(p);
}

void server(Patronus::pointer p)
{
    p->keeper_barrier("begin-barrier-test", 1s);
    bench_barrier(p);
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    bindCore(0);

    rdmaQueryDevice();

    PatronusConfig config;
    config.machine_nr = kMachineNr;

    auto patronus = Patronus::ins(config);

    auto nid = patronus->get_node_id();
    if (nid == kClientNodeId)
    {
        patronus->registerClientThread();
        client(patronus);
        patronus->finished(kWaitKey);
    }
    else
    {
        patronus->registerServerThread();
        patronus->finished(kWaitKey);
        server(patronus);
    }

    StrDataFrame df;
    df.load_index(std::move(col_idx));
    df.load_column<size_t>("task_nr", std::move(col_x_task_nr));
    df.load_column<size_t>("ns", std::move(col_x_ns));

    auto ops_f = gen_F_ops<size_t, size_t, double>();
    auto div_f = gen_F_div<size_t, size_t, double>();

    df.consolidate<size_t, size_t, double>(
        "task_nr", "ns", "ops", ops_f, false);
    df.consolidate<size_t, size_t, double>(
        "ns", "task_nr", "ns (average)", div_f, false);

    auto filename = binary_to_csv_filename(argv[0], FLAGS_exec_meta);
    df.write<std::ostream, std::string, size_t, double>(std::cout,
                                                        io_format::csv2);
    df.write<std::string, size_t, double>(filename.c_str(), io_format::csv2);

    LOG(INFO) << "finished. ctrl+C to quit.";
}