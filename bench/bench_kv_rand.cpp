#include <algorithm>
#include <functional>
#include <iostream>
#include <queue>
#include <set>

#include "Common.h"
#include "PerThread.h"
#include "Rdma.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "util/BenchRand.h"
#include "util/DataFrameF.h"
#include "util/Rand.h"

using namespace define::literals;

std::vector<std::string> col_idx;
std::vector<size_t> col_x_thread_nr;
std::vector<size_t> col_rand_nr;
std::vector<size_t> col_rand_ns;

constexpr static size_t kMaxKey = 10_M;

using namespace hmdf;

DEFINE_string(exec_meta, "", "The meta data of this execution");

using RandFunc = std::function<uint64_t()>;

void bench_thread(size_t tid,
                  size_t test_times,
                  std::atomic<ssize_t> &work_nr,
                  IKVRandGenerator *g)
{
    bindCore(tid + 1);

    ssize_t task_per_sync = test_times / 32;
    ssize_t remain = work_nr.load(std::memory_order_relaxed);

    volatile uint64_t key = 0;
    while (remain > 0)
    {
        size_t task_nr = std::min(remain, (ssize_t) task_per_sync);
        for (size_t i = 0; i < task_nr; ++i)
        {
            g->gen_key((char *) &key, 8);
            // g->gen_value(value, 64 - sizeof(uint64_t));
        }
        remain = work_nr.fetch_sub(task_nr) - task_nr;
    }
}

void bench_template(const std::string &bench_name,
                    size_t test_times,
                    size_t thread_nr,
                    std::vector<IKVRandGenerator::pointer> gs,
                    bool report)
{
    std::vector<std::thread> threads;

    std::atomic<ssize_t> work_nr{ssize_t(test_times)};

    ChronoTimer timer;
    for (size_t i = 0; i < thread_nr; ++i)
    {
        threads.emplace_back([i, &work_nr, test_times, g = gs[i].get()]() {
            bench_thread(i, test_times, work_nr, g);
        });
    }
    for (auto &t : threads)
    {
        t.join();
    }
    auto total_ns = timer.pin();

    if (report)
    {
        col_idx.push_back(bench_name);
        col_x_thread_nr.push_back(thread_nr);
        col_rand_nr.push_back(test_times);
        col_rand_ns.push_back(total_ns);
    }
}

void bench_uniform(size_t test_times, size_t thread_nr, bool report)
{
    std::vector<IKVRandGenerator::pointer> gs;
    for (size_t i = 0; i < thread_nr; ++i)
    {
        auto g = UniformRandGenerator::new_instance(0, kMaxKey);
        gs.emplace_back(std::move(g));
    }
    return bench_template(
        "uniform_generator", test_times, thread_nr, gs, report);
}
void bench_memcached_zip(size_t test_times, size_t thread_nr, bool report)
{
    std::vector<IKVRandGenerator::pointer> gs;
    for (size_t i = 0; i < thread_nr; ++i)
    {
        auto g = MehcachedZipfianRandGenerator::new_instance(0, kMaxKey);
        gs.emplace_back(std::move(g));
    }
    return bench_template(
        "memcached_zipfian_generator", test_times, thread_nr, gs, report);
}
void bench_nothing(size_t test_times, size_t thread_nr, bool report)
{
    std::vector<IKVRandGenerator::pointer> gs;
    for (size_t i = 0; i < thread_nr; ++i)
    {
        auto g = NothingRandGenerator::new_instance();
        gs.emplace_back(std::move(g));
    }
    return bench_template(
        "nothing_generator", test_times, thread_nr, gs, report);
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    for (size_t thread_nr : {1, 2, 4, 8, 16})
    // for (size_t thread_nr : {8})
    {
        LOG(INFO) << "Benching thread " << thread_nr;
        bench_uniform(40_M * thread_nr, thread_nr, true);
        bench_memcached_zip(40_M * thread_nr, thread_nr, true);
        bench_nothing(40_M * thread_nr, thread_nr, true);
    }

    StrDataFrame df;
    df.load_index(std::move(col_idx));
    df.load_column<size_t>("x_thread_nr", std::move(col_x_thread_nr));
    df.load_column<size_t>("rand_nr(total)", std::move(col_rand_nr));
    df.load_column<size_t>("rand_ns(total)", std::move(col_rand_ns));

    auto div_f = gen_F_div<size_t, size_t, double>();
    auto div_f2 = gen_F_div<double, size_t, double>();
    auto ops_f = gen_F_ops<size_t, size_t, double>();
    auto mul_f = gen_F_mul<double, size_t, double>();
    auto mul_f2 = gen_F_mul<size_t, size_t, size_t>();
    df.consolidate<size_t, size_t, size_t>(
        "rand_ns(total)", "x_thread_nr", "rand_ns(effective)", mul_f2, false);
    df.consolidate<size_t, size_t, double>(
        "rand_ns(total)", "rand_nr(total)", "rand lat", div_f, false);
    df.consolidate<size_t, size_t, double>(
        "rand_ns(effective)", "rand_nr(total)", "rand lat(avg)", div_f, false);
    df.consolidate<size_t, size_t, double>(
        "rand_nr(total)", "rand_ns(total)", "rand ops(total)", ops_f, false);
    df.consolidate<double, size_t, double>(
        "rand ops(total)", "x_thread_nr", "rand ops(thread)", div_f2, false);

    auto filename = binary_to_csv_filename(argv[0], FLAGS_exec_meta);
    df.write<std::ostream, std::string, size_t, double>(std::cout,
                                                        io_format::csv2);
    df.write<std::string, size_t, double>(filename.c_str(), io_format::csv2);
}
