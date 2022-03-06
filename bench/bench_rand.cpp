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
#include "util/DataFrameF.h"
#include "util/Rand.h"

using namespace define::literals;

std::vector<std::string> col_idx;
std::vector<size_t> col_x_thread_nr;
std::vector<size_t> col_rand_nr;
std::vector<size_t> col_rand_ns;

using namespace hmdf;

DEFINE_string(exec_meta, "", "The meta data of this execution");

using RandFunc = std::function<uint64_t()>;

void bench_alloc_thread(size_t tid,
                        size_t total_task_nr,
                        std::atomic<ssize_t> &work_nr,
                        RandFunc f)
{
    bindCore(tid + 1);

    ssize_t remain = total_task_nr;

    uint64_t magic = 0;

    while (remain > 0)
    {
        size_t task_nr = std::min(remain, (ssize_t) 50);
        for (size_t i = 0; i < task_nr; ++i)
        {
            magic += f();
        }
        remain = work_nr.fetch_sub(task_nr) - task_nr;
    }
}

void bench_template(const std::string &bench_name,
                    size_t test_times,
                    size_t thread_nr,
                    RandFunc f,
                    bool report)
{
    std::vector<std::thread> threads;

    std::atomic<ssize_t> work_nr{ssize_t(test_times)};

    ChronoTimer timer;
    for (size_t i = 0; i < thread_nr; ++i)
    {
        threads.emplace_back([i, &work_nr, test_times, f]() {
            bench_alloc_thread(i, test_times, work_nr, f);
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

uint64_t __fast_pseudo_rand_int()
{
    return fast_pseudo_rand_int();
}
uint64_t __accurate_pseudo_rand_int()
{
    return accurate_pseudo_rand_int();
}
uint64_t __non_deterministic_pseudo_rand_int()
{
    return non_deterministic_rand_int();
}

void bench_rand_fast(size_t test_times, size_t thread_nr, bool report)
{
    return bench_template(
        "rand (fast)", test_times, thread_nr, __fast_pseudo_rand_int, report);
}
void bench_rand_accurate(size_t test_times, size_t thread_nr, bool report)
{
    return bench_template("rand (accurate)",
                          test_times,
                          thread_nr,
                          __accurate_pseudo_rand_int,
                          report);
}
void bench_rand_non_deterministic(size_t test_times,
                                  size_t thread_nr,
                                  bool report)
{
    return bench_template("rand (non-deterministic)",
                          test_times,
                          thread_nr,
                          __non_deterministic_pseudo_rand_int,
                          report);
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    for (size_t thread_nr : {1, 2, 4, 8, 16})
    // for (size_t thread_nr : {8})
    {
        // warm up
        for (size_t i = 0; i < 2; ++i)
        {
            bench_rand_accurate(4_M * thread_nr, thread_nr, false);
            bench_rand_fast(4_M * thread_nr, thread_nr, false);
            bench_rand_non_deterministic(4_M * thread_nr, thread_nr, false);
        }

        bench_rand_accurate(4_M * thread_nr, thread_nr, true);
        bench_rand_fast(4_M * thread_nr, thread_nr, true);
        bench_rand_non_deterministic(4_M * thread_nr, thread_nr, true);
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
    df.consolidate<size_t, size_t, double>(
        "rand_ns(total)", "rand_nr(total)", "rand lat", div_f, false);
    df.consolidate<size_t, size_t, double>(
        "rand_nr(total)", "rand_ns(total)", "rand ops(total)", ops_f, false);
    df.consolidate<double, size_t, double>(
        "rand ops(total)", "x_thread_nr", "rand ops(thread)", div_f2, false);

    auto filename = binary_to_csv_filename(argv[0], FLAGS_exec_meta);
    df.write<std::ostream, std::string, size_t, double>(std::cout,
                                                        io_format::csv2);
    df.write<std::string, size_t, double>(filename.c_str(), io_format::csv2);
}
