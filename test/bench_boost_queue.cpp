#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <functional>
#include <random>
#include <thread>

#include "Common.h"
#include "boost/lockfree/queue.hpp"
#include "util/PerformanceReporter.h"

using namespace define::literals;
using namespace std::chrono_literals;

constexpr static size_t kTestSize = 20_M;

using Task = void (*)();
void do_nothing()
{
}

void bench_boost_queue(size_t producer_nr, size_t consumer_nr)
{
    // boost::lockfree::queue<Task> queue;

    // std::vector<std::thread> producer;
    // std::vector<std::thread> consumer;

    // std::atomic<size_t> producer_finish_nr{0};
    // std::atomic<bool> all_finished{false};
    // std::atomic<size_t> producer_thread_finish_nr{0};
    // for (size_t i = 0; i < producer_nr; ++i)
    // {
    //     producer.emplace_back([&queue,
    //                            &producer_finish_nr,
    //                            &producer_thread_finish_nr,
    //                            &all_finished,
    //                            producer_nr]() {
    //         size_t remain_task = kTestSize;
    //         while (remain_task > 0)
    //         {
    //             size_t task = std::min(remain_task, size_t(100));
    //             for (size_t i = 0; i < task; ++i)
    //             {
    //                 queue.bounded_push([i]() { LOG(INFO) << "hello world";
    //                 });
    //             }
    //             producer_finish_nr.fetch_add(task,
    //             std::memory_order_relaxed);
    //         }
    //         auto old = producer_thread_finish_nr.fetch_add(1) + 1;
    //         if (old == producer_nr)
    //         {
    //             all_finished = true;
    //         }
    //     });
    // }
    // std::atomic<size_t> consumer_finish_nr{0};
    // for (size_t i = 0; i < consumer_nr; ++i)
    // {
    //     consumer.emplace_back([&queue, &all_finished, &consumer_finish_nr]()
    //     {
    //         Task task;
    //         while (!all_finished.load(std::memory_order_relaxed))
    //         {
    //             size_t finish_nr = 0;
    //             for (size_t i = 0; i < 100; ++i)
    //             {
    //                 if (queue.pop(task))
    //                 {
    //                     finish_nr++;
    //                 }
    //             }
    //             consumer_finish_nr.fetch_add(finish_nr,
    //                                          std::memory_order_relaxed);
    //         }
    //     });
    // }
    // PerformanceReporter p("producer", producer_finish_nr, all_finished);
    // p.start(100ms);
    // PerformanceReporter c("consumer", consumer_finish_nr, all_finished);
    // c.start(100ms);
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // size_t producer_nr = 4;
    // size_t consumer_nr = 1;
    // bench_boost_queue(producer_nr, consumer_nr);
    LOG(WARNING) << "Deprecated. not use boost::queue, so do not care about "
                    "its performance";
    return 0;
}