#include <boost/version.hpp>
#include <iostream>
#include <thread>

#include "thirdparty/concurrent-queue/entry.h"
#include "util/Pre.h"
#include "util/Rand.h"
#include "util/Tracer.h"

using namespace patronus::cqueue;
using namespace util;
using namespace std::chrono_literals;

int main()
{
    TraceManager tm(1);
    {
        auto trace = tm.trace("test");

        trace.pin("a");
        trace.pin("b");
        trace.pin("c");

        auto trace_a = trace.child("c-a");
        trace_a.pin("c-a.one");
        trace_a.pin("c-a.two");
        auto trace_a_a = trace_a.child("c-a-a");
        trace_a_a.pin("c-a-a.one");
        auto trace_b = trace.child("c-b");
        trace_b.pin("c-b.one");
        LOG(INFO) << trace;

        LOG(INFO) << "==================";

        auto vec = trace.get_flat_records();
        LOG(INFO) << pre_vec(vec);

        auto vec2 = trace.get_flat_records(1);
        LOG(INFO) << pre_vec(vec2);
    }
    {
        auto trace = tm.trace("loop");
        ChronoTimer timer;

        for (size_t i = 0; i < 10; ++i)
        {
            auto next = trace.child(std::to_string(i));
            for (size_t j = 0; j < 10; ++j)
            {
                next.pin(std::to_string(j));
            }
        }
        auto ns = timer.pin();
        LOG(INFO) << trace << " TAKE " << ns << " ns";
    }
    {
        auto trace = tm.trace("empty");

        LOG(INFO) << trace;
    }
    {
        auto trace = tm.trace("correct");

        std::this_thread::sleep_for(10ms);
        auto t2 = trace.child("sleeped");
        t2.pin("t2");

        std::this_thread::sleep_for(10ms);
        auto t3 = trace.child("sleeped again");
        t3.pin("t3");

        LOG(INFO) << trace;
    }
    return 0;
}