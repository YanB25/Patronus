#include <iostream>
#include <thread>

#include "DSM.h"
#include "gflags/gflags.h"
#include "util/Tracer.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    util::TraceManager tm(1);

    {
        auto trace = tm.trace("test simple");

        trace.pin("1");
        trace.pin("2");
        LOG(INFO) << trace;
    }

    {
        auto trace = tm.trace("test child");

        trace.child("A");
        trace.child("B");
        LOG(INFO) << trace;
    }
    {
        auto trace = tm.trace("test child 2");

        auto a = trace.child("A");
        a.pin("A.1");
        a.pin("A.2");
        auto b = trace.child("B");
        b.pin("B.1");
        b.pin("B.2");
        LOG(INFO) << trace;
    }

    {
        auto trace = tm.trace("test 1");

        trace.pin("A");
        trace.pin("B");
        auto c = trace.child("C");
        c.pin("C.a");
        c.pin("C.b");
        c.pin("C.c");
        auto cd = c.child("D");
        cd.pin("C.D.a");
        cd.pin("C.D.b");
        cd.pin("C.D.c");
        trace.pin("C");
        trace.pin("D");
        trace.pin("E");
        LOG(INFO) << trace;
    }

    {
        auto trace = tm.trace("complex");
        for (size_t i = 0; i < 10; ++i)
        {
            auto c = trace.child(std::to_string(i));
            for (size_t j = 0; j < 10; ++j)
            {
                c.pin(std::to_string(j));
            }
        }
        LOG(INFO) << trace;
    }

    return 0;
}