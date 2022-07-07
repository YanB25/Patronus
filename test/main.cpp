#include <iostream>
#include <thread>
#include <unordered_map>

#include "DSM.h"
#include "gflags/gflags.h"
#include "patronus/Patronus.h"
// #include "util/ezprint.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

struct Test
{
    uint64_t a;
    char buffer[];
};

struct Test2
{
    size_t age;
    std::string name;
};

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    Test2 test;
    test.age = 10;
    test.name = "yb";
    // LOG(INFO) << "It is " << util::pre_ez(test);
    // ez::print(test);

    // DSMConfig config;
    // config.machineNR = ::config::kMachineNr;

    // // Do all the things here
    // auto dsm = DSM::getInstance(config);
    // // When you reach here, all the things have done

    // // You must call registerThread() before using DSM.
    // dsm->registerThread();

    // LOG(INFO) << "Connection build. node_id: " << dsm->get_node_id()
    //           << ", thread_id: " << dsm->get_thread_id();
    // size_t m = patronus::MemoryMessagePayload();
    // LOG(INFO) << "max size: " << m;
    using namespace util::pre;
    {
        std::unordered_map<std::string, std::string> m;
        m["a"] = "b";
        m["c"] = "d";
        m["e"] = "f";
        m["g"] = "h";
        // LOG(INFO) << util::pre_umap(m, 2);
        // LOG(INFO) << util::pre_umap(m);
        // LOG(INFO) << m;
    }
    {
        std::map<std::string, std::string> m;
        m["a"] = "b";
        m["c"] = "d";
        m["e"] = "f";
        m["g"] = "h";
        // LOG(INFO) << util::pre_map(m, 2);
        // LOG(INFO) << util::pre_map(m);
        // LOG(INFO) << m;
    }
    {
        std::vector<std::string> v;
        v.push_back("a");
        v.push_back("b");
        v.push_back("c");
        v.push_back("d");
        // LOG(INFO) << util::pre_iter(v, 2);
        // LOG(INFO) << util::pre_iter(v);
        // LOG(INFO) << v;
    }
    {
        std::pair<std::string, std::string> p("abc", "def");
        // LOG(INFO) << util::pre_pair(p);
        // LOG(INFO) << p;
    }

    return 0;
}