#include <algorithm>
#include <chrono>
#include <random>
#include <thread>

#include "Timer.h"
#include "gflags/gflags.h"
#include "patronus/Patronus.h"
#include "thirdparty/racehashing/mock_rdma_adaptor.h"
#include "util/Rand.h"
#include "util/monitor.h"

using namespace std::chrono_literals;

DEFINE_string(exec_meta, "", "The meta data of this execution");

// Two nodes
// one node issues cas operations

constexpr static uint64_t kWaitKey = 0;

using namespace patronus;

using namespace std::chrono_literals;

void test_complex_barrier(Patronus::pointer p)
{
    for (size_t i = 0; i < 1000; ++i)
    {
        auto v = p->try_get("barrier-" + std::to_string(i), 100us);
        CHECK(v.empty());
        p->keeper_barrier(std::to_string(i), 100us);
        p->put("barrier-" + std::to_string(i), "0", 100us);
        if (i > 1)
        {
            auto v = p->get("barrier-" + std::to_string(i - 1), 100us);
            CHECK_EQ(v, "0");
        }
    }
    LOG(INFO) << "[bench] pass barrier";
}
void test_basic_client(Patronus::pointer p)
{
    p->put("magic", "magic!", 100us);
    auto v = p->get("magic2", 100us);
    while (v.empty())
    {
        v = p->get("magic2", 100us);
    }
    CHECK_EQ(v, "magic2!");
}
void test_basic_server(Patronus::pointer p)
{
    auto v = p->get("magic", 100us);
    while (v.empty())
    {
        v = p->get("magic", 100us);
    }
    CHECK_EQ(v, "magic!") << " v is `" << v << "`, length " << v.length();
    p->put("magic2", "magic2!", 100us);
}

void test_bulk_get_client(Patronus::pointer p)
{
    for (size_t i = 0; i < 1000; ++i)
    {
        auto k = std::to_string(i);
        auto v = p->get(k, 10us);
        CHECK_EQ(k, v);
    }
}
void test_bulk_put_server(Patronus::pointer p)
{
    for (size_t i = 0; i < 1000; ++i)
    {
        p->put(std::to_string(i), std::to_string(i), 10us);
    }
}

void test_type_client(Patronus::pointer p)
{
    auto gaddr = p->get_object<GlobalAddress>("race:gaddr", 10us);
    LOG(INFO) << "Got v: " << gaddr;
}

void test_type_server(Patronus::pointer p)
{
    GlobalAddress gaddr((void *) fast_pseudo_rand_int());
    p->put("race:gaddr", gaddr, 10ns);
    LOG(INFO) << "Put v: " << gaddr;
}

void client(Patronus::pointer p)
{
    LOG(INFO) << "[bench] begin basic test";
    test_basic_client(p);
    LOG(INFO) << "[bench] PASS basic test";
    test_bulk_get_client(p);
    LOG(INFO) << "[bench] PASS bulk test";
    test_complex_barrier(p);

    test_type_client(p);
}

void server(Patronus::pointer p)
{
    LOG(INFO) << "[bench] begin basic test";
    test_basic_server(p);
    LOG(INFO) << "[bench] PASS basic test";
    test_bulk_put_server(p);
    LOG(INFO) << "[bench] PASS bulk test";
    test_complex_barrier(p);

    test_type_server(p);
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    bindCore(0);

    rdmaQueryDevice();

    PatronusConfig config;
    config.machine_nr = ::config::kMachineNr;

    auto patronus = Patronus::ins(config);

    auto nid = patronus->get_node_id();
    if (::config::is_client(nid))
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

    LOG(INFO) << "finished. ctrl+C to quit.";
}