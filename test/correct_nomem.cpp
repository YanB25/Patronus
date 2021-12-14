#include <algorithm>
#include <random>

#include "DSM.h"
#include "Timer.h"
#include "util/monitor.h"

// Two nodes
// one node issues cas operations

constexpr uint16_t kClientNodeId = 0;
constexpr uint16_t kServerNodeId = 1;
constexpr uint32_t kMachineNr = 2;

constexpr static size_t kRpcNr = 2 * define::M;
constexpr static size_t kMWNr = 200 * define::M;
// constexpr static size_t kRpcNr = 200;
// constexpr static size_t kMWNr = 1000;
constexpr static size_t kSyncBatch = 64;
constexpr static size_t kBindSize = 64;

void loop_expect(const char *lhs_buf, const char *rhs_buf, size_t size)
{
    while (memcmp(lhs_buf, rhs_buf, size) != 0)
    {
        printf("buf %p != expect\n", lhs_buf);
        sleep(1);
    }
    printf("buf %p == expect!\n", lhs_buf);
}

void expect(const char *lhs_buf, const char *rhs_buf, size_t size)
{
    if (memcmp(lhs_buf, rhs_buf, size) != 0)
    {
        error("buf %p != expect\n", lhs_buf);
    }
    printf("buf %p == expect!\n", lhs_buf);
}

void server(std::shared_ptr<DSM> dsm)
{
    size_t remain_sync = kRpcNr;
    size_t index = 0;
    while (remain_sync > 0)
    {
        size_t should_sync = std::min(remain_sync, kSyncBatch);
        for (size_t i = 0; i < should_sync; ++i)
        {
            uint32_t rkey = 0xabcdef;
            dsm->send((char *) &rkey, sizeof(uint32_t), kClientNodeId);
            index++;
        }
        // wait and sync
        dsm->recv();
        remain_sync -= should_sync;
    }
    info("Server starts to idle");
    while (true)
    {

    }
}
// Notice: TLS object is created only once for each combination of type and
// thread. Only use this when you prefer multiple callers share the same
// instance.
template <class T, class... Args>
inline T &TLS(Args &&...args)
{
    thread_local T _tls_item(std::forward<Args>(args)...);
    return _tls_item;
}
inline std::mt19937 &rand_generator()
{
    return TLS<std::mt19937>();
}

// [min, max]
uint64_t rand_int(uint64_t min, uint64_t max)
{
    std::uniform_int_distribution<uint64_t> dist(min, max);
    return dist(rand_generator());
}

void client(std::shared_ptr<DSM> dsm)
{
    size_t remain_mw = kRpcNr;
    size_t done_work = 0;
    while (remain_mw > 0)
    {
        size_t should_recv = std::min(remain_mw, kSyncBatch);
        for (size_t i = 0; i < should_recv; ++i)
        {
            [[maybe_unused]] uint32_t rkey = *(uint32_t *) dsm->recv();
        }
        dsm->send(nullptr, 0, kServerNodeId);
        remain_mw -= should_recv;

        done_work += should_recv;
        if (done_work > 10 * define::M)
        {
            info("finish 10M. Remain %zu", remain_mw);
            done_work = 0;
        }
    }
    info("OK. RPC does not leak memory. perfect.");

    const auto &buf_conf = dsm->get_server_internal_buffer();
    char *buffer = buf_conf.buffer;

    auto *mw = dsm->alloc_mw();
    for (size_t i = 0; i < kMWNr; ++i)
    {
        bool succ = dsm->bind_memory_region_sync(
            mw, kServerNodeId, 0, buffer, kBindSize);
        CHECK(succ);
        if (i % (1 * define::K) == 0)
        {
            info("Finish alloc/free mw for 1k. Now: %zu", i);
        }
        // if (i % (20 * define::K) == 0)
        // {
        //     info("Finish 20K. sleep 10s for NIC...");
        //     sleep(10);
        //     info("Wakeup");
        // }
    }
    dsm->free_mw(mw);
}
int main()
{
    // if (argc < 3)
    // {
    //     fprintf(stderr, "%s [window_nr] [window_size]\n", argv[0]);
    //     return -1;
    // }
    // size_t window_nr = 0;
    // size_t window_size = 0;
    // sscanf(argv[1], "%lu", &window_nr);
    // sscanf(argv[2], "%lu", &window_size);

    // printf("window_nr: %lu, window_size: %lu\n", window_nr, window_size);

    bindCore(0);

    rdmaQueryDevice();

    DSMConfig config;
    config.machineNR = kMachineNr;

    auto dsm = DSM::getInstance(config);

    sleep(1);

    dsm->registerThread();

    // let client spining
    auto nid = dsm->getMyNodeID();
    if (nid == kClientNodeId)
    {
        client(dsm);
    }
    else
    {
        server(dsm);
    }

    info("finished. ctrl+C to quit.");
}