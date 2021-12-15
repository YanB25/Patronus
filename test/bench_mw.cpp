#include <algorithm>
#include <random>

#include "DSM.h"
#include "Timer.h"
#include "util/monitor.h"

// Two nodes
// one node issues cas operations

constexpr uint16_t kClientNodeId = 0;
// constexpr uint16_t kServerNodeId = 1;
constexpr uint32_t kMachineNr = 2;

void client([[maybe_unused]] std::shared_ptr<DSM> dsm)
{
    LOG(INFO) << "client: TODO";
}
std::atomic<size_t> window_nr_;
std::atomic<size_t> window_size_;
// 0 for sequential addr
// 1 for random addr
// 2 for random but 4KB aligned addr
std::atomic<int> random_addr_;
std::atomic<size_t> batch_poll_size_;

std::atomic<size_t> alloc_mw_ns;
std::atomic<size_t> free_mw_ns;
std::atomic<size_t> bind_mw_ns;

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

void server(std::shared_ptr<DSM> dsm,
            size_t mw_nr,
            size_t window_size,
            int random_addr,
            size_t batch_poll_size)
{
    const auto &cache = dsm->get_server_internal_buffer();
    char *buffer = (char *) cache.buffer;
    size_t max_size = cache.size;

    {
        Timer timer;

        printf("\n-------- alloc mw ----------\n");
        timer.begin();
        std::vector<ibv_mw *> mws;
        mws.resize(mw_nr);
        for (size_t i = 0; i < mw_nr; ++i)
        {
            mws[i] = dsm->alloc_mw();
        }
        alloc_mw_ns += timer.end(mw_nr);
        timer.print();

        printf("\n-------- bind mw ----------\n");
        CHECK(window_size < max_size) << 
              "mw_nr " << mw_nr << " too large, overflow an rdma buffer.";
        timer.begin();
        size_t remain_nr = mw_nr;
        size_t window_nr = max_size / window_size;
        while (remain_nr > 0)
        {
            size_t work_nr = std::min(remain_nr, batch_poll_size);
            for (size_t i = 0; i < work_nr; ++i)
            {
                const char *buffer_start = 0;
                if (random_addr == 1 || random_addr == 2)
                {
                    // random address
                    size_t rand_min = 0;
                    size_t rand_max = max_size - window_size;

                    buffer_start = buffer + rand_int(rand_min, rand_max);
                    if (random_addr == 2)
                    {
                        // ... but with 4KB aligned
                        buffer_start =
                            (char *) (((uint64_t) buffer_start) % 4096);
                    }
                }
                else
                {
                    CHECK(random_addr == 0);
                    // if i too large, we roll back i to 0.
                    buffer_start = buffer + (i % window_nr) * window_size;
                }
                LOG(WARNING) << "TODO: CHECK if thread_id == 0 is correct.";
                dsm->bind_memory_region(
                    mws[i], kClientNodeId, 0, buffer_start, window_size);
            }
            dsm->poll_rdma_cq(work_nr);
            remain_nr -= work_nr;
        }
        bind_mw_ns += timer.end(mw_nr);
        timer.print();

        printf("\n-------- free mw ----------\n");
        timer.begin();
        for (size_t i = 0; i < mw_nr; ++i)
        {
            dsm->free_mw(mws[i]);
        }
        free_mw_ns += timer.end(mw_nr);
        timer.print();
    }
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

    auto &m = bench::BenchManager::ins();
    auto &bench = m.reg("memory-window");
    bench.add_column("window_nr", &window_nr_)
        .add_column("window_size", &window_size_)
        .add_column("addr-access-type", &random_addr_)
        .add_column("batch-poll-size", &batch_poll_size_)
        .add_column_ns("alloc-mw", &alloc_mw_ns)
        .add_column_ns("bind-mw", &bind_mw_ns)
        .add_column_ns("free-mw", &free_mw_ns)
        .add_dependent_throughput("bind-mw");

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
        // 150 us to alloc one mw.
        // 10000000 mws need 16 min, so we don't bench it.
        std::vector<size_t> window_nr_arr{1000, 10000};
        std::vector<size_t> window_size_arr{
            1, 64, 1024, 4096, 2ull * define::MB, 512 * define::MB};
        std::vector<bool> random_addr_arr{true, false};
        for (auto window_nr : window_nr_arr)
        {
            for (auto window_size : window_size_arr)
            {
                for (int random_addr : {0, 1, 2})
                {
                    for (size_t batch_poll_size : {1, 10, 100})
                    {
                        window_nr_ = window_nr;
                        window_size_ = window_size;
                        random_addr_ = random_addr;
                        batch_poll_size_ = batch_poll_size;

                        server(dsm,
                               window_nr,
                               window_size,
                               random_addr,
                               batch_poll_size);
                        bench.snapshot();
                        bench.clear();
                    }
                }
            }
        }
        m.report("memory-window");
        m.to_csv("memory-window");
    }

    LOG(INFO) << "finished. ctrl+C to quit.";
    while (1)
    {
        sleep(1);
    }
}