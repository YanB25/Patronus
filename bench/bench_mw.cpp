#include <algorithm>
#include <random>

#include "DSM.h"
#include "Timer.h"
#include "gflags/gflags.h"
#include "util/Rand.h"
#include "util/monitor.h"

constexpr uint16_t kClientNodeId = 0;
// constexpr uint16_t kServerNodeId = 1;
constexpr uint32_t kMachineNr = 2;

constexpr static size_t dirID = 0;

DEFINE_string(exec_meta, "", "The meta data of this execution");

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

void server(std::shared_ptr<DSM> dsm,
            size_t mw_nr,
            size_t window_size,
            int random_addr,
            size_t batch_poll_size)
{
    const auto &cache = dsm->get_server_buffer();
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
            mws[i] = dsm->alloc_mw(dirID);
        }
        alloc_mw_ns += timer.end(mw_nr);
        timer.print();

        printf("\n-------- bind mw ----------\n");
        auto bind_mw_begin = std::chrono::steady_clock::now();

        CHECK(window_size < max_size)
            << "mw_nr " << mw_nr << " too large, overflow an rdma buffer.";
        timer.begin();
        size_t remain_nr = mw_nr;
        size_t window_nr = max_size / window_size;

        ibv_wc wc_buffer[1024];

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

                    buffer_start =
                        buffer + fast_pseudo_rand_int(rand_min, rand_max);
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
                // every post send will be signaled.
                dsm->bind_memory_region(mws[i],
                                        kClientNodeId,
                                        0,
                                        buffer_start,
                                        window_size,
                                        dirID,
                                        0,
                                        true);
            }
            // dsm->poll_dir_cq(dirID, work_nr);
            size_t polled = 0;
            while (polled < work_nr)
            {
                polled += dsm->try_poll_dir_cq(wc_buffer, dirID, 1024);
            }
            remain_nr -= work_nr;
        }
        bind_mw_ns += timer.end(mw_nr);
        timer.print();

        auto bind_mw_end = std::chrono::steady_clock::now();
        auto bind_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           bind_mw_end - bind_mw_begin)
                           .count();
        LOG(INFO) << "[bench] bind memory window op: " << mw_nr
                  << ", ns: " << bind_ns
                  << ", ops: " << 1.0 * 1e9 * mw_nr / bind_ns;

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
int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

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
        // std::vector<size_t> window_size_arr{
        //     1, 2ull * define::MB, 512 * define::MB};
        std::vector<bool> random_addr_arr{true, false};
        // for (auto window_nr : window_nr_arr)
        for (auto window_nr : {10000})
        {
            for (auto window_size : {2 * define::MB})
            // for (auto window_size : {64})
            {
                // for (int random_addr : {0, 1, 2})
                for (int random_addr : {2})
                {
                    for (size_t batch_poll_size : {8})
                    // for (size_t batch_poll_size : {1, 10, 100})
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