#include <algorithm>
#include <random>

#include "DSM.h"
#include "Timer.h"
#include "boost/thread/barrier.hpp"
#include "gflags/gflags.h"
#include "util/Rand.h"
#include "util/monitor.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

uint32_t magic = 0b1010101010;
uint16_t mask = 0b1111111111;

std::atomic<uint64_t> master_tid{(uint64_t) -1};

constexpr static size_t kSyncBatch = 100;

enum RWType
{
    kRO,
    kWO,
};
std::atomic<size_t> window_nr_x;
std::atomic<size_t> thread_nr_x;
std::atomic<size_t> io_size_x;
std::atomic<size_t> size_x;
std::atomic<size_t> ops_y;
std::atomic<size_t> avg_lat_y;
std::atomic<size_t> rkey_warmup_fail_y;
std::atomic<size_t> rkey_fail_y;
std::atomic<size_t> expr_id{0};
std::atomic<RWType> bench_type{kWO};

constexpr static size_t dirID = 0;

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
        LOG(ERROR) << "buf " << lhs_buf << " != expect\n";
    }
    printf("buf %p == expect!\n", lhs_buf);
}

std::vector<uint32_t> recv_rkeys(std::shared_ptr<DSM> dsm, size_t size)
{
    LOG(INFO) << "[master] Recving mws " << size << " from server.";
    auto server_nid = ::config::get_server_nids().front();
    std::vector<uint32_t> ret;
    ret.reserve(size);
    size_t remain_mw = size;
    while (remain_mw > 0)
    {
        size_t should_recv = std::min(remain_mw, kSyncBatch);
        for (size_t i = 0; i < should_recv; ++i)
        {
            uint32_t rkey = *(uint32_t *) dsm->recv();
            ret.push_back(rkey);
        }
        dsm->send(nullptr, 0, server_nid);
        remain_mw -= should_recv;
    }
    LOG(INFO) << "Finish recving " << size << " mws";
    // for (size_t i = 0; i < ret.size(); ++i)
    // {
    //     fprintf(stderr, "##%u\n", ret[i]);
    // }
    return ret;
}
void send_rkeys(std::shared_ptr<DSM> dsm, std::vector<ibv_mw *> &mws)
{
    LOG(INFO) << "Sending mws with client.";
    size_t remain_sync_mw = mws.size();
    size_t index = 0;
    while (remain_sync_mw > 0)
    {
        size_t should_sync = std::min(remain_sync_mw, kSyncBatch);
        for (size_t i = 0; i < should_sync; ++i)
        {
            // if ((index & mask) == magic)
            // {
            //     warn(
            //         "rkey %u, index %u likely to crash. so skip it. handle: "
            //         "%u, pd: %p",
            //         mws[index]->rkey,
            //         index,
            //         mws[index]->handle,
            //         mws[index]->pd);
            //     mws[index]->rkey = 0;
            // }
            CHECK(false) << "the 0 below should be client_nid";
            dsm->send((char *) &mws[index]->rkey,
                      sizeof(uint32_t),
                      0 /* client_nid */);
            index++;
        }
        // wait and sync
        dsm->recv();
        remain_sync_mw -= should_sync;
    }
    LOG(INFO) << "Finish sending mws.";
}

void bind_rkeys(std::shared_ptr<DSM> dsm,
                std::vector<ibv_mw *> &mws,
                size_t size)
{
    const auto &buf_conf = dsm->get_server_buffer();
    char *buffer = buf_conf.buffer;
    size_t buffer_size = buf_conf.size;

    CHECK(size <= buffer_size);

    for (size_t t = 0; t < mws.size(); ++t)
    {
        CHECK(false) << "TODO: below should be correct client_nid";
        dsm->bind_memory_region_sync(
            mws[t], 0 /* client_nid */, 0, buffer, size, dirID, 0);
    }
}

constexpr static size_t kClientBatchWrite = 1;

std::vector<uint32_t> rkeys;

// [rkey_start_idx, rkey_end_idx)
void client_burn(std::shared_ptr<DSM> dsm,
                 size_t size,
                 size_t io_size,
                 RWType bt,
                 bool sequantial,
                 bool warmup)
{
    constexpr static size_t test_times = 100_K;
    Timer timer;

    auto *buffer = dsm->get_rdma_buffer().buffer;
    size_t dsm_size = size;
    size_t io_rng = dsm_size / io_size;
    auto server_nid = ::config::get_server_nids().front();

    GlobalAddress gaddr;
    gaddr.nodeID = server_nid;

    auto handler = [sequantial, warmup](ibv_wc *wc) {
        Data data;
        CHECK(sizeof(Data) == sizeof(uint64_t));
        memcpy(&data, &wc->wr_id, sizeof(uint64_t));
        LOG(ERROR) << "Failed for rkey: " << data.lower
                   << ", rkey_idx: " << data.upper
                   << ". Remove from the rkey pool.";
        rkeys[data.upper] = 0;
        if (sequantial || warmup)
        {
            rkey_warmup_fail_y.fetch_add(1);
        }
        else
        {
            rkey_fail_y.fetch_add(1);
        }
    };

    timer.begin();
    size_t rkey_idx = 0;
    for (size_t i = 0; i < test_times; i++)
    {
        size_t io_block_nth = fast_pseudo_rand_int(0, io_rng - 1);
        DCHECK(io_block_nth >= 0);
        gaddr.offset = io_block_nth * io_size;
        DCHECK(gaddr.offset + io_size < size);
        if (sequantial)
        {
            rkey_idx = (rkey_idx + 1) % rkeys.size();
        }
        else
        {
            rkey_idx = fast_pseudo_rand_int(0, rkeys.size() - 1);
        }
        DCHECK(rkey_idx < rkeys.size());
        uint32_t rkey = rkeys[rkey_idx];
        if (rkey == 0)
        {
            continue;
        }

        Data data;
        data.lower = rkey;
        data.upper = rkey_idx;
        if (bt == kWO)
        {
            if (sequantial || (i % kClientBatchWrite == 0))
            {
                dsm->rkey_write_sync(rkey,
                                     buffer,
                                     gaddr,
                                     io_size,
                                     dirID,
                                     nullptr,
                                     data.val,
                                     handler);
            }
            else
            {
                dsm->rkey_write(rkey,
                                buffer,
                                gaddr,
                                io_size,
                                dirID,
                                false,
                                nullptr,
                                data.val);
            }
        }
        else if (bt == kRO)
        {
            if (sequantial || (i % kClientBatchWrite == 0))
            {
                dsm->rkey_read_sync(rkey,
                                    buffer,
                                    gaddr,
                                    io_size,
                                    dirID,
                                    nullptr,
                                    data.val,
                                    handler);
            }
            else
            {
                dsm->rkey_read(rkey,
                               buffer,
                               gaddr,
                               io_size,
                               dirID,
                               false,
                               nullptr,
                               data.val);
            }
        }
        else
        {
            CHECK(false) << "unknown Benchmark type: " << bt;
        }
    }

    auto ns = timer.end();
    if (!warmup)
    {
        double my_ops = test_times * 1e9 / ns;
        ops_y.fetch_add(my_ops);
        // only the last one will be reported
        avg_lat_y.store(1.0 * ns / test_times);
    }
}

std::vector<uint32_t> prepare_client(std::shared_ptr<DSM> dsm, uint32_t mw_nr)
{
    auto server_nid = config::get_server_nids().front();
    LOG(INFO) << "requiring: mw_nr: " << mw_nr;
    dsm->send((char *) &mw_nr, sizeof(uint32_t), server_nid);
    auto rkeys = recv_rkeys(dsm, mw_nr);
    CHECK(rkeys.size() == mw_nr);
    return rkeys;
}

void client(std::shared_ptr<DSM> dsm,
            size_t size,
            size_t io_size,
            size_t thread_nr,
            size_t tid,
            RWType bt,
            boost::barrier &client_bar)
{
    auto server_nid = config::get_server_nids().front();

    size_t mw_per_thread = std::max(rkeys.size() / thread_nr, size_t(1));
    CHECK_LE(mw_per_thread, rkeys.size());

    client_bar.wait();

    LOG_IF(INFO, tid == master_tid) << "detecting failed rkeys...";
    if (tid == master_tid)
    {
        client_burn(dsm, size, io_size, bt, true, true);
    }

    client_bar.wait();
    LOG_IF(INFO, tid == master_tid) << "warming up...";
    if (tid < thread_nr)
    {
        client_burn(dsm, size, io_size, bt, false, true);
    }
    client_bar.wait();
    LOG_IF(INFO, tid == master_tid) << "benchmarking...";
    if (tid < thread_nr)
    {
        client_burn(dsm, size, io_size, bt, false, false);
    }
    client_bar.wait();
    if (tid == master_tid)
    {
        uint8_t ig = 0xfc;
        DLOG(INFO) << "client 0 sending 0xfc ...";
        dsm->send((char *) &ig, sizeof(ig), server_nid);
    }
}
void cleanup_server(std::shared_ptr<DSM> dsm, std::vector<ibv_mw *> &mws)
{
    for (ibv_mw *mw : mws)
    {
        dsm->free_mw(mw);
    }
}
std::vector<ibv_mw *> prepare_server(std::shared_ptr<DSM> dsm, size_t size)
{
    Timer timer;
    timer.begin();

    LOG(INFO) << "Server expecting new mw_nr";
    uint32_t mw_nr = *(uint32_t *) dsm->recv();
    LOG(INFO) << "Client request mw_nr: " << mw_nr;
    std::vector<ibv_mw *> mws;
    mws.reserve(mw_nr);
    for (size_t i = 0; i < mw_nr; ++i)
    {
        mws.emplace_back(dsm->alloc_mw(dirID));
        CHECK(mws.back() != nullptr);
    }
    bind_rkeys(dsm, mws, size);
    send_rkeys(dsm, mws);

    LOG(INFO) << "The latency of handshaking for " << mw_nr << " mw";
    timer.end_print(1);

    return mws;
}

void server(std::shared_ptr<DSM> dsm, size_t thread_nr)
{
    char *recv_buf = dsm->try_recv();
    DLOG(INFO) << "Entering loop";
    while (true)
    {
        if (recv_buf != nullptr)
        {
            uint8_t ig = *(uint8_t *) recv_buf;
            if (ig == 0xfc)
            {
                break;
            }
            else
            {
                LOG(ERROR) << "Expect 0xfc but get " << ig;
            }
        }
        for (size_t tid = 0; tid < 1 + thread_nr; ++tid)
        {
            for (size_t dir = 0; dir < NR_DIRECTORY; ++dir)
            {
                for (auto client_nid : ::config::get_client_nids())
                {
                    if (rdmaQueryQueuePair(dsm->get_dir_qp(
                            client_nid, tid, dir)) == IBV_QPS_ERR)
                    {
                        Timer timer;
                        LOG(INFO) << ("Benchmarking latency of QP recovery");
                        timer.begin();
                        CHECK(dsm->recoverDirQP(client_nid, tid, dir));
                        timer.end_print(1);
                    }
                }
            }
            fflush(stdout);
        }
        recv_buf = dsm->try_recv();
    }
    DLOG(INFO) << "Leaving loop";
}

constexpr static size_t kMaxThread = 24;

void thread_main(std::shared_ptr<DSM> dsm,
                 size_t mw_nr,
                 size_t thread_nr,
                 size_t nid,
                 size_t tid,
                 size_t size,
                 size_t io_size,
                 RWType bt,
                 boost::barrier &client_bar,
                 bool is_client)
{
    CHECK(false) << "TODO: std::ignore = nid";
    std::ignore = nid;
    if (is_client)
    {
        // let client spining
        if (tid == master_tid)
        {
            rkeys.clear();
            rkeys = prepare_client(dsm, mw_nr);
            LOG(INFO) << "[" << tid << "] rkeys.size() " << rkeys.size();
        }
        client_bar.wait();
        client(dsm, size, io_size, thread_nr, tid, bt, client_bar);
        client_bar.wait();
    }
    else
    {
        if (tid == master_tid)
        {
            auto mws = prepare_server(dsm, size);
            server(dsm, thread_nr);
            cleanup_server(dsm, mws);
        }
    }
}
int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    constexpr static size_t kSize = 2_GB;

    rdmaQueryDevice();

    DSMConfig config;
    config.machineNR = ::config::kMachineNr;

    CHECK(false) << "TODO: too old and don't want to run this file.";

    auto dsm = DSM::getInstance(config);
    auto nid = dsm->get_node_id();

    std::vector<std::thread> threads;

    boost::barrier client_bar(kMaxThread);

    auto &b = bench::BenchManager::ins().reg("mw-rw-scalability");
    b.add_column("window-nr", &window_nr_x)
        .add_column("id", &expr_id)
        .add_column("thread-nr", &thread_nr_x)
        .add_column("size", &size_x)
        .add_column("io-size", &io_size_x)
        .add_column("bench-type", &bench_type)
        .add_column("ops", &ops_y)
        .add_column_ns("avg-latency", &avg_lat_y)
        .add_column("rkey-warmup-fail-nr", &rkey_warmup_fail_y)
        .add_column("rkey-fail-nr", &rkey_fail_y);

    dsm->keeper_barrier("begin", 100ms);

    bool is_client = ::config::is_client(nid);

    for (size_t i = 0; i < kMaxThread; ++i)
    {
        threads.emplace_back([dsm, &client_bar, &b, is_client]() {
            dsm->registerThread();
            auto nid = dsm->getMyNodeID();
            uint64_t tid = dsm->get_thread_id();

            // select a leader
            uint64_t old = (uint64_t) -1;
            if (master_tid.compare_exchange_strong(
                    old, tid, std::memory_order_seq_cst))
            {
                LOG(INFO) << "Leader is tid " << tid;
            }

            // for (Type bt : {kRO, kWO})
            for (RWType bt : {kRO})
            {
                for (size_t window_nr : {1, 100, 10000})
                // for (size_t window_nr : {100, 10000})
                {
                    // for (size_t thread_nr : {1, 8, 16, int(kMaxThread)})
                    for (size_t thread_nr : {1, 8, 16})
                    {
                        for (size_t size : {2_MB, kSize})
                        {
                            for (size_t io_size : {8})
                            // for (size_t io_size : {8, 64, 256, 1024})
                            {
                                if (tid == master_tid)
                                {
                                    window_nr_x = window_nr;
                                    thread_nr_x = thread_nr;
                                    size_x = size;
                                    io_size_x = io_size;
                                    bench_type = bt;
                                    expr_id.fetch_add(1);
                                }
                                LOG_IF(INFO, tid == master_tid && is_client)
                                    << "Benchmarking mw: " << window_nr
                                    << ", thread: " << thread_nr
                                    << ", "
                                       "io_size: "
                                    << io_size;
                                thread_main(dsm,
                                            window_nr,
                                            thread_nr,
                                            nid,
                                            tid,
                                            kSize,
                                            io_size,
                                            bt,
                                            client_bar,
                                            is_client);
                                if (tid == master_tid)
                                {
                                    b.snapshot();
                                    b.clear();
                                }
                            }
                        }
                    }
                }
            }
        });
    }
    for (auto &t : threads)
    {
        t.join();
    }

    if (is_client)
    {
        b.report(std::cout);
        bench::BenchManager::ins().to_csv("mw-rw-scalability");
    }

    LOG(INFO) << "finished. ctrl+C to quit.";
    LOG(WARNING) << "TODO: There is still bug: the server bind mw for only "
                    "thread 0, but "
                    "24 threads at the client use the rkeys.";
    LOG(WARNING) << "TODO: Because we are using TYPE_1 MW, and it binds to pd "
                    "instead of "
                    "QP, so the bug does not take effect";
    LOG(WARNING)
        << "TODO: If we start to use TYPE_2, the bug may cause failing";
}