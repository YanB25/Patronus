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

// TODO: strange bug: if mw_idx & mask == magic, will error
uint32_t magic = 0b1010101010;
uint16_t mask = 0b1111111111;

constexpr static size_t kSyncBatch = 100;

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

std::vector<uint32_t> recv_rkeys(std::shared_ptr<DSM> dsm, size_t size)
{
    info("Recving mws with server.");
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
        dsm->send(nullptr, 0, kServerNodeId);
        remain_mw -= should_recv;
    }
    info("Finish recving %lu mws", size);
    // for (size_t i = 0; i < ret.size(); ++i)
    // {
    //     fprintf(stderr, "##%u\n", ret[i]);
    // }
    return ret;
}
void send_rkeys(std::shared_ptr<DSM> dsm, std::vector<ibv_mw *> &mws)
{
    info("Sending mws with client.");
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
            dsm->send(
                (char *) &mws[index]->rkey, sizeof(uint32_t), kClientNodeId);
            index++;
        }
        // wait and sync
        dsm->recv();
        remain_sync_mw -= should_sync;
    }
    info(
        "regular: rkey %u, index %u likely to crash. so skip it. handle: "
        "%u, pd: %p",
        mws[0]->rkey,
        0,
        mws[0]->handle,
        mws[0]->pd);
    info("Finish sending mws.");
}

void bind_rkeys(std::shared_ptr<DSM> dsm,
                std::vector<ibv_mw *> &mws,
                size_t size)
{
    const auto &buf_conf = dsm->get_server_internal_buffer();
    char *buffer = buf_conf.buffer;
    size_t buffer_size = buf_conf.size;

    check(size < buffer_size);

    for (size_t i = 0; i < mws.size(); ++i)
    {
        dsm->bind_memory_region_sync(mws[i], kClientNodeId, 0, buffer, size);
    }

    // size_t remain_nr = mws.size();
    // size_t idx = 0;
    // while (remain_nr > 0)
    // {
    //     size_t kPollSize = 32;
    //     size_t work = std::min(remain_nr, kPollSize);
    //     if (work > 1)
    //     {
    //         for (size_t i = 0; i < work - 1; ++i)
    //         {
    //             check(idx < mws.size());
    //             // TODO: here should not be sync, but I am debugging.
    //             bool succ = dsm->bind_memory_region_sync(
    //                 mws[idx], kClientNodeId, 0 /* thread */, buffer, size);
    //             check(succ);
    //             idx++;
    //         }
    //     }
    //     check(idx < mws.size());
    //     bool succ = dsm->bind_memory_region_sync(
    //         mws[idx], kClientNodeId, 0, buffer, size);
    //     check(succ);
    //     idx++;
    //     remain_nr -= work;
    // }
    // for (size_t i = 0; i < mws.size(); ++i)
    // {
    //     fprintf(stderr, "##%u\n", mws[i]->rkey);
    // }
}

void client(std::shared_ptr<DSM> dsm,
            uint32_t mw_nr,
            size_t size,
            size_t io_size)
{
    constexpr static size_t test_times = 10 * define::K;
    Timer timer;

    info("requiring: mw_nr: %u", mw_nr);
    dsm->send((char *) &mw_nr, sizeof(uint32_t), kServerNodeId);

    auto rkeys = recv_rkeys(dsm, mw_nr);
    check(rkeys.size() == mw_nr);

    auto *buffer = dsm->get_rdma_buffer();

    size_t dsm_size = size;
    size_t io_rng = dsm_size / io_size;

    GlobalAddress gaddr;
    gaddr.nodeID = kServerNodeId;

    info("Benchmarking random write with mw_nr: %u, io_size: %lu at size: %lu",
         mw_nr,
         io_size,
         size);

    timer.begin();
    for (size_t i = 0; i < test_times; ++i)
    {
        size_t io_block_nth = rand_int(0, io_rng - 2);
        dcheck(io_block_nth >= 0);
        dcheck(io_block_nth <= io_rng - 1);
        gaddr.offset = io_block_nth * io_size;
        dcheck(gaddr.offset + io_size < size);
        size_t rkey_idx = rand_int(0, rkeys.size() - 1);
        dcheck(rkey_idx < rkeys.size());
        uint32_t rkey = rkeys.at(rkey_idx);
        if (rkey == 0)
        {
            continue;
        }
        Data data;
        data.lower = rkey;
        data.upper = rkey_idx;

        auto handler = [&rkeys](ibv_wc *wc)
        {
            Data data;
            check(sizeof(Data) == sizeof(uint64_t));
            memcpy(&data, &wc->wr_id, sizeof(uint64_t));
            error("Failed at rkey: %u, rkey_idx: %u", data.lower, data.upper);
            rkeys[data.upper] = 0;
        };
        dsm->rkey_write_sync(
            rkey, buffer, gaddr, io_size, nullptr, data.val, handler);
        // dsm->write_sync(buffer, gaddr, io_size);
    }
    timer.end_print(test_times);

    dsm->send(nullptr, 0, kServerNodeId);
}

void server(std::shared_ptr<DSM> dsm, size_t size)
{
    Timer timer;
    timer.begin();

    uint32_t mw_nr = *(uint32_t *) dsm->recv();
    info("Client request mw_nr: %u", mw_nr);
    std::vector<ibv_mw *> mws;
    mws.reserve(mw_nr);
    for (size_t i = 0; i < mw_nr; ++i)
    {
        mws.emplace_back(dsm->alloc_mw());
        check(mws.back() != nullptr);
    }
    bind_rkeys(dsm, mws, size);
    send_rkeys(dsm, mws);

    info("The latency of handshaking for %u mw", mw_nr);
    timer.end_print(1);

    while (true)
    {
        if (rdmaQueryQueuePair(dsm->get_dir_qp(kClientNodeId, 0)) ==
            IBV_QPS_ERR)
        {
            info("Benchmarking latency of QP recovery");
            timer.begin();
            check(dsm->recover_dir_qp(kClientNodeId, 0));
            timer.end_print(1);
        }
        fflush(stdout);
        sleep(1);
    }

    // wait and sync
    dsm->recv();
}
int main(int argc, char **argv)
{
    if (argc < 2)
    {
        fprintf(stderr, "%s [window_nr]\n", argv[0]);
        return -1;
    }
    size_t window_nr = 0;
    // size_t window_size = 0;
    sscanf(argv[1], "%lu", &window_nr);
    // sscanf(argv[2], "%lu", &window_size);

    constexpr static size_t kSize = 1 * define::GB;
    constexpr static size_t kIOSize = 64;

    // printf("window_nr: %lu, window_size: %lu\n", window_nr, window_size);
    info("memory window nr: %lu from %s", window_nr, argv[1]);
    fflush(stdout);

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
        client(dsm, window_nr, kSize, kIOSize);
    }
    else
    {
        server(dsm, kSize);
    }

    info("finished. ctrl+C to quit.");
}