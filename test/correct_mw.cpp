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

constexpr static size_t kMagic = 0xffffffffffffffff;
constexpr static size_t kMagic2 = 0xabcdef1234567890;
constexpr static size_t kMagic3 = 0xcccdef1aa45ff890;
constexpr static size_t kOffset = 0;
constexpr static size_t kOffset2 = 0;
constexpr static size_t kOffset3 = 0;

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
        LOG(ERROR) << "buf " << lhs_buf << " != expect";
    }
    printf("buf %p == expect!\n", lhs_buf);
}

void client(std::shared_ptr<DSM> dsm)
{
    GlobalAddress gaddr;
    gaddr.nodeID = kServerNodeId;
    gaddr.offset = kOffset;

    auto *buffer = dsm->get_rdma_buffer();

    *(uint64_t *) buffer = kMagic;

    dsm->write_sync(buffer, gaddr, sizeof(kMagic));
    LOG(INFO) << "write finished at offset " << kOffset << ": " << kMagic;

    while (true)
    {
        auto *read_buffer = buffer + 40960;
        dsm->read_sync(read_buffer, gaddr, sizeof(kMagic));
        printf("read at offset %lu: %lx\n", kOffset, *(uint64_t *) read_buffer);
        if (*(uint64_t *) read_buffer == kMagic)
        {
            break;
        }
        *(uint64_t *) read_buffer = 0;
        sleep(1);
    }
    LOG(INFO) << "Finish basic RDMA write. Testing memory window. ";
    LOG(INFO) << "Sending Identify to server";

    auto id = dsm->get_identify();
    dsm->send((char *) &id, sizeof(id), kServerNodeId);

    LOG(INFO) << "Waiting for rkey from server...";
    char *msg = dsm->recv();
    uint32_t rkey = *(uint32_t *) msg;
    LOG(INFO) << "Get rkey " << rkey;

    LOG(INFO) << "Trying to loop. Expect 8 success and 2 failure.";
    for (size_t i = 0; i < 10; ++i)
    {
        *(uint64_t *) buffer = kMagic2;
        gaddr.offset = kOffset2 + i * sizeof(kMagic2);
        dsm->rkey_write_sync(rkey, buffer, gaddr, sizeof(kMagic2), dirID);
    }

    LOG(INFO) << "We do it again. Expect the rkey still work: 8 success.";
    for (size_t i = 0; i < 8; ++i)
    {
        *(uint64_t *) buffer = kMagic2;
        gaddr.offset = kOffset2 + i * sizeof(kMagic2);
        dsm->rkey_write_sync(rkey, buffer, gaddr, sizeof(kMagic2), dirID);
    }

    dsm->send(nullptr, 0, kServerNodeId);

    if constexpr (NR_DIRECTORY >= 2)
    {
        size_t second_dir = dirID + 1;

        char *msg = dsm->recv();
        uint32_t rkey = *(uint32_t *) msg;
        LOG(INFO) << "Get rkey " << rkey;
        for (size_t i = 0; i < 8; ++i)
        {
            *(uint64_t *) buffer = kMagic3;
            gaddr.offset = kOffset3 + i * sizeof(kMagic3);
            dsm->rkey_write_sync(rkey, buffer, gaddr, sizeof(kMagic3),
            second_dir);
        }
        dsm->send(nullptr, 0, kServerNodeId);
    }
    else
    {
        LOG(WARNING) << "[system] skip second round test.";
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

void server(std::shared_ptr<DSM> dsm)
{
    Timer timer;

    const auto &buf_conf = dsm->get_server_internal_buffer();
    char *buffer = buf_conf.buffer;
    LOG(INFO) << "get buffer addr: " << (void*) buffer;
    // size_t max_size = buf_conf.size;

    loop_expect(buffer + kOffset, (char *) &kMagic, sizeof(kMagic));

    struct ibv_mw *mw = dsm->alloc_mw(dirID);
    LOG(INFO) << "the allocated mw with pd: " << mw->pd;

    Identify *client_id = (Identify *) dsm->recv();
    int node_id = client_id->node_id;
    int thread_id = client_id->thread_id;
    LOG(INFO) << "Get client node_id: " << node_id
              << ", thread_id: " << thread_id;

    dsm->bind_memory_region_sync(mw, node_id, thread_id, buffer, 64, dirID);
    LOG(INFO) << "bind memory window success. Rkey: " << mw->rkey;

    dsm->send((char *) &mw->rkey, sizeof(mw->rkey), kClientNodeId);

    loop_expect(buffer + kOffset2, (char *) &kMagic2, sizeof(kMagic2));

    while (dsm->try_recv() == nullptr)
    {
        if (rdmaQueryQueuePair(dsm->get_dir_qp(node_id, thread_id, dirID)) ==
            IBV_QPS_ERR)
        {
            CHECK(dsm->recoverDirQP(node_id, thread_id, dirID));
            LOG(WARNING) << "[bench] Finished recover DIR QP for node "
                         << node_id << ", thread_id " << thread_id
                         << " at dirID " << dirID;
        }
        usleep(100);
    }

    if constexpr (NR_DIRECTORY >= 2)
    {
        size_t second_dir = dirID + 1;
        LOG(INFO) << "[system] begin testing rolling dir";
        auto *mw2 = dsm->alloc_mw(second_dir);
        dsm->bind_memory_region_sync(
            mw2, node_id, thread_id, buffer, 64, second_dir);
        LOG(INFO) << "bind memory window 2 success. Rkey: " << mw2->rkey;
        dsm->send((char *) &mw2->rkey, sizeof(mw2->rkey), kClientNodeId);

        while (dsm->try_recv() == nullptr)
        {
            if (rdmaQueryQueuePair(dsm->get_dir_qp(
                    node_id, thread_id, second_dir)) == IBV_QPS_ERR)
            {
                LOG(INFO) << "Benchmarking latency of QP recovery";
                timer.begin();
                CHECK(dsm->recoverDirQP(node_id, thread_id, second_dir));
                timer.end_print(1);
            }
            usleep(100);
        }

        loop_expect(buffer + kOffset3, (char *) &kMagic3, sizeof(kMagic3));

        dsm->free_mw(mw2);
    }
    else
    {
        LOG(WARNING) << "[system] skip testing rolling dir. NR_DIRECTORY == "
                     << NR_DIRECTORY;
    }

    dsm->free_mw(mw);

    LOG(INFO) << "Exiting...";
}
int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
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

    LOG(INFO) << "finished. ctrl+C to quit.";
}