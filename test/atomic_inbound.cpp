#include <thread>

#include "DSM.h"
#include "Timer.h"
#include "zipf.h"

/*
./restartMemc.sh && /usr/local/openmpi/bin/mpiexec --allow-run-as-root -hostfile
./host  -np 9 ./atomic_inbound 9 4 0 0 8
*/

const int kMaxTestThread = 24;
const int kBucketPerThread = 32;

std::thread th[kMaxTestThread];
uint64_t tp_counter[kMaxTestThread][8];
uint64_t tp_write_counter[kMaxTestThread][8];
std::shared_ptr<DSM> dsm;

int node_nr, my_node;
int thread_nr;
double kZipfan = 0;
uint64_t cas_sleep_ns = 0;
int kPacketSize = 16;

void send_cas(int node_id, int thread_id)
{
    bindCore(thread_id);

    dsm->registerThread();

    uint64_t sendCounter = 0;
    uint64_t *buffer = (uint64_t *) dsm->get_rdma_buffer().buffer;
    size_t buffer_size = sizeof(uint64_t) * kBucketPerThread;

    GlobalAddress gaddr;
    const uint64_t offset_start =
        node_id * (thread_nr * buffer_size) + buffer_size * thread_id;
    printf("global address is [%lx %lx)\n",
           offset_start,
           offset_start + buffer_size);
    gaddr.nodeID = node_nr - 1;

    memset(buffer, 0, buffer_size);
    gaddr.offset = offset_start;
    dsm->write_sync((char *) buffer, gaddr, buffer_size);

    uint64_t new_val = 0;
    while (true)
    {
        if ((sendCounter & SIGNAL_BATCH) == 0 && sendCounter > 0)
        {
            dsm->poll_rdma_cq(1);
            tp_counter[thread_id][0] += SIGNAL_BATCH + 1;
        }

        int kth = sendCounter % kBucketPerThread;
        if (kth == 0)
        {
            new_val++;
        }

        // sleep(1);
        // printf("%ld %d\n", buffer[kth], new_val - 1);
        // assert(buffer[kth] == new_val - 1);
        gaddr.offset = offset_start + kth * sizeof(uint64_t);

        // sleep
        // dsm->write((char *)buffer, gaddr, sizeof(uint64_t), (sendCounter &
        // SIGNAL_BATCH) == 0);
        dsm->cas_dm(gaddr,
                    new_val - 1,
                    new_val,
                    buffer + kth,
                    (sendCounter & SIGNAL_BATCH) == 0);

        ++sendCounter;
    }
}

void send_skew_cas([[maybe_unused]] int node_id, int thread_id)
{
    const int kCounterBucket = 4096;
    const uint64_t seed = time(nullptr);

    // zipfan
    struct zipf_gen_state state;
    mehcached_zipf_init(&state, kCounterBucket, kZipfan, seed);

    bindCore(thread_id);
    dsm->registerThread();

    uint64_t sendCounter = 0;
    uint64_t *buffer = (uint64_t *) dsm->get_rdma_buffer().buffer;
    size_t buffer_size = sizeof(uint64_t) * kCounterBucket;

    GlobalAddress gaddr;
    gaddr.nodeID = node_nr - 1;

    memset(buffer, 0, buffer_size);
    gaddr.offset = 0;
    dsm->write_sync((char *) buffer, gaddr, buffer_size);

    while (true)
    {
        if ((sendCounter & SIGNAL_BATCH) == 0 && sendCounter > 0)
        {
            dsm->poll_rdma_cq(1);
            tp_counter[thread_id][0] += SIGNAL_BATCH + 1;
        }

        if (cas_sleep_ns != 0)
        {
            Timer::sleep(cas_sleep_ns);
        }

        gaddr.offset = mehcached_zipf_next(&state) * sizeof(uint64_t);
        // + 1 * define::MB;

        dsm->cas(gaddr,
                 0,
                 100,
                 buffer + gaddr.offset,
                 (sendCounter & SIGNAL_BATCH) == 0,
                 0 /* wr_id */,
                 nullptr);

        ++sendCounter;
    }
}

void send_write(int node_id, int thread_id)
{
    const int kDifferLocation = 256;

    bindCore(thread_id);
    dsm->registerThread();

    uint64_t sendCounter = 0;
    char *buffer = dsm->get_rdma_buffer().buffer;
    size_t buffer_size = kPacketSize * kDifferLocation;

    GlobalAddress gaddr;
    gaddr.nodeID = node_nr - 1;

    memset(buffer, 0, buffer_size);
    const uint64_t offset_start =
        node_id * (thread_nr * buffer_size) + buffer_size * thread_id;
    printf("global address is [%lx %lx)\n",
           offset_start,
           offset_start + buffer_size);

    while (true)
    {
        if ((sendCounter & SIGNAL_BATCH) == 0 && sendCounter > 0)
        {
            dsm->poll_rdma_cq(1);
            tp_write_counter[thread_id][0] += SIGNAL_BATCH + 1;
        }

        int kTh = sendCounter % kDifferLocation;
        gaddr.offset = kTh * kPacketSize;

        dsm->write(buffer + gaddr.offset,
                   gaddr,
                   kPacketSize,
                   (sendCounter & SIGNAL_BATCH) == 0);

        ++sendCounter;
    }
}

void read_args(int argc, char **argv)
{
    if (argc < 3)
    {
        fprintf(stderr,
                "Usage: ./atomic_inbound node_nr thread_nr [zipfan] [cas cap] "
                "packet_size\n");
        exit(-1);
    }

    node_nr = std::atoi(argv[1]);
    thread_nr = std::atoi(argv[2]);

    if (argc >= 4)
    {
        kZipfan = std::atof(argv[3]);
    }

    if (argc >= 5)
    {
        cas_sleep_ns = std::atoi(argv[4]);
    }

    if (argc >= 6)
    {
        kPacketSize = std::atoi(argv[5]);
    }

    LOG(INFO) << "node_nr [" << node_nr << "], thread_nr [" << thread_nr
              << "], zipfan [" << kZipfan << " << ], cas sleep ns ["
              << cas_sleep_ns
              << "] "
                 "packsize ["
              << kPacketSize << "]";
}

int main(int argc, char **argv)
{
    bindCore(0);

    read_args(argc, argv);

    DSMConfig config;
    config.machineNR = node_nr;
    dsm = DSM::getInstance(config);

    if (dsm->getMyNodeID() == node_nr - 1)
    {
        while (true)
        {
            sleep(1);
        }
    }

    bindCore(20);

    for (int i = 0; i < thread_nr; ++i)
    {
        // th[i] = std::thread(send_cas, dsm->getMyNodeID(), i);
        //  th[i] = std::thread(send_skew_cas, dsm->getMyNodeID(), i);
        th[i] = std::thread(send_write, dsm->getMyNodeID(), i);
    }

    Timer timer;

    while (true)
    {
        timer.begin();

        sleep(1);

        auto ns = timer.end();

        uint64_t tp = 0;
        uint64_t write_tp = 0;
        for (int i = 0; i < thread_nr; ++i)
        {
            tp += tp_counter[i][0];
            tp_counter[i][0] = 0;

            write_tp += tp_write_counter[i][0];
            tp_write_counter[i][0] = 0;
        }

        double atomic_tp = tp * 1.0 / (1.0 * ns / 1000 / 1000 / 1000);
        double data_tp = write_tp * 1.0 / (1.0 * ns / 1000 / 1000 / 1000);
        printf("node %d, cas tp %.2lf, data tp %.2lf, all data %.2lf\n",
               dsm->getMyNodeID(),
               atomic_tp,
               data_tp,
               data_tp * (node_nr - 1));
    }
}