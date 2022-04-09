#include <thread>

#include "DSM.h"
#include "Timer.h"
#include "util/zipf.h"

const int kMaxTestThread = 24;
[[maybe_unused]] const int kBucketPerThread = 32;

std::thread th[kMaxTestThread];
uint64_t tp_write_counter[kMaxTestThread][8];
std::shared_ptr<DSM> dsm{nullptr};

int node_nr, my_node;
int thread_nr;
int kPacketSize = 16;

void send_write(int node_id, int thread_id)
{
    const int kDifferLocation = 256;

    bindCore(thread_id);
    dsm->registerThread();

    uint64_t sendCounter = 0;
    char *buffer = dsm->get_rdma_buffer().buffer;
    size_t buffer_size = kPacketSize * kDifferLocation;

    GlobalAddress gaddr;
    gaddr.nodeID = thread_id % (node_nr - 1);

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

        dsm->read(buffer + gaddr.offset,
                  gaddr,
                  kPacketSize,
                  (sendCounter & SIGNAL_BATCH) == 0);

        ++sendCounter;
    }
}

void read_args(int argc, char **argv)
{
    if (argc != 4)
    {
        fprintf(stderr, "Usage: ./outbound node_nr thread_nr packet_size\n");
        exit(-1);
    }

    node_nr = std::atoi(argv[1]);
    thread_nr = std::atoi(argv[2]);
    kPacketSize = std::atof(argv[3]);

    LOG(INFO) << "node_nr [" << node_nr << "], thread_nr [" << thread_nr
              << "], packsize [" << kPacketSize << "]";
}

int main(int argc, char **argv)
{
    bindCore(0);

    read_args(argc, argv);

    DSMConfig config;
    config.machineNR = node_nr;
    dsm = DSM::getInstance(config);

    if (dsm->getMyNodeID() != node_nr - 1)
    {
        while (true)
            ;
    }

    bindCore(20);

    for (int i = 0; i < thread_nr; ++i)
    {
        th[i] = std::thread(send_write, dsm->getMyNodeID(), i);
    }

    Timer timer;

    while (true)
    {
        timer.begin();

        sleep(1);

        auto ns = timer.end();

        uint64_t write_tp = 0;
        for (int i = 0; i < thread_nr; ++i)
        {
            write_tp += tp_write_counter[i][0];
            tp_write_counter[i][0] = 0;
        }

        double data_tp = write_tp * 1.0 / (1.0 * ns / 1000 / 1000 / 1000);
        printf("node %d, data tp %.2lf\n", dsm->getMyNodeID(), data_tp);
    }
}