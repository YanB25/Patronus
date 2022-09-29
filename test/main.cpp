#include <iostream>
#include <thread>
#include <unordered_map>

#include "DSM.h"
#include "gflags/gflags.h"
#include "patronus/Patronus.h"
#include "util/TimeConv.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

[[maybe_unused]] constexpr static size_t kClientNid = 0;
[[maybe_unused]] constexpr static size_t kServerNid = 1;

constexpr static uint64_t kMagic = 0xaabb1122ccdd5566;

void run(DSM::pointer dsm)
{
    auto node_id = dsm->get_node_id();
    if (node_id == 0)
    {
        auto *buffer = dsm->get_rdma_buffer().buffer;
        GlobalAddress gaddr;
        gaddr.nodeID = kServerNid;
        gaddr.offset = 0;
        memcpy(buffer, &kMagic, sizeof(uint64_t));

        // ChronoTimer timer;
        // dsm->write_sync(buffer, gaddr, sizeof(uint64_t));
        // auto ns = timer.pin();
        // LOG(INFO) << "It takes " << util::pre_ns(ns);

        // dsm->write_sync(buffer, gaddr, sizeof(uint64_t));
        // ns = timer.pin();
        // LOG(INFO) << "It takes " << util::pre_ns(ns) << " again";
        auto min = util::time::to_ns(0ns);
        auto max = util::time::to_ns(100us);
        auto rng = util::time::to_ns(10ns);
        OnePassBucketMonitor lat_m(min, max, rng);
        Sequence<uint64_t> seq;

        for (size_t i = 0; i < 20; ++i)
        {
            ChronoTimer timer;
            dsm->write_sync(buffer, gaddr, sizeof(uint64_t));
            auto ns = timer.pin();
            lat_m.collect(ns);
            seq.push_back(ns);
        }

        LOG(INFO) << "[lat_m] " << lat_m;
        LOG(INFO) << "detail: " << util::pre_vec(seq.to_vector(), 10);

        ChronoTimer timer;
        auto illegal_gaddr = gaddr;
        illegal_gaddr.offset = 1024_GB;
        dsm->write(buffer, illegal_gaddr, sizeof(uint64_t));
        auto post_ns = timer.pin();
        uint64_t poll_ns = 0;

        auto *cq = dsm->get_icon_cq();
        ibv_wc wc;
        while (true)
        {
            auto ret = ibv_poll_cq(cq, 1, &wc);
            PLOG_IF(FATAL, ret < 0) << "ibv_poll_cq failed.";
            if (ret)
            {
                poll_ns = timer.pin();
                if (wc.status == IBV_WC_SUCCESS)
                {
                    LOG(FATAL) << "** Illegal access succeeded.";
                }
                else
                {
                    PLOG(ERROR)
                        << "[wc] Failed status " << ibv_wc_status_str(wc.status)
                        << " (" << wc.status << ") for wr_id " << WRID(wc.wr_id)
                        << " at QP: " << wc.qp_num
                        << ". vendor err: " << wc.vendor_err;
                }
                break;
            }
        }

        LOG(INFO) << "Illegal post takes " << util::pre_ns(post_ns)
                  << ", poll takes " << util::pre_ns(poll_ns);
    }
    else
    {
        auto *buffer = (char *) dsm->get_server_buffer().buffer;
        uint64_t read = 0;
        while (read != kMagic)
        {
            LOG(INFO) << "Got " << (void *) read;
            memcpy(&read, buffer, sizeof(uint64_t));
            std::this_thread::sleep_for(1s);
        }
        LOG(INFO) << "OK. got " << (void *) read;
    }
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    DSMConfig config;
    config.machineNR = ::config::kMachineNr;
    auto dsm = DSM::getInstance(config);
    dsm->registerThread();

    std::this_thread::sleep_for(1s);
    dsm->keeper_barrier("start", 10us);

    auto node_id = dsm->get_node_id();
    if (node_id == 0 || node_id == 1)
    {
        run(dsm);
    }

    dsm->keeper_barrier("finished", 10ms);

    LOG(INFO) << "Finished.";
    return 0;
}