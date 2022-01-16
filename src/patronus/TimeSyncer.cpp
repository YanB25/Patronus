#include "patronus/TimeSyncer.h"

#include "util/PerformanceReporter.h"

namespace patronus::time
{
using namespace define::literals;
using namespace std::chrono_literals;

TimeSyncer::TimeSyncer(DSM::pointer dsm,
                       GlobalAddress gaddr,
                       char *buffer,
                       size_t buf_size)
    : dsm_(dsm),
      target_gaddr_(gaddr),
      clock_info_(*(ClockInfo *) buffer),
      buffer_(buffer),
      buf_size_(buf_size)
{
    CHECK_LE(sizeof(ClockInfo), buf_size_);
}

void TimeSyncer::sync()
{
    auto nid = dsm_->get_node_id();
    auto parent_node = target_gaddr_.nodeID;

    time_exposer_ = std::thread([this]() {
        bindCore(1);
        clock_info_.adjustment = 0;
        clock_info_.magic = kMagic;
        while (!expose_finish_.load(std::memory_order_relaxed))
        {
            auto now = chrono_now();
            uint64_t ns = patronus::to_ns(now);
            clock_info_.ns = ns;
        }
    });

    if (unlikely(nid == parent_node))
    {
        DVLOG(1) << "[TimeSyncer] nid " << nid
                 << " equal to parent node. Will not try to sync clock.";
    }
    else
    {
        do_sync();
    }

    signal_finish();
    finish();

    expose_finish_ = true;
    time_exposer_.join();
}

void TimeSyncer::signal_finish()
{
    for (size_t nid = 0; nid < dsm_->getClusterSize(); ++nid)
    {
        if (nid == dsm_->get_node_id())
        {
            node_finished_[nid] = true;
            continue;
        }
        auto *buf = dsm_->get_rdma_buffer();
        auto &sync_finish_message =
            *(SyncFinishedMessage *) (buf + sizeof(SyncFinishedMessage) * nid);
        sync_finish_message.type = RequestType::kTimeSync;
        sync_finish_message.cid.coro_id = kNotACoro;
        sync_finish_message.cid.mid = kMid;
        sync_finish_message.cid.node_id = dsm_->get_node_id();
        sync_finish_message.cid.thread_id = dsm_->get_thread_id();
        dsm_->reliable_send((char *) &sync_finish_message,
                            sizeof(SyncFinishedMessage),
                            nid,
                            kMid);
    }
}

void TimeSyncer::do_sync()
{
    auto *rdma_buffer = dsm_->get_rdma_buffer();
    auto &parent_clock_info = *(ClockInfo *) rdma_buffer;
    DCHECK_LE(sizeof(ClockInfo), buf_size_);

    size_t continuous_converge_nr = 0;
    size_t sync_nr = 0;

    auto start_do_sync_time = std::chrono::steady_clock::now();

    OnePassMonitor adjustments;

    while (true)
    {
        dsm_->read_sync(rdma_buffer, target_gaddr_, sizeof(ClockInfo));
        auto parent_time = parent_clock_info.ns + parent_clock_info.adjustment;
        if (unlikely(parent_clock_info.magic == 0))
        {
            DVLOG(4) << "[TimeSyncer] magic == 0. node " << target_gaddr_.nodeID
                     << " not ready. ";
            std::this_thread::sleep_for(1ms);
            continue;
        }
        auto now = chrono_now();
        auto client_time = patronus::to_ns(now) + clock_info_.adjustment;

        int64_t this_adjustment = parent_time - client_time;
        clock_info_.adjustment += this_adjustment;

        auto after_client_time = patronus::to_ns(now) + clock_info_.adjustment;
        DVLOG(4) << "[TimeSyncer] adjust " << this_adjustment
                 << ", total_adjustment: " << clock_info_.adjustment
                 << ", parent_time: " << parent_time
                 << ", parent nid: " << target_gaddr_.nodeID
                 << ", before_client_time: " << client_time
                 << ", after client time: " << after_client_time;

        // only detect if we can converge after syncing for 500 ms
        auto sync_now = std::chrono::steady_clock::now();
        if (sync_now - start_do_sync_time >= 500ms)
        {
            adjustments.collect(this_adjustment);
            if (abs(this_adjustment) <= kSyncTimeBoundNs)
            {
                continuous_converge_nr++;
            }
            else
            {
                continuous_converge_nr = 0;
            }
            if (unlikely(continuous_converge_nr >= kRequiredContConvergeEpoch))
            {
                LOG(INFO) << "[TimeSyncer] node finished sycing time to "
                             "its parent. "
                          << clock_info_
                          << " last adjustments: " << adjustments;
                break;
            }
            sync_nr++;
            CHECK_LT(sync_nr, 100) << "Failed to sync within another 100ms";
        }
        CHECK_EQ(parent_clock_info.magic, kMagic);
        std::this_thread::sleep_for(1ms);
    }
}
void TimeSyncer::finish()
{
    char recv_buf[1024];
    DCHECK_LT(sizeof(SyncFinishedMessage), 1024);
    while (true)
    {
        auto nid = dsm_->get_node_id();

        bool can_exit = true;
        for (size_t i = 0; i < dsm_->getClusterSize(); ++i)
        {
            if (!node_finished_[i])
            {
                can_exit = false;
            }
        }
        if (can_exit)
        {
            return;
        }

        // can not exit, try to recv message
        if (dsm_->reliable_try_recv(kMid, recv_buf, 1))
        {
            const auto &sync_finished_message =
                *(SyncFinishedMessage *) recv_buf;
            DCHECK_EQ(sync_finished_message.type, RequestType::kTimeSync);
            auto target_nid = sync_finished_message.cid.node_id;
            DCHECK_NE(target_nid, nid);
            node_finished_[target_nid] = true;
        }
    }
}

}  // namespace patronus::time