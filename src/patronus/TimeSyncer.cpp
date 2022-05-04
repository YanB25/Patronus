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
    node_finished_.fill(false);
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
            uint64_t ns = to_ns(now);
            clock_info_.ns = ns;
        }
    });

    if (unlikely(nid == parent_node))
    {
        LOG(INFO) << "[TimeSyncer] nid " << nid
                  << " equal to parent node. Will not try to sync clock.";
    }
    else
    {
        do_sync();
    }

    signal_finish();
    wait_finish();

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
        auto rdma_buffer = dsm_->get_rdma_buffer();
        auto *buf = rdma_buffer.buffer;
        DCHECK_LT(sizeof(SyncFinishedMessage) * dsm_->getClusterSize(),
                  rdma_buffer.size);
        auto &sync_finish_message =
            *(SyncFinishedMessage *) (buf + sizeof(SyncFinishedMessage) * nid);
        sync_finish_message.type = RequestType::kTimeSync;
        sync_finish_message.cid.coro_id = kNotACoro;
        sync_finish_message.cid.node_id = dsm_->get_node_id();
        sync_finish_message.cid.thread_id = dsm_->get_thread_id();
        sync_finish_message.self_epsilon = clock_info_.self_epsilon;
        dsm_->unreliable_send((char *) &sync_finish_message,
                              sizeof(SyncFinishedMessage),
                              nid,
                              kDirId);
    }
}

void TimeSyncer::do_sync()
{
    auto rdma_buffer_ = dsm_->get_rdma_buffer();
    auto *rdma_buffer = rdma_buffer_.buffer;
    auto &parent_clock_info = *(ClockInfo *) rdma_buffer;
    DCHECK_LE(sizeof(ClockInfo), buf_size_);
    DCHECK_LT(sizeof(ClockInfo), rdma_buffer_.size);

    size_t continuous_converge_nr = 0;
    size_t sync_nr = 0;

    auto start_do_sync_time = std::chrono::steady_clock::now();

    OnePassMonitor adjustments;
    OnePassMonitor read_sync_latency_ns;

    while (true)
    {
        auto before_read_sync = std::chrono::steady_clock::now();
        dsm_->read_sync(rdma_buffer, target_gaddr_, sizeof(ClockInfo));
        auto end_read_sync = std::chrono::steady_clock::now();
        auto read_sync_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                end_read_sync - before_read_sync)
                .count();

        auto parent_time = parent_clock_info.ns + parent_clock_info.adjustment;
        // we estimate the actual time of server as: time + 1/2 * latency
        auto actual_parent_time = parent_time + (read_sync_ns / 2);
        if (unlikely(parent_clock_info.magic == 0))
        {
            DVLOG(5) << "[TimeSyncer] magic == 0. node " << target_gaddr_.nodeID
                     << " not ready. ";
            std::this_thread::sleep_for(1ms);
            continue;
        }
        auto now = chrono_now();
        auto client_time =
            to_ns(now) + clock_info_.adjustment.load(std::memory_order_relaxed);

        int64_t this_adjustment = actual_parent_time - client_time;
        clock_info_.adjustment += this_adjustment;

        auto after_client_time =
            to_ns(now) + clock_info_.adjustment.load(std::memory_order_relaxed);
        DVLOG(5) << "[TimeSyncer] adjust " << this_adjustment
                 << ", total_adjustment: " << clock_info_.adjustment
                 << ", parent_time: " << parent_time
                 << ", estimated actual parent_time: " << actual_parent_time
                 << ", parent nid: " << target_gaddr_.nodeID
                 << ", before_client_time: " << client_time
                 << ", after client time: " << after_client_time
                 << ", detail: " << parent_clock_info;

        // only detect if we can converge after syncing for 500 ms
        auto sync_now = std::chrono::steady_clock::now();
        if (sync_now - start_do_sync_time >= 500ms)
        {
            adjustments.collect(this_adjustment);
            read_sync_latency_ns.collect(read_sync_ns);
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
                // take @latency / 2 as the epsilon
                clock_info_.self_epsilon = read_sync_latency_ns.max() / 2;
                LOG(INFO) << "[TimeSyncer] node finished syncing time to "
                             "its parent. "
                          << clock_info_ << " last adjustments: " << adjustments
                          << " communication latency: " << read_sync_latency_ns;
                break;
            }
            sync_nr++;
            CHECK_LT(sync_nr, 100) << "Failed to sync within another 100ms";
        }
        CHECK_EQ(parent_clock_info.magic, kMagic);
        std::this_thread::sleep_for(1ms);
    }
}
// TODO(TimeSyncer): currently, the time syncing is a Tree-like procedure: each
// node sync to its parent.
// However, the estimation of @epsilon is a broadcast
// precedure, which may cause problem.
// If the tree has multiple layers, the g_epsilon should be *added*, not *maxed*
// Not fix now, since for a small cluster, there will be only one node as
// the gobal parent
void TimeSyncer::wait_finish()
{
    char recv_buf[1024];
    DCHECK_LT(sizeof(SyncFinishedMessage), 1024);

    OnePassMonitor epsilons;
    epsilons.collect(clock_info_.self_epsilon);

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
        // can not exit, try to recv message
        if (dsm_->unreliable_try_recv(recv_buf, 1))
        {
            const auto &sync_finished_message =
                *(SyncFinishedMessage *) recv_buf;
            DCHECK_EQ(sync_finished_message.type, RequestType::kTimeSync);
            auto target_nid = sync_finished_message.cid.node_id;
            DCHECK_NE(target_nid, nid);
            auto target_epsilon = sync_finished_message.self_epsilon;
            epsilons.collect(target_epsilon);
            node_finished_[target_nid] = true;
            // LOG(INFO) << "debug: recv message " << sync_finished_message;
        }

        if (can_exit)
        {
            break;
        }
    }
    // TODO(patronus): add 500 ns just to be safe.
    global_epsilon_ = epsilons.max();
    LOG(INFO) << "[TimeSyncer] set g_epsilon to " << global_epsilon_
              << " according to " << epsilons;
    ready_ = true;
}

}  // namespace patronus::time