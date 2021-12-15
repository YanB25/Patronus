#include "ClockManager.h"

#include <glog/logging.h>

#include "DSM.h"
ClockManager::ClockManager(DSM *dsm) : dsm_(dsm)
{
    LOG(FATAL)
        << "I figure out that the design with rdtsc does not work, because CPU "
           "has different frequency. Don't want to walk around. Maybe I just "
           "can use std::chrono.";
}
void ClockManager::init_sync_clock()
{
    if (dsm_->get_node_id() == 0)
    {
        primary_init_sync_clock();
    }
    else
    {
        backup_init_sync_clock();
    }
}

void ClockManager::sync_clock()
{
    if (dsm_->get_node_id() == 0)
    {
        primary_sync_clock();
    }
    else
    {
        backup_sync_clock();
    }
}

/**
 * @brief basically is Berkeley algorithm
 */
void ClockManager::primary_init_sync_clock()
{
    DLOG(INFO) << "[clock] primary init sync clock...";
    auto dsm = dsm_;

    uint64_t max_clock = clock();
    for (size_t i = 1; i < dsm->getClusterSize(); ++i)
    {
        DLOG(INFO) << "[clock] RECV that clock";
        auto that_clock = *(uint64_t *) dsm->recv();
        max_clock = std::max(max_clock, that_clock);
    }
    set_clock(max_clock);
    for (size_t i = 1; i < dsm->getClusterSize(); ++i)
    {
        DLOG(INFO) << "[clock] SEND max clock to " << i;
        dsm->send((char *) &max_clock, sizeof(max_clock), i);
    }
    DLOG(INFO) << "[clock] primary init sync clock finished.";
}

void ClockManager::backup_init_sync_clock()
{
    DLOG(INFO) << "[clock] backup init sync clock...";
    auto dsm = dsm_;

    auto my_clock = clock();
    // dinfo("[clock] SEND backup init sync clock...");
    dsm->send((char *) &my_clock, sizeof(my_clock), 0);

    // dinfo("[clock] RECV backup get set clock...");
    auto get_set_clock = *(uint64_t *) dsm->recv();
    set_clock(get_set_clock);
    // dinfo("[clock] backup init sync clock finished");
}

void ClockManager::primary_sync_clock()
{
    DLOG(INFO) << "[clock] primary start to sync clock...";
    auto dsm = dsm_;

    std::unordered_map<int, uint64_t> node_rtt;
    std::unordered_map<int, uint64_t> node_clock;

    // measure RTT
    for (size_t i = 1; i < dsm->getClusterSize(); ++i)
    {
        auto begin_rdtsc = rdtsc();
        DLOG(INFO) << "[clock] SEND measuring RTT with node" << i;
        dsm->send(nullptr, 0, i, 0, true);
        auto end_rdtsc = rdtsc();

        node_rtt[i] = end_rdtsc - begin_rdtsc;
    }

    for (const auto &[node_id, rtt] : node_rtt)
    {
        LOG(INFO) << "[clock] node: " << node_id << ", rtt: " << rtt;
    }

    // sync clock
    node_clock[dsm->get_node_id()] = clock();

    for (size_t i = 1; i < dsm->getClusterSize(); ++i)
    {
        // dinfo("[clock] SEND triggering clock with node %zu", i);
        dsm->send(nullptr, 0, i);
    }

    for (size_t i = 1; i < dsm->getClusterSize(); ++i)
    {
        // dinfo("[clock] RECV getting cur clock with node %zu", i);
        auto sync_time_msg = *(SyncTimeMessage *) dsm->recv();
        uint64_t half_rtt = node_rtt[i] / 2;
        node_clock[sync_time_msg.node_id] = sync_time_msg.clock - half_rtt;
    }

    // calculate average
    double sum = 0;
    for (const auto &[node_id, get_clock] : node_clock)
    {
        sum += get_clock;
    }
    double avg = sum / node_clock.size();
    DLOG(INFO) << "[clock] get avg clock: " << avg;

    for (size_t i = 1; i < dsm->getClusterSize(); ++i)
    {
        double off = avg - node_clock[i];
        DLOG(INFO) << "[clock] node: " << i << " clock: " << node_clock[i]
                   << ", avg: " << avg << ", offset: " << off;
        dsm->send((char *) &off, sizeof(off), i, 0, true);
    }
    double my_off = avg - node_clock[dsm->get_node_id()];
    set_offset(my_off);
    DLOG(INFO) << "[clock] sync finished";
}

void ClockManager::backup_sync_clock()
{
    auto dsm = dsm_;

    // for measure RTT
    // dinfo("[clock] RECV wait sync 1...");
    dsm->recv();

    // for sync clock
    // dinfo("[clock] RECV wait sync 2...");
    dsm->recv();

    SyncTimeMessage msg;
    msg.node_id = dsm->get_node_id();
    msg.clock = clock();
    // dinfo("[clock] SEND sending cur clock...");
    dsm->send((char *) &msg, sizeof(msg), 0);

    // dinfo("[clock] RECV getting off...");
    double off = *(double *) dsm->recv();
    set_offset(off);
    DLOG(INFO) << "[clock] sync finished.";
}