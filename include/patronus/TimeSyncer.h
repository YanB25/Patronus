#pragma once
#ifndef PATRONUS_TIME_SYNCER_H
#define PATRONUS_TIME_SYNCER_H

#include <atomic>
#include <thread>

#include "Common.h"
#include "DSM.h"
#include "patronus/Time.h"
#include "patronus/Type.h"

namespace patronus::time
{
struct ClockInfo
{
    std::atomic<int64_t> ns;
    std::atomic<int64_t> adjustment;
    std::atomic<uint64_t> magic;
};
inline std::ostream &operator<<(std::ostream &os, const ClockInfo &clock)
{
    os << "{ClockInfo adjustment: " << clock.adjustment << ", ns: " << clock.ns
       << "}";
    return os;
}

struct SyncFinishedMessage
{
    enum RequestType type;
    ClientID cid;
};
using namespace define::literals;
class TimeSyncer
{
    constexpr static size_t kMagic = 0xaabbccdd10103232;
    constexpr static size_t kRequiredContConvergeEpoch = 5;
    constexpr static int64_t kSyncTimeBoundNs = 1_K;  // 1us
    constexpr static uint64_t kMid = 0;

public:
    TimeSyncer(DSM::pointer dsm,
               GlobalAddress gaddr,
               char *buffer,
               size_t buf_size);
    /**
     * @brief When return, the TimeSyncer is ready to provide synced time
     * service
     *
     */
    void sync();

private:
    /**
     * @brief Signal to the cluster that this node finishes its syncing
     *
     */
    void signal_finish();
    /**
     * @brief Iteratively set the @clock_info.adjustment so that clock is
     * synced.
     *
     */
    void do_sync();
    std::chrono::time_point<std::chrono::system_clock> chrono_now()
    {
        return std::chrono::system_clock::now();
    }

    void finish();

    DSM::pointer dsm_;
    GlobalAddress target_gaddr_;

    // for clock_info
    ClockInfo &clock_info_;
    [[maybe_unused]] char *buffer_ { nullptr };
    size_t buf_size_{0};

    std::atomic<bool> expose_finish_;
    std::thread time_exposer_;

    std::array<bool, MAX_MACHINE> node_finished_;
};
}  // namespace patronus::time
#endif