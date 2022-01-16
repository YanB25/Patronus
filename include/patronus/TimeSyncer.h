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
    // be atomic, because don't want to cache them in registers
    // could be volatile, but I think atomic is fine.
    std::atomic<int64_t> ns;
    std::atomic<int64_t> adjustment;
    std::atomic<uint64_t> self_epsilon;
    std::atomic<uint64_t> magic;
};
inline std::ostream &operator<<(std::ostream &os, const ClockInfo &clock)
{
    os << "{ClockInfo adjustment: "
       << clock.adjustment.load(std::memory_order_relaxed)
       << ", ns: " << clock.ns.load(std::memory_order_relaxed)
       << ", self_epsilon: "
       << clock.self_epsilon.load(std::memory_order_relaxed)
       << ", magic: " << clock.magic.load(std::memory_order_relaxed) << "}";
    return os;
}

struct SyncFinishedMessage
{
    enum RequestType type;
    ClientID cid;
    int64_t self_epsilon;
};
inline std::ostream &operator<<(std::ostream &os,
                                const SyncFinishedMessage &msg)
{
    CHECK_EQ(msg.type, RequestType::kTimeSync);
    os << "{SyncFinishedMessage cid: " << msg.cid
       << ", self_epsilon: " << msg.self_epsilon << "}";
    return os;
}
using namespace define::literals;

/**
 * @brief PatronusTime is the adjusted time by TimeSyncer. Considered the time
 * drift in the cluster.
 *
 */
class PatronusTime
{
public:
    PatronusTime(uint64_t adjusted_ns) : adjusted_ns_(adjusted_ns)
    {
    }
    uint64_t ns() const
    {
        return adjusted_ns_;
    }

private:
    uint64_t adjusted_ns_{0};
};
class TimeSyncer
{
    constexpr static size_t kMagic = 0xaabbccdd10103232;
    constexpr static size_t kRequiredContConvergeEpoch = 5;
    constexpr static int64_t kSyncTimeBoundNs = 1_K;  // 1us
    constexpr static uint64_t kMid = 0;

public:
    using ns_t = int64_t;
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

    template <typename T>
    PatronusTime to_patronus_time(const T &t) const
    {
        DCHECK(ready_);
        auto raw_ns = to_ns(t);
        auto adjusted_ns =
            raw_ns + clock_info_.adjustment.load(std::memory_order_relaxed);
        return PatronusTime(adjusted_ns);
    }
    static PatronusTime to_patronus_time(const PatronusTime &t)
    {
        return t;
    }
    static ns_t to_ns(
        const std::chrono::time_point<std::chrono::steady_clock> &t)
    {
        auto raw = std::chrono::duration_cast<std::chrono::nanoseconds>(
                       t.time_since_epoch())
                       .count();
        return raw;
    }
    static ns_t to_ns(
        const std::chrono::time_point<std::chrono::system_clock> &t)
    {
        auto raw = std::chrono::duration_cast<std::chrono::nanoseconds>(
                       t.time_since_epoch())
                       .count();
        return raw;
    }
    static ns_t to_ns(ns_t ns)
    {
        return ns;
    }

    ns_t epsilon() const
    {
        DCHECK(ready_);
        DCHECK_GT(global_epsilon_, 0);
        return global_epsilon_;
    }

    /**
     * Does not want to define to_ns for @PatronusTime
     * Because the @to_ns means to the *unadjusted* ns.
     * It makes no sense to PatronusTime, because it is already adjusted
     */
    bool definitely_lt(PatronusTime lhs, PatronusTime rhs) const
    {
        return definitely_lt(lhs.ns(), rhs.ns());
    }
    bool definitely_gt(PatronusTime lhs, PatronusTime rhs) const
    {
        return definitely_gt(lhs.ns(), rhs.ns());
    }
    bool may_eq(PatronusTime lhs, PatronusTime rhs) const
    {
        return may_eq(lhs.ns(), rhs.ns());
    }
    template <typename T, typename U>
    bool definitely_lt(const T &lhs, const U &rhs) const
    {
        DCHECK(ready_);
        ns_t lhs_ns = to_ns(lhs);
        ns_t rhs_ns = to_ns(rhs);
        return (lhs_ns + epsilon()) < rhs_ns;
    }
    template <typename T, typename U>
    bool definitely_gt(const T &lhs, const U &rhs) const
    {
        DCHECK(ready_);
        ns_t lhs_ns = to_ns(lhs);
        ns_t rhs_ns = to_ns(rhs);
        return lhs_ns > (rhs_ns + epsilon());
    }
    template <typename T, typename U>
    bool may_eq(const T &lhs, const U &rhs) const
    {
        DCHECK(ready_);
        return !definitely_lt(lhs, rhs) && !definitely_gt(lhs, rhs);
    }

    static std::chrono::time_point<std::chrono::system_clock> chrono_now()
    {
        return std::chrono::system_clock::now();
    }

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

    void wait_finish();

    bool ready_{false};

    DSM::pointer dsm_;
    GlobalAddress target_gaddr_;

    // for clock_info
    ClockInfo &clock_info_;
    [[maybe_unused]] char *buffer_ { nullptr };
    size_t buf_size_{0};

    std::atomic<bool> expose_finish_{false};
    std::thread time_exposer_;

    std::array<bool, MAX_MACHINE> node_finished_;
    uint64_t global_epsilon_{0};
};
}  // namespace patronus::time
#endif