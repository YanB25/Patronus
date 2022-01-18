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

class TimeSyncer
{
    constexpr static size_t kMagic = 0xaabbccdd10103232;
    constexpr static size_t kRequiredContConvergeEpoch = 5;
    constexpr static int64_t kSyncTimeBoundNs = 1_K;  // 1us
    constexpr static uint64_t kMid = 0;

public:
    using pointer = std::unique_ptr<TimeSyncer>;
    TimeSyncer(DSM::pointer dsm,
               GlobalAddress gaddr,
               char *buffer,
               size_t buf_size);
    TimeSyncer(const TimeSyncer &) = delete;
    TimeSyncer &operator=(const TimeSyncer &) = delete;

    static pointer new_instance(DSM::pointer dsm,
                                GlobalAddress gaddr,
                                char *buffer,
                                size_t buf_size)
    {
        return std::make_unique<TimeSyncer>(dsm, gaddr, buffer, buf_size);
    }
    /**
     * @brief When return, the TimeSyncer is ready to provide synced time
     * service
     *
     */
    void sync();

    static std::chrono::time_point<std::chrono::system_clock>
    ns_to_system_clock(uint64_t ns)
    {
        std::chrono::time_point<std::chrono::system_clock> ret{};
        return ret + std::chrono::nanoseconds(ns);
    }
    /**
     * @brief note this can not be called across nodes without adjustment
     *
     * @return std::chrono::time_point<std::chrono::steady_clock>
     */
    static std::chrono::time_point<std::chrono::steady_clock>
    ns_to_steady_clock(uint64_t ns)
    {
        std::chrono::time_point<std::chrono::steady_clock> ret{};
        return ret + std::chrono::nanoseconds(ns);
    }

    template <typename T>
    PatronusTime to_patronus_time(const T &t) const
    {
        DCHECK(ready_);
        auto raw_ns = to_ns(t);
        return PatronusTime(
            raw_ns, clock_info_.adjustment.load(std::memory_order_relaxed));
    }
    PatronusTime patronus_later(ns_t ns) const
    {
        DCHECK(ready_);
        auto later = chrono_now() + std::chrono::nanoseconds(ns);
        return to_patronus_time(later);
    }
    PatronusTime patronus_now() const
    {
        DCHECK(ready_);
        auto now = chrono_now();
        return to_patronus_time(now);
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
    static std::chrono::time_point<std::chrono::system_clock> to_chrono(ns_t ns)
    {
        std::chrono::time_point<std::chrono::system_clock> ret{};
        return ret + std::chrono::nanoseconds(ns);
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
        return definitely_lt(lhs.term(), rhs.term());
    }
    bool definitely_gt(PatronusTime lhs, PatronusTime rhs) const
    {
        return definitely_gt(lhs.term(), rhs.term());
    }
    bool may_eq(PatronusTime lhs, PatronusTime rhs) const
    {
        return may_eq(lhs.term(), rhs.term());
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
    static std::chrono::time_point<std::chrono::system_clock> chrono_later(
        uint64_t ns)
    {
        return std::chrono::system_clock::now() + std::chrono::nanoseconds(ns);
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