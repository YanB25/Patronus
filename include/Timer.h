#pragma once
#if !defined(_TIMER_H_)
#define _TIMER_H_

#include <time.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <list>
#include <map>
#include <string>
#include <unordered_map>

#include "Common.h"
#include "boost/format.hpp"

#define DEFINE_VARNAME(base, line) DEFINE_VARNAME_CONCAT(base, line)
#define DEFINE_VARNAME_CONCAT(base, line) base##line
#define VAR_OCCURENCE DEFINE_VARNAME(__occurence, __LINE__)
#define VAR_MAGIC_ENABLED DEFINE_VARNAME(__enabled, __LINE__)

#define ENABLED_FIRST_N(enabled, n)               \
    do                                            \
    {                                             \
        static std::atomic<int> VAR_OCCURENCE{0}; \
        int __entered_time = VAR_OCCURENCE++;     \
        if (__entered_time < n)                   \
        {                                         \
            enabled = true;                       \
        }                                         \
        else                                      \
        {                                         \
            enabled = false;                      \
        }                                         \
    } while (0)

#define ENABLED_ONCE(enabled) ENABLED_FIRST_N(enabled, 1)

class Timer
{
public:
    Timer() = default;

    void begin()
    {
        clock_gettime(CLOCK_REALTIME, &s);
    }

    uint64_t end(uint64_t loop = 1)
    {
        this->loop = loop;
        clock_gettime(CLOCK_REALTIME, &e);
        uint64_t ns_all =
            (e.tv_sec - s.tv_sec) * 1000000000ull + (e.tv_nsec - s.tv_nsec);
        ns = ns_all / loop;

        return ns;
    }

    void print()
    {
        if (ns < 1000)
        {
            printf("%ld ns per loop\n", ns);
        }
        else
        {
            printf("%lf us per loop\n", ns * 1.0 / 1000);
        }
    }

    static uint64_t get_time_ns()
    {
        timespec now;
        clock_gettime(CLOCK_REALTIME, &now);
        return 1000000000ull * now.tv_sec + now.tv_nsec;
    }

    static void sleep(uint64_t sleep_ns)
    {
        Timer clock;

        clock.begin();
        while (true)
        {
            if (clock.end() >= sleep_ns)
            {
                return;
            }
        }
    }

    void end_print(uint64_t loop = 1)
    {
        end(loop);
        print();
    }

private:
    timespec s, e;
    uint64_t loop;
    uint64_t ns;
};

template <bool kEnabled = true>
class ContTimer;

template <>
class ContTimer<true>
{
public:
    ContTimer(const std::string &name, const std::string &step = "Start");
    ContTimer();
    void init(const std::string &name, const std::string &first_step = "Start");
    void pin(const std::string this_step);
    ContTimer operator=(const ContTimer &) = delete;
    ContTimer(const ContTimer &) = delete;

    void report(std::ostream & = std::cout) const;
    ~ContTimer();
    friend inline std::ostream &operator<<(std::ostream &os,
                                           const ContTimer<true> &t);
    void clear()
    {
        inited_ = false;
        event_ns_.clear();
    }

private:
    bool inited_{false};
    std::string name_;
    std::string step_;
    std::chrono::time_point<std::chrono::steady_clock> start_;
    std::chrono::time_point<std::chrono::steady_clock> pin_;
    std::unordered_map<std::string, uint64_t> event_ns_;
};

std::ostream &operator<<(std::ostream &os, const ContTimer<true> &t)
{
    size_t total_ns = 0;
    for (const auto &[event, ns] : t.event_ns_)
    {
        std::ignore = event;
        total_ns += ns;
    }

    auto fmt = boost::format("[%1%]: *summary* takes %2% ns ( %3% ms)\n") %
               t.name_ % total_ns % (total_ns / 1e6);
    auto str = boost::str(fmt);
    for (const auto &[event, ns] : t.event_ns_)
    {
        fmt = boost::format("%1% %% [%2%] takes %3% ns (%4% ms)\n") %
              (100.0f * ns / total_ns) % event % ns % (ns / 1e6);
        str += boost::str(fmt);
    }
    os << str << std::endl;

    return os;
}

template <>
class ContTimer<false>
{
public:
    ContTimer(const std::string &, const std::string & = "")
    {
    }
    ContTimer()
    {
    }
    void init(const std::string &, const std::string & = "")
    {
    }
    void pin(const std::string)
    {
    }
    ContTimer operator=(const ContTimer &) = delete;
    ContTimer(const ContTimer &) = delete;
    ~ContTimer()
    {
    }
    void report(std::ostream & = std::cout) const
    {
    }
    friend inline std::ostream &operator<<(std::ostream &os,
                                           const ContTimer<false> &t);
    void clear()
    {
    }

private:
};

std::ostream &operator<<(std::ostream &os, const ContTimer<false> &)
{
    return os;
}

template <bool Enabled>
class OnceContTimer;

#define DefOnceContTimer(name, enabled, msg) \
    bool VAR_MAGIC_ENABLED = false;          \
    ENABLED_ONCE(VAR_MAGIC_ENABLED);         \
    OnceContTimer<enabled> name(msg, VAR_MAGIC_ENABLED)

template <bool Enabled>
class OnceContTimer
{
public:
    OnceContTimer(const std::string &name, bool enabled)
        : timer_(name, "Start"), enabled_(enabled)
    {
    }
    void pin(const std::string this_step)
    {
        if (enabled_)
        {
            timer_.pin(this_step);
        }
    }
    OnceContTimer operator=(const OnceContTimer &) = delete;
    OnceContTimer(const OnceContTimer &) = delete;

    void report(std::ostream &os = std::cout) const
    {
        if (enabled_)
        {
            timer_.report(os);
        }
    }
    ~OnceContTimer() = default;

private:
    ContTimer<Enabled> timer_;
    bool enabled_;
};

class ChronoTimer
{
public:
    ChronoTimer() : now_(std::chrono::steady_clock::now())
    {
    }

    uint64_t pin()
    {
        auto now = std::chrono::steady_clock::now();
        auto diff = now - now_;
        auto ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(diff).count();
        now_ = now;
        return ns;
    }

private:
    std::chrono::time_point<std::chrono::steady_clock> now_;
};

struct RetrieveTimerRecord
{
    RetrieveTimerRecord(const std::string &n, uint64_t t) : name(n), ns(t)
    {
    }
    std::string name;
    uint64_t ns;
};
class RetrieveTimer
{
public:
    RetrieveTimer() = default;
    void clear()
    {
        records_.clear();
    }
    void init(const std::string &name)
    {
        records_.clear();
        name_ = name;
        now_ = std::chrono::steady_clock::now();
    }
    uint64_t pin(const std::string &name)
    {
        auto now = std::chrono::steady_clock::now();
        auto ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(now - now_)
                .count();
        now_ = now;
        records_.emplace_back(name, ns);
        return ns;
    }
    /**
     * @brief This will not combined pin(s) with same name
     *
     * @return std::vector<RetrieveTimerRecord>
     */
    std::vector<RetrieveTimerRecord> retrieve_vec() const
    {
        std::vector<RetrieveTimerRecord> ret;
        ret.reserve(records_.size());
        for (const auto &record : records_)
        {
            ret.push_back(record);
        }
        return ret;
    }
    /**
     * @brief This will auto-ly combine pin(s) with same name
     *
     * @return std::map<std::string, uint64_t>
     */
    std::map<std::string, uint64_t> retrieve_map() const
    {
        std::map<std::string, uint64_t> ret;
        for (const auto &record : records_)
        {
            ret[record.name] += record.ns;
        }
        return ret;
    }

private:
    std::string name_;
    std::list<RetrieveTimerRecord> records_;
    std::chrono::time_point<std::chrono::steady_clock> now_;
};

inline std::ostream &operator<<(std::ostream &os, const RetrieveTimer &timer)
{
    os << "{RetrieveTimer " << std::endl;
    for (const auto &record : timer.retrieve_vec())
    {
        os << "[" << record.name << "] takes " << record.ns << " ns"
           << std::endl;
    }
    os << "}";
    return os;
}

#endif  // _TIMER_H_
