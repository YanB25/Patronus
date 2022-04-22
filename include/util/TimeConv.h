#pragma once
#ifndef SHERMAM_TIMECONV_H_
#define SHERMAM_TIMECONV_H_

#include <chrono>

namespace util::time
{
template <typename T>
constexpr uint64_t to_ns(T time)
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(time).count();
}
template <typename T>
constexpr uint64_t to_us(T time)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(time).count();
}
template <typename T>
constexpr uint64_t to_ms(T time)
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(time).count();
}
template <typename T>
constexpr uint64_t to_second(T time)
{
    return std::chrono::duration_cast<std::chrono::seconds>(time).count();
}

template <typename T>
inline void busy_wait_for(T time)
{
    auto start = std::chrono::steady_clock::now();
    uint64_t wait_ns = to_ns(time);
    while (true)
    {
        auto now = std::chrono::steady_clock::now();
        uint64_t elaps_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(now - start)
                .count();
        if (unlikely(elaps_ns >= wait_ns))
        {
            return;
        }
    }
}

template <typename T>
inline void busy_wait_until(
    std::chrono::time_point<std::chrono::steady_clock> &cur, T time)
{
    uint64_t wait_ns = to_ns(time);
    while (true)
    {
        auto now = std::chrono::steady_clock::now();
        uint64_t elaps_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(now - cur)
                .count();
        if (unlikely(elaps_ns >= wait_ns))
        {
            cur = now;
            return;
        }
    }
}

}  // namespace util::time

#endif