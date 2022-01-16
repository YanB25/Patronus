#ifndef PATRONUS_TIME_H_
#define PATRONUS_TIME_H_

#include <chrono>

namespace patronus::time
{
using patronus_time_t = uint64_t;

inline std::chrono::time_point<std::chrono::system_clock> ns_to_system_clock(
    uint64_t ns)
{
    std::chrono::time_point<std::chrono::system_clock> ret{};
    return ret + std::chrono::nanoseconds(ns);
}
/**
 * @brief note this can not be called across nodes without adjustment
 *
 * @return std::chrono::time_point<std::chrono::steady_clock>
 */
inline std::chrono::time_point<std::chrono::steady_clock> ns_to_steady_clock(
    uint64_t ns)
{
    std::chrono::time_point<std::chrono::steady_clock> ret{};
    return ret + std::chrono::nanoseconds(ns);
}
inline uint64_t to_ns(
    const std::chrono::time_point<std::chrono::system_clock> &time)
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               time.time_since_epoch())
        .count();
}
inline uint64_t to_ns(
    const std::chrono::time_point<std::chrono::steady_clock> &time)
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               time.time_since_epoch())
        .count();
}

}  // namespace patronus::time
#endif