#ifndef PATRONUS_TIME_H_
#define PATRONUS_TIME_H_

#include <chrono>

namespace patronus
{
std::chrono::time_point<std::chrono::system_clock> ns_to_system_clock(
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
std::chrono::time_point<std::chrono::steady_clock> ns_to_steady_clock(
    uint64_t ns)
{
    std::chrono::time_point<std::chrono::steady_clock> ret{};
    return ret + std::chrono::nanoseconds(ns);
}
uint64_t to_ns(const std::chrono::time_point<std::chrono::system_clock> &time)
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               time.time_since_epoch())
        .count();
}
uint64_t to_ns(const std::chrono::time_point<std::chrono::steady_clock> &time)
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               time.time_since_epoch())
        .count();
}

}  // namespace patronus
#endif