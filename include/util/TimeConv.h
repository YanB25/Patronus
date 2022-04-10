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
}  // namespace util::time

#endif