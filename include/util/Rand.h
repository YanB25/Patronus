#pragma once
#ifndef SHERMAM_UTIL_FAST_RAND_H_
#define SHERMAM_UTIL_FAST_RAND_H_

#include <random>

#include "glog/logging.h"

// Notice: TLS object is created only once for each combination of type and
// thread. Only use this when you prefer multiple callers share the same
// instance.
template <class T, class... Args>
inline T &TLS(Args &&... args)
{
    thread_local T _tls_item(std::forward<Args>(args)...);
    return _tls_item;
}

/**
 * @brief slower and has greater state storage, but with the right parameters
 * has the longest non-repeating sequence with the most desirable spectral
 * characteristics
 *
 * @return std::mt19937&
 */
inline std::mt19937 &mt19937_generator()
{
    return TLS<std::mt19937>();
}

/**
 * @brief very fast, at the expense of greater state storage and sometimes less
 * desirable spectral characteristics
 *
 * @return std::ranlux48_base&
 */
inline std::ranlux48_base &ranlux48_base_generator()
{
    return TLS<std::ranlux48_base>();
}

/**
 * @brief non-deterministic uniform random bit generator, although
 * implemntations are allowed to implement using a pseudo-random number engine
 * if non-deterministic ones are not supported.
 *
 * @return std::random_device&
 */
inline std::random_device &random_device_generator()
{
    return TLS<std::random_device>();
}

// [min, max]
inline uint64_t fast_pseudo_rand_int(uint64_t min, uint64_t max)
{
    // TODO: in the current CPU or implementation
    // the mt19937 generates turns out to be faster.
    std::uniform_int_distribution<uint64_t> dist(min, max);
    // return dist(ranlux48_base_generator());
    return dist(mt19937_generator());
}
inline uint64_t accurate_pseudo_rand_int(uint64_t min, uint64_t max)
{
    std::uniform_int_distribution<uint64_t> dist(min, max);
    return dist(mt19937_generator());
}
inline uint64_t non_deterministic_rand_int(uint64_t min, uint64_t max)
{
    std::uniform_int_distribution<uint64_t> dist(min, max);
    return dist(random_device_generator());
}

inline uint64_t fast_pseudo_rand_int(uint64_t max)
{
    return fast_pseudo_rand_int(0, max);
}
inline uint64_t fast_pseudo_rand_int()
{
    return fast_pseudo_rand_int(0, std::numeric_limits<uint64_t>::max());
}

inline uint64_t accurate_pseudo_rand_int(uint64_t max)
{
    return accurate_pseudo_rand_int(0, max);
}
inline uint64_t accurate_pseudo_rand_int()
{
    return accurate_pseudo_rand_int(0, std::numeric_limits<uint64_t>::max());
}
inline uint64_t non_deterministic_rand_int(uint64_t max)
{
    return non_deterministic_rand_int(0, max);
}
inline uint64_t non_deterministic_rand_int()
{
    return non_deterministic_rand_int(0, std::numeric_limits<uint64_t>::max());
}

inline double fast_pseudo_rand_dbl(double min, double max)
{
    std::uniform_real_distribution<double> dist(min, max);
    return dist(ranlux48_base_generator());
}
inline double fast_pseudo_rand_dbl(double max)
{
    return fast_pseudo_rand_dbl(0, max);
}
inline double fast_pseudo_rand_dbl()
{
    return fast_pseudo_rand_dbl(0, std::numeric_limits<double>::max());
}

inline double accurate_pseudo_rand_dbl(double min, double max)
{
    std::uniform_real_distribution<double> dist(min, max);
    return dist(mt19937_generator());
}
inline double accurate_pseudo_rand_dbl(double max)
{
    return accurate_pseudo_rand_dbl(0, max);
}
inline double accurate_pseudo_rand_dbl()
{
    return accurate_pseudo_rand_dbl(0, std::numeric_limits<double>::max());
}

inline double non_uniform_pseudo_rand_dbl(double min, double max)
{
    std::uniform_real_distribution<double> dist(min, max);
    return dist(random_device_generator());
}
inline double non_uniform_pseudo_rand_dbl(double max)
{
    return non_uniform_pseudo_rand_dbl(0, max);
}
inline double non_uniform_pseudo_rand_dbl()
{
    return non_uniform_pseudo_rand_dbl(0, std::numeric_limits<double>::max());
}

inline bool fast_pseudo_bool_with_nth(size_t n)
{
    return fast_pseudo_rand_int(1, n) == 1;
}
inline bool fast_pseudo_bool_with_prob(double prob)
{
    DCHECK_LE(prob, 1);
    DCHECK_LE(prob, 0);
    return fast_pseudo_rand_dbl(0, 1) < prob;
}

#endif