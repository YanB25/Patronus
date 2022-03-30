#pragma once
#ifndef SHERMAM_UTIL_FAST_RAND_H_
#define SHERMAM_UTIL_FAST_RAND_H_

#include <random>
#include <set>

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

inline void fast_pseudo_fill_buf(char *s, size_t len)
{
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    constexpr size_t size_of_alpha = (sizeof(alphanum) - 1) / sizeof(char);
    for (size_t i = 0; i < len; ++i)
    {
        s[i] = alphanum[fast_pseudo_rand_int(0, size_of_alpha - 1)];
        DCHECK(isalnum(s[i])) << " sizeof alpha: " << size_of_alpha
                              << ", size: " << sizeof(alphanum);
    }
}

inline bool true_with_prob(double prob)
{
    return fast_pseudo_rand_dbl(0, 1) <= prob;
}

template <typename T>
inline auto random_choose(
    const std::set<T> &set,
    size_t rand_limit = std::numeric_limits<size_t>::max())
{
    DCHECK(!set.empty());
    auto it = set.begin();
    size_t limit = std::min(set.size(), rand_limit);
    auto adv = fast_pseudo_rand_int(0, limit - 1);
    std::advance(it, adv);
    return *it;
}

#endif