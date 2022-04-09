#pragma once
#ifndef SHERMAM_UTIL_ZIP_RAND_H_
#define SHERMAM_UTIL_ZIP_RAND_H_

#include <glog/logging.h>

#include <atomic>
#include <cinttypes>
#include <cmath>
#include <cstddef>

#include "util/Rand.h"
#include "util/zipf.h"

namespace util
{
class BaseZipfianGenerator
{
public:
    constexpr static const double kZipfianConst = 0.99;

    BaseZipfianGenerator(uint64_t max,
                         double zipfian_const = kZipfianConst,
                         uint64_t seed = 0)
    {
        mehcached_zipf_init(&state_, max, zipfian_const, seed);
    }

    uint64_t Next()
    {
        return mehcached_zipf_next(&state_);
    }

private:
    zipf_gen_state state_;
};

class ZipfianGenerator
{
public:
    constexpr static const double kZipfianConst = 0.99;
    ZipfianGenerator(uint64_t min,
                     uint64_t max,
                     double zipfian_const = kZipfianConst,
                     uint64_t seed = 0)
        : base_(min), g_(max - min, zipfian_const, seed)
    {
    }
    uint64_t Next()
    {
        return g_.Next() + base_;
    }

private:
    uint64_t base_;
    BaseZipfianGenerator g_;
};

}  // namespace util

#endif