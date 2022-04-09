#pragma once
#ifndef SHERMAM_UTIL_BENCH_RAND_H_
#define SHERMAM_UTIL_BENCH_RAND_H_

#include <memory>

#include "Common.h"
#include "util/Rand.h"
#include "util/ZipRand.h"
#include "util/zipf.h"

class IKVRandGenerator
{
public:
    using pointer = std::shared_ptr<IKVRandGenerator>;
    virtual size_t gen_key(char *buf, size_t size) = 0;
    virtual size_t gen_value(char *buf, size_t size) = 0;
    virtual ~IKVRandGenerator() = default;
};

class UniformRandGenerator : public IKVRandGenerator
{
public:
    using pointer = std::shared_ptr<UniformRandGenerator>;
    static pointer new_instance(uint64_t min_key, uint64_t max_key)
    {
        return std::make_shared<UniformRandGenerator>(min_key, max_key);
    }

    // [min_key, max_key)
    UniformRandGenerator(uint64_t min_key, uint64_t max_key)
        : min_key_(min_key), max_key_(max_key)
    {
    }
    size_t gen_key(char *buf, size_t size) override
    {
        uint64_t r = 0;
        if (likely(max_key_ >= 1))
        {
            r = fast_pseudo_rand_int(min_key_, max_key_ - 1);
        }
        DCHECK_GE(size, sizeof(uint64_t));
        memcpy(buf, &r, sizeof(uint64_t));
        return sizeof(uint64_t);
    }
    size_t gen_value(char *buf, size_t size) override
    {
        fast_pseudo_fill_buf(buf, size);
        return size;
    }

private:
    uint64_t min_key_{0};
    uint64_t max_key_{0};
};

class MehcachedZipfianRandGenerator : public IKVRandGenerator
{
public:
    using pointer = std::shared_ptr<MehcachedZipfianRandGenerator>;
    constexpr static double kSkewness = 0.99;
    // [min_key, max_key)
    static pointer new_instance(uint64_t min,
                                uint64_t max,
                                double z = kSkewness,
                                uint64_t seed = 0)
    {
        return std::make_shared<MehcachedZipfianRandGenerator>(
            min, max, z, seed);
    }
    MehcachedZipfianRandGenerator(uint64_t min,
                                  uint64_t max,
                                  double z,
                                  uint64_t seed)
        : min_(min)
    {
        mehcached_zipf_init(&state_, max - min, z, seed);
    }
    size_t gen_key(char *buf, size_t size) override
    {
        uint64_t r = mehcached_zipf_next(&state_) + min_;
        DCHECK_GE(size, sizeof(uint64_t));
        memcpy(buf, &r, sizeof(uint64_t));
        return sizeof(uint64_t);
    }
    size_t gen_value(char *buf, size_t size) override
    {
        fast_pseudo_fill_buf(buf, size);
        return size;
    }

private:
    uint64_t min_{0};
    zipf_gen_state state_;
};

class NothingRandGenerator : public IKVRandGenerator
{
public:
    using pointer = std::shared_ptr<NothingRandGenerator>;
    static pointer new_instance()
    {
        return std::make_shared<NothingRandGenerator>();
    }
    NothingRandGenerator()
    {
    }
    size_t gen_key(char *buf, size_t size) override
    {
        uint64_t r = 0;
        DCHECK_GE(size, sizeof(uint64_t));
        memcpy(buf, &r, sizeof(uint64_t));
        return sizeof(uint64_t);
    }
    size_t gen_value(char *buf, size_t size) override
    {
        fast_pseudo_fill_buf(buf, size);
        return size;
    }

private:
};

#endif