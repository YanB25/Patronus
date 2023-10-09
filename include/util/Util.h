#pragma once
#ifndef UTIL_UTIL_H_
#define UTIL_UTIL_H_
#include <boost/algorithm/string.hpp>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "Common.h"
#include "util/Debug.h"

#define ROUND_UP(num, multiple) ceil(((double) (num)) / (multiple)) * (multiple)

struct Data
{
    union
    {
        struct
        {
            uint32_t lower;
            uint32_t upper;
        };
        uint64_t val;
    };
} __attribute__((packed));

void bindCore(uint16_t core);
char *getIP();
char *getMac();

namespace util
{
static inline unsigned long long asm_rdtsc(void)
{
    unsigned hi, lo;
    __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
    return ((unsigned long long) lo) | (((unsigned long long) hi) << 32);
}

__inline__ unsigned long long rdtsc(void)
{
    unsigned hi, lo;
    __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
    return ((unsigned long long) lo) | (((unsigned long long) hi) << 32);
}

inline void mfence()
{
    asm volatile("mfence" ::: "memory");
}

inline void compiler_barrier()
{
    asm volatile("" ::: "memory");
}

static inline uint64_t djb2_digest(const void *void_str, size_t size)
{
    const char *str = (const char *) void_str;
    unsigned long hash = 5381;
    int c;

    for (size_t i = 0; i < size; ++i)
    {
        c = str[i];
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    }
    return hash;
}

}  // namespace util

inline std::string binary_to_csv_filename(
    const std::string &bench_path,
    const std::string &exec_meta,
    const std::map<std::string, std::string> &extra = {})
{
    std::string root = "../result/";
    auto filename = std::filesystem::path(bench_path).filename().string();

    std::string ret = root + filename + "." + exec_meta + ".";
    for (const auto &[k, v] : extra)
    {
        ret += k + ":" + v + ".";
    }
    if constexpr (debug())
    {
        ret += "DEBUG.";
    }
    ret += "csv";
    return ret;
}
inline std::filesystem::path artifacts_directory()
{
    return "../artifacts/";
}

inline void ro_prefetch([[maybe_unused]] const void *addr)
{
    // __builtin_prefetch(addr, 0 /* for read */, 1);
}

#endif