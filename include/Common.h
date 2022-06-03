#pragma once
#ifndef __COMMON_H__
#define __COMMON_H__

#include <glog/logging.h>

#include <atomic>
#include <bitset>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <limits>

#include "HugePageAlloc.h"
#include "Statistics.h"
#include "Timer.h"
#include "WRID.h"
#include "WRLock.h"
#include "util/Type.h"

// CONFIG_ENABLE_EMBEDDING_LOCK and CONFIG_ENABLE_CRC
// **cannot** be ON at the same time

// #define CONFIG_ENABLE_EMBEDDING_LOCK
// #define CONFIG_ENABLE_CRC

// #define CONFIG_ENABLE_ON_CHIP_LOCK
// #define CONFIG_ENABLE_FINER_VERSION
// #define CONFIG_ENABLE_OP_COUPLE
// #define CONFIG_ENABLE_HOT_FILTER
// #define CONFIG_ENABLE_HIERARCHIAL_LOCK

// #define CONFIG_EABLE_BAKERY_LOCK

// #define TEST_SINGLE_THREAD

#define LATENCY_WINDOWS 1000000

#define MAX_MACHINE 4

#define ADD_ROUND(x, n) ((x) = ((x) + 1) % (n))

#define MESSAGE_SIZE 96  // byte

#define POST_RECV_PER_RC_QP 128

#define RAW_RECV_CQ_COUNT 128

#define APP_MESSAGE_NR 96

// }

// { dir thread
#define NR_DIRECTORY (4)

#define DIR_MESSAGE_NR 128

// }

// { app thread
// #define kMaxAppThread 26
constexpr static ssize_t kMaxAppThread = 32;

/**
 * Equivalent to c++14 [[maybe_unused]]
 */
#define __maybe_unused(x) ((void) (x))

void bindCore(uint16_t core);
char *getIP();
char *getMac();

inline int bits_in(std::uint64_t u)
{
    auto bs = std::bitset<64>(u);
    return bs.count();
}

#include <boost/coroutine/all.hpp>

using CoroYield =
    typename boost::coroutines::symmetric_coroutine<void>::yield_type;
using CoroCall =
    typename boost::coroutines::symmetric_coroutine<void>::call_type;

using coro_t = uint8_t;
static constexpr coro_t kMasterCoro = coro_t(-1);
static constexpr coro_t kNotACoro = coro_t(-2);
using trace_t = uint8_t;
namespace define
{
constexpr uint64_t KB = 1024ull;
constexpr uint64_t MB = 1024ull * KB;
constexpr uint64_t GB = 1024ull * MB;
constexpr uint64_t TB = 1024ull * GB;
constexpr uint16_t kCacheLineSize = 64;

constexpr uint64_t K = 1000ull;
constexpr uint64_t M = 1000ull * K;
constexpr uint64_t G = 1000ull * M;
constexpr uint64_t T = 1000ull * G;

namespace literals
{
// this is useful for implicite type conversion
constexpr uint64_t operator""_B(unsigned long long i)
{
    return (uint64_t) i;
}
constexpr uint64_t operator""_K(unsigned long long i)
{
    return (uint64_t) i * 1000;
}
constexpr uint64_t operator""_KB(unsigned long long i)
{
    return (uint64_t) i * 1024;
}
constexpr uint64_t operator""_M(unsigned long long i)
{
    return (uint64_t) i * 1000 * 1_K;
}
constexpr uint64_t operator""_MB(unsigned long long i)
{
    return (uint64_t) i * 1024 * 1_KB;
}
constexpr uint64_t operator""_G(unsigned long long i)
{
    return (uint64_t) i * 1000 * 1_M;
}
constexpr uint64_t operator""_GB(unsigned long long i)
{
    return (uint64_t) i * 1024 * 1_MB;
}
constexpr uint64_t operator""_T(unsigned long long i)
{
    return (uint64_t) i * 1000 * 1_G;
}
constexpr uint64_t operator""_TB(unsigned long long i)
{
    return (uint64_t) i * 1024 * 1_GB;
}
constexpr uint64_t operator""_P(unsigned long long i)
{
    return (uint64_t) i * 1000 * 1_T;
}
constexpr uint64_t operator""_PB(unsigned long long i)
{
    return (uint64_t) i * 1024 * 1_TB;
}
}  // namespace literals

// for remote allocate
constexpr uint64_t kChunkSize = 32 * MB;

// for store root pointer
constexpr uint64_t kRootPointerStoreOffest = kChunkSize / 2;
static_assert(kRootPointerStoreOffest % sizeof(uint64_t) == 0, "XX");

// lock on-chip memory
constexpr uint64_t kLockStartAddr = 0;
constexpr uint64_t kLockChipMemSize = 256 * 1024;

// number of locks
constexpr uint64_t kNumOfLock = kLockChipMemSize / sizeof(uint64_t);

// level of tree
constexpr uint64_t kMaxLevelOfTree = 7;

constexpr uint16_t kMaxCoroNr = 32;

constexpr uint8_t kMaxHandOverTime = 8;

constexpr int kIndexCacheSize = 1024;  // MB

// for dsm
constexpr static uint32_t kRDMABufferSize = 32 * define::MB;
constexpr int64_t kPerCoroRdmaBuf = 32 * define::KB;
}  // namespace define

static inline unsigned long long asm_rdtsc(void)
{
    unsigned hi, lo;
    __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
    return ((unsigned long long) lo) | (((unsigned long long) hi) << 32);
}

// For Tree
using Key = uint64_t;
using Value = uint64_t;
constexpr Key kKeyMin = std::numeric_limits<Key>::lowest();
constexpr Key kKeyMax = std::numeric_limits<Key>::max();
constexpr Value kValueNull = 0;
constexpr uint32_t kInternalPageSize = 1024;
constexpr uint32_t kLeafPageSize = 1024;

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

/**
 * It's c++ 11, so implement my make_unique
 */
namespace future
{
template <typename T, typename... Args>
std::unique_ptr<T> make_unique(Args &&... args)
{
    return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
}
template <typename T, typename... Args>
std::shared_ptr<T> make_shared(Args &&... args)
{
    return std::shared_ptr<T>(new T(std::forward<Args>(args)...));
}

}  // namespace future

struct Data
{
    union {
        struct
        {
            uint32_t lower;
            uint32_t upper;
        };
        uint64_t val;
    };
} __attribute__((packed));

#define ROUND_UP(num, multiple) ceil(((double) (num)) / (multiple)) * (multiple)

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

#ifdef NDEBUG
constexpr bool debug()
{
    return false;
}
#else
constexpr bool debug()
{
    return true;
}
#endif

namespace config
{
constexpr static size_t kMachineNr = 4;
static_assert(kMachineNr <= MAX_MACHINE);
static const std::vector<size_t> __kServerNodeIds{0};
static const std::vector<size_t> __kClientNodeIds{1, 2, 3};
inline bool is_server(size_t nid)
{
    auto it =
        std::find(__kServerNodeIds.cbegin(), __kServerNodeIds.cend(), nid);
    return it != __kServerNodeIds.cend();
}
inline bool is_client(size_t nid)
{
    auto it =
        std::find(__kClientNodeIds.cbegin(), __kClientNodeIds.cend(), nid);
    return it != __kClientNodeIds.cend();
}
inline std::vector<size_t> get_client_nids()
{
    return __kClientNodeIds;
}
inline std::vector<size_t> get_server_nids()
{
    return __kServerNodeIds;
}

using namespace define::literals;

// about enabling monitors, sacrifying performance
constexpr static bool kMonitorControlPath = false;
constexpr static bool kMonitorReconnection = false;
constexpr static bool kMonitorFailureRecovery = false;
constexpr static bool kMonitorAddressConversion = false;

constexpr static bool kEnableReliableMessageSingleThread = true;
constexpr static bool kEnableSkipMagicMw = true;

constexpr static bool kEnableRdmaTrace = false;
// constexpr static double kRdmaTraceRateGet = 1.0 / 200_K;
// constexpr static double kRdmaTraceRatePut = 1.0 / 20_K;
// constexpr static double kRdmaTraceRateDel = 1.0 / 20_K;
// constexpr static double kRdmaTraceRateExpand = 1.0;
constexpr static double kRdmaTraceRateBootstrap = 1.0 / 20_K;
constexpr static double kRdmaTraceRateGet = 0;
constexpr static double kRdmaTraceRatePut = 0;
constexpr static double kRdmaTraceRateDel = 0;
constexpr static double kRdmaTraceRateExpand = 0;
// constexpr static double kRdmaTraceRateBootstrap = 0;

// about opening a feature
constexpr static bool kEnableReliableMessage = true;

// about higher level of debugging, sacrifying performance.
constexpr static bool kEnableValidityMutex = false;
constexpr static bool kEnableTrace = false;
constexpr static uint64_t kTraceRate =
    100000;  // (1.0 / kTraceRate) possibility
// slab allocator checks whether each free is valid.
// Turn this off, since patronus allows free-ing buffers across clients
constexpr static bool kEnableSlabAllocatorStrictChecking = false;
constexpr static bool kMonitorSlabAllocator = false;
// other settings
constexpr static size_t kLeaseCacheItemLimitNr = 3;

// the unsignaled unbinds may flood the QP
// change unsignaled to signaled ones every @kUnsignaledUnbindRateLimit
constexpr static size_t kUnsignaledUnbindRateLimit = 4;

}  // namespace config

#define likely(x) __builtin_expect((x), 1)
#define unlikely(x) __builtin_expect((x), 0)

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

inline bool is_mw_magic_err(uint64_t id)
{
    constexpr static uint32_t magic = 0b1010101010;
    constexpr static uint16_t mask = 0b1111111111;
    if (unlikely((id & mask) == magic))
    {
        return true;
    }
    return false;
}

#endif /* __COMMON_H__ */