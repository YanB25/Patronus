#ifndef __COMMON_H__
#define __COMMON_H__

#include <atomic>
#include <bitset>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>

#include "HugePageAlloc.h"
#include "Rdma.h"
#include "Statistics.h"
#include "WRLock.h"

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

#define MAX_MACHINE 8

#define ADD_ROUND(x, n) ((x) = ((x) + 1) % (n))

#define MESSAGE_SIZE 96  // byte

#define POST_RECV_PER_RC_QP 128

#define RAW_RECV_CQ_COUNT 128

// { app thread
#define MAX_APP_THREAD 26

#define APP_MESSAGE_NR 96

// }

// { dir thread
#define NR_DIRECTORY 2

#define DIR_MESSAGE_NR 128

#define RMSG_MULTIPLEXING 12
// }

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

struct CoroContext
{
    CoroYield *yield;
    CoroCall *master;
    int coro_id;
};

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

constexpr uint16_t kMaxCoro = 8;

constexpr uint8_t kMaxHandOverTime = 8;

constexpr int kIndexCacheSize = 1024;  // MB

// for dsm
constexpr static uint32_t kRDMABufferSize = 12 * define::MB;
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
constexpr Key kKeyMin = std::numeric_limits<Key>::min();
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
std::unique_ptr<T> make_unique(Args &&...args)
{
    return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
}
template <typename T, typename... Args>
std::shared_ptr<T> make_shared(Args &&...args)
{
    return std::shared_ptr<T>(new T(std::forward<Args>(args)...));
}

}  // namespace future

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

struct WRID
{
    WRID(uint16_t p, uint16_t a16, uint16_t b16, uint16_t c16)
        : prefix(p), u16_a(a16), u16_b(b16), u16_c(c16)
    {
    }
    WRID(uint64_t v) : val(v)
    {
    }
    WRID(uint16_t p, uint32_t id) : prefix(p), u16_a(0), id(id)
    {
    }
    union
    {
        struct
        {
            uint16_t prefix;
            uint16_t u16_a;
            union
            {
                struct
                {
                    uint16_t u16_b;
                    uint16_t u16_c;
                } __attribute__((packed));
                uint32_t id;
            };
        } __attribute__((packed));
        uint64_t val;
    };
} __attribute__((packed));

inline std::ostream &operator<<(std::ostream &os, WRID wrid)
{
    os << "{WRID prefix: " << wrid.prefix << ", a: " << wrid.u16_a
       << ", b: " << wrid.u16_b << ", c: " << wrid.u16_c << "/ id: " << wrid.id
       << "/ val: " << wrid.val << "}";
    return os;
}

#define ROUND_UP(num, multiple) ceil(((double) (num)) / (multiple)) * (multiple)

#define WRID_PREFIX_EXMETA 1
#define WRID_PREFIX_RELIABLE_SEND 2
#define WRID_PREFIX_RELIABLE_RECV 3

static inline uint64_t djb2_digest(const char *str, size_t size)
{
    unsigned long hash = 5381;
    int c;

    for (size_t i = 0; i < size; ++i)
    {
        c = str[i];
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    }
    return hash;
}

namespace config
{
constexpr static bool kMonitorControlPath = false;
constexpr static bool kMonitorReconnection = false;

constexpr static bool kEnableReliableMessage = true;
}  // namespace config

#endif /* __COMMON_H__ */