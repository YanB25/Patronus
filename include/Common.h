#pragma once
#ifndef __COMMON_H__
#define __COMMON_H__

#include <glog/logging.h>

#include <algorithm>
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
#include "util/Literals.h"
#include "util/Type.h"
#include "util/Util.h"

using namespace util::literals;

constexpr static size_t MAX_MACHINE = 4;
constexpr static int NR_DIRECTORY = 4;
constexpr static ssize_t kMaxAppThread = 32;
constexpr static const char *kNICName = "eno1";
constexpr static const char *kNodeRankFileName = "inet.conf";

#define MESSAGE_SIZE 96  // byte
#define RAW_RECV_CQ_COUNT 128
#define APP_MESSAGE_NR 96
#define DIR_MESSAGE_NR 128

using trace_t = uint8_t;

namespace config
{
constexpr static size_t kMachineNr = 4;
static_assert(kMachineNr <= MAX_MACHINE);
constexpr static size_t kDefaultDSMSize = 16_GB;
constexpr static size_t kDefaultCacheSize = 8_GB;

// it seems that vectorization gets slower.
constexpr static bool kUseAVXMemcpy = false;
constexpr static bool kUseSSEMemcpy = false;

}  // namespace config

namespace define
{
constexpr uint16_t kCacheLineSize = 64;

// for remote allocate
constexpr uint64_t kChunkSize = 32_MB;

// lock on-chip memory
constexpr uint64_t kLockStartAddr = 0;
// constexpr uint64_t kLockChipMemSize = 256 * 1024;
// Just one 1/4, yes, you could use all of them.
constexpr uint64_t kLockChipMemSize = 64_KB;

constexpr uint16_t kMaxCoroNr = 32;

// for dsm
constexpr static uint32_t kRDMABufferSize =
    ::config::kDefaultCacheSize / kMaxAppThread;
constexpr int64_t kPerCoroRdmaBuf = 32_KB;
}  // namespace define

// For Tree
using Key = uint64_t;
using Value = uint64_t;
constexpr Key kKeyMin = std::numeric_limits<Key>::lowest();
constexpr Key kKeyMax = std::numeric_limits<Key>::max();
constexpr Value kValueNull = 0;
constexpr uint32_t kInternalPageSize = 1024;
constexpr uint32_t kLeafPageSize = 1024;

namespace config
{
// for example, for "mlx5_0",
// the kSelectMlVersion should be '5'
// the kSelectMlxIdx should be '0'
constexpr static char kSelectMlxVersion = '5';  // mlx5
// If you have multiple devices
constexpr static char kSelectMlxNicIdx = '0';
static const std::vector<size_t> __kServerNodeIds{1};
static const std::vector<size_t> __kClientNodeIds{0, 2, 3};
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

// about enabling monitors, sacrifying performance
constexpr static bool kMonitorControlPath = false;
constexpr static bool kMonitorReconnection = false;
constexpr static bool kMonitorFailureRecovery = false;
constexpr static bool kMonitorAddressConversion = false;
constexpr static bool kMonitorLeaseContext = false;
constexpr static bool kMonitorCoroSwitch = false;

constexpr static bool kReportTraceViewRoute = false;

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
}  // namespace config

#define likely(x) __builtin_expect((x), 1)
#define unlikely(x) __builtin_expect((x), 0)

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

namespace config::verbose
{
constexpr static size_t kBenchReserve_1 = 1;
constexpr static size_t kBenchReserve_2 = 2;
constexpr static size_t kSystem = 3;
constexpr static size_t kUserApp_1 = 4;
constexpr static size_t kUserApp_2 = 5;
constexpr static size_t kRdmaAdpt = 6;
constexpr static size_t kPatronus = 7;
constexpr static size_t kPatronusUtils = 8;
constexpr static size_t kDSM = 9;
constexpr static size_t kUmsg = 10;
constexpr static size_t kRdmaOperation = 11;

constexpr static size_t kCoroLauncher = kPatronus;
constexpr static size_t kTimeSyncer = kRdmaOperation;

constexpr static size_t kVerbose = 20;
}  // namespace config::verbose

namespace util
{
inline std::optional<int> node_id_in_rank_file()
{
    constexpr static size_t V = ::config::verbose::kSystem;
    auto rank_file_name = std::filesystem::current_path() / kNodeRankFileName;
    if (std::filesystem::exists(rank_file_name))
    {
        std::ifstream t(rank_file_name);
        std::string buffer((std::istreambuf_iterator<char>(t)),
                           std::istreambuf_iterator<char>());
        std::vector<std::string> results;
        boost::split(results, buffer, boost::is_any_of("\n"));
        std::string ip_addr = getIP();
        auto it = std::find(results.begin(), results.end(), ip_addr);
        if (it == results.end())
        {
            LOG(FATAL) << "[util] file " << rank_file_name << " exists. but ip "
                       << ip_addr << " not found.";
            return std::nullopt;
        }
        else
        {
            int idx = it - results.begin();
            VLOG(V) << "[util] file " << rank_file_name
                    << " exists. got node_id: " << idx;
            return idx;
        }
    }
    else
    {
        LOG(ERROR) << "[util] rank_file " << rank_file_name << " not found. ";
        return std::nullopt;
    }
}
}  // namespace util

#endif /* __COMMON_H__ */