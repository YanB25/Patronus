#pragma once

#ifndef PATRONUS_TYPE_H_
#define PATRONUS_TYPE_H_

#include <cstdint>
#include <cstdlib>
#include <iostream>

#include "ReliableMessageConnection.h"
#include "patronus/Time.h"
#include "util/Debug.h"

namespace patronus
{
using id_t = uint64_t;
using rkey_t = uint32_t;
using chrono_time_t = std::chrono::time_point<std::chrono::steady_clock>;
using flag_t = uint64_t;

// force enum to be sizeof(uint8_t)
enum class RequestType : uint8_t
{
    kAcquireRLease,
    kAcquireWLease,
    kAcquireNoLease,  // for debug purpose
    kUpgrade,
    kRelinquish,
    kExtend,
    kAdmin,
    kTimeSync,
};
std::ostream &operator<<(std::ostream &os, const RequestType &t);

enum class AcquireRequestStatus : uint8_t
{
    kSuccess,
    kMagicMwErr,
    kLockedErr,
    kBindErr,
    kRegMrErr,
    kAddressOutOfRangeErr,
    kNoMem,
    kNoMw,
    kReserved,
    kReservedNoReturn,
};
std::ostream &operator<<(std::ostream &os, AcquireRequestStatus status);

struct ClientID
{
    union {
        struct
        {
            uint16_t node_id;
            uint16_t thread_id;
            uint8_t mid;
            coro_t coro_id;
            uint16_t rpc_ctx_id;
            // below two idx is used for internal management
        } __attribute__((packed));
        uint64_t cid;
    };
    /**
     * only the node_id, thread_id and coro_id is the identity.
     */
    bool is_same(const ClientID &rhs) const
    {
        return node_id == rhs.node_id && thread_id == rhs.thread_id &&
               coro_id == rhs.coro_id;
    }

    // to be safe. use is_same instead
    bool operator==(const ClientID &rhs) const = delete;
    bool operator!=(const ClientID &rhs) const = delete;

} __attribute__((packed));
static_assert(sizeof(ClientID) == sizeof(uint64_t));
std::ostream &operator<<(std::ostream &os, const ClientID &cid);

struct BaseMessage
{
    enum RequestType type;
    ClientID cid;
    char others[0];
} __attribute__((packed));

enum class AcquireRequestFlag : uint16_t
{
    kNoGc = 1 << 0,
    kWithConflictDetect = 1 << 1,
    kNoBindPR = 1 << 2,
    kNoBindAny = 1 << 3,
    kWithAllocation = 1 << 4,
    kOnlyAllocation = 1 << 5,
    kUseMR = 1 << 6,
    kDoNothing = 1 << 7,
    kReserved = 1 << 8,
};

void debug_validate_acquire_request_flag(flag_t flag);
struct AcquireRequestFlagOut
{
    AcquireRequestFlagOut(flag_t flag) : flag(flag)
    {
    }
    flag_t flag;
};
std::ostream &operator<<(std::ostream &os, AcquireRequestFlagOut flag);
struct AcquireRequest
{
    enum RequestType type;
    ClientID cid;
    id_t key;
    size_t size;
    time::ns_t required_ns;
    uint16_t dir_id;
    trace_t trace;
    uint16_t flag;  // should be AcquireRequestFlag
    Debug<uint64_t> digest;
} __attribute__((packed));
static_assert(sizeof(AcquireRequest) < ReliableConnection::kMessageSize);
static_assert(NR_DIRECTORY <
              std::numeric_limits<decltype(AcquireRequest::dir_id)>::max());
static_assert(sizeof(AcquireRequest::flag) >= sizeof(AcquireRequestFlag));
std::ostream &operator<<(std::ostream &os, const AcquireRequest &req);

struct AcquireResponse
{
    enum RequestType type;
    ClientID cid;
    uint32_t rkey_0;
    uint32_t rkey_header;
    uint64_t buffer_base;
    uint64_t header_base;
    // about time management of Lease
    time::term_t begin_term;
    uint32_t ns_per_unit;
    uint16_t lease_id;
    uint32_t aba_id;
    AcquireRequestStatus status;
} __attribute__((packed));
static_assert(sizeof(AcquireResponse) < ReliableConnection::kMessageSize);
std::ostream &operator<<(std::ostream &os, const AcquireResponse &resp);

enum class AdminFlag : uint8_t
{
    kAdminReqExit = 0,
    kAdminReqRecovery = 1,
    kAdminBarrier = 2,
};
std::ostream &operator<<(std::ostream &os, const AdminFlag &f);

struct AdminRequest
{
    enum RequestType type;
    ClientID cid;
    uint8_t flag;  // enum AdminFlag
    Debug<uint64_t> digest;
    uint16_t dir_id;
    uint64_t data;  // used by p->barrier()
} __attribute__((packed));
static_assert(sizeof(AdminRequest) < ReliableConnection::kMessageSize);
static_assert(sizeof(AdminRequest::flag) >= sizeof(AdminFlag));
std::ostream &operator<<(std::ostream &os, const AdminRequest &resp);

enum class LeaseModifyFlag : uint8_t
{
    kNoRelinquishUnbind = 1 << 0,
    kForceUnbind = 1 << 1,
    kWithDeallocation = 1 << 2,
    kOnlyDeallocation = 1 << 3,
    // wait until unbind success before returning
    // will harm performance
    kWaitUntilSuccess = 1 << 4,
    kUseMR = 1 << 5,
    kReserved = 1 << 6,
};
void debug_validate_lease_modify_flag(flag_t flag);
struct LeaseModifyFlagOut
{
    LeaseModifyFlagOut(flag_t flag) : flag(flag)
    {
    }
    flag_t flag;
};
std::ostream &operator<<(std::ostream &os, LeaseModifyFlagOut flag);

struct LeaseModifyRequest
{
    enum RequestType type;
    ClientID cid;
    id_t lease_id;
    time::ns_t ns;
    uint64_t hint; /* when only_dealloc is ON */
    uint8_t flag;  /* LeaseModifyFlag */
    uint64_t addr; /* when only_dealloc is ON */
    uint32_t size; /* when only_dealloc is ON */
    Debug<uint64_t> digest;
} __attribute__((packed));
static_assert(sizeof(LeaseModifyRequest) < ReliableConnection::kMessageSize);
std::ostream &operator<<(std::ostream &os, const LeaseModifyRequest &req);
static_assert(sizeof(LeaseModifyRequest::flag) >= sizeof(LeaseModifyFlag));

struct LeaseModifyResponse
{
    enum RequestType type;
    ClientID cid;
    uint64_t lease_id;
    bool success;
    Debug<uint64_t> digest;

} __attribute__((packed));
static_assert(sizeof(LeaseModifyResponse) < ReliableConnection::kMessageSize);
std::ostream &operator<<(std::ostream &os, const LeaseModifyResponse &req);

enum class RWFlag : uint8_t
{
    kNoLocalExpireCheck = 1 << 0,
    kWithAutoExtend = 1 << 1,
    kWithCache = 1 << 2,
    kUseUniversalRkey = 1 << 3,
    kReserved = 1 << 4,
};

struct RWFlagOut
{
    RWFlagOut(flag_t flag) : flag(flag)
    {
    }
    flag_t flag;
};
std::ostream &operator<<(std::ostream &os, RWFlagOut flag);

}  // namespace patronus

#endif