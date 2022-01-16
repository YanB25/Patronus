#pragma once

#ifndef PATRONUS_TYPE_H_
#define PATRONUS_TYPE_H_

#include <cstdint>
#include <cstdlib>
#include <iostream>

#include "ReliableMessageConnection.h"
#include "util/Debug.h"

namespace patronus
{
using id_t = uint64_t;
using term_t = int64_t;
using rkey_t = uint32_t;
using chrono_time_t = std::chrono::time_point<std::chrono::steady_clock>;

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
} __attribute__((packed));
static_assert(sizeof(ClientID) == sizeof(uint64_t));
std::ostream &operator<<(std::ostream &os, const ClientID &cid);

struct BaseMessage
{
    enum RequestType type;
    ClientID cid;
    char others[0];
} __attribute__((packed));

enum class AcquireRequestFlag : uint8_t
{
    kNoGc = 1 << 0,
    kReserved = 1 << 1,
};
struct AcquireRequestFlagOut
{
    AcquireRequestFlagOut(uint8_t flag) : flag(flag)
    {
    }
    uint8_t flag;
};
std::ostream &operator<<(std::ostream &os, AcquireRequestFlagOut flag);
struct AcquireRequest
{
    enum RequestType type;
    ClientID cid;
    id_t key;
    size_t size;
    term_t require_term;
    uint16_t dir_id;
    trace_t trace;
    uint8_t flag;  // should be AcquireRequestFlag
    Debug<uint64_t> digest;
} __attribute__((packed));
static_assert(sizeof(AcquireRequest) < ReliableConnection::kMessageSize);
static_assert(NR_DIRECTORY <
              std::numeric_limits<decltype(AcquireRequest::dir_id)>::max());
std::ostream &operator<<(std::ostream &os, const AcquireRequest &req);

struct AcquireResponse
{
    enum RequestType type;
    ClientID cid;
    uint32_t rkey_0;
    uint32_t rkey_header;
    uint64_t buffer_base;
    uint64_t header_base;
    term_t ddl_term;
    uint16_t lease_id;
    bool success;
    Debug<uint64_t> digest;
} __attribute__((packed));
static_assert(sizeof(AcquireResponse) < ReliableConnection::kMessageSize);
std::ostream &operator<<(std::ostream &os, const AcquireResponse &resp);

enum class AdminFlag : uint8_t
{
    kAdminReqExit = 0,
    kAdminReqRecovery = 1,
};
std::ostream &operator<<(std::ostream &os, const AdminFlag &f);

struct AdminRequest
{
    enum RequestType type;
    ClientID cid;
    uint8_t flag;  // enum AdminFlag
    Debug<uint64_t> digest;
    uint16_t dir_id;
} __attribute__((packed));
static_assert(sizeof(AdminRequest) < ReliableConnection::kMessageSize);
std::ostream &operator<<(std::ostream &os, const AdminRequest &resp);

struct LeaseModifyRequest
{
    enum RequestType type;
    ClientID cid;
    id_t lease_id;
    term_t term;
    Debug<uint64_t> digest;
} __attribute__((packed));
static_assert(sizeof(LeaseModifyRequest) < ReliableConnection::kMessageSize);
std::ostream &operator<<(std::ostream &os, const LeaseModifyRequest &req);

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

}  // namespace patronus

#endif