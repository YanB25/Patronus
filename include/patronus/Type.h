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

// force enum to be sizeof(uint8_t)
enum class RequestType : uint8_t
{
    kAcquireRLease,
    kAcquireWLease,
    kUpgrade,
    kRelinquish,
    kExtend,
    kAdmin,
};
std::ostream &operator<<(std::ostream &os, const RequestType &t);

struct ClientID
{
    union
    {
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

struct AcquireRequest
{
    enum RequestType type;
    ClientID cid;
    id_t key;
    size_t size;
    term_t require_term;
    uint16_t dir_id;
    Debug<uint64_t> digest;
} __attribute__((packed));
static_assert(sizeof(AcquireRequest) < ReliableConnection::kMessageSize);
std::ostream &operator<<(std::ostream &os, const AcquireRequest &req);

struct AcquireResponse
{
    enum RequestType type;
    ClientID cid;
    uint32_t rkey_0;
    uint64_t base;
    term_t term;
    Debug<uint64_t> digest;
} __attribute__((packed));
static_assert(sizeof(AcquireResponse) < ReliableConnection::kMessageSize);
std::ostream &operator<<(std::ostream &os, const AcquireResponse &resp);

enum class AdminFlag : uint8_t
{
    kAdminReqExit = 0,
    kAdminReqRecovery = 1,
};

struct AdminRequest
{
    enum RequestType type;
    ClientID cid;
    uint8_t flag; // enum AdminFlag
    Debug<uint64_t> digest;
    uint16_t dir_id;
} __attribute__((packed));
static_assert(sizeof(AdminRequest) < ReliableConnection::kMessageSize);
std::ostream &operator<<(std::ostream &os, const AdminRequest &resp);

}  // namespace patronus

#endif