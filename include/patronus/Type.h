#pragma once

#ifndef PATRONUS_TYPE_H_
#define PATRONUS_TYPE_H_

#include <cstdint>
#include <cstdlib>

namespace patronus
{

using id_t = uint64_t;
using term_t = int64_t;
using rkey_t = uint32_t;

struct ClientID
{
    union
    {
        struct
        {
            uint16_t node_id;
            uint16_t thread_id;
            uint8_t mid;
            uint8_t coro_id;
            uint16_t rpc_ctx_id;
            // below two idx is used for internal management
            // uint8_t lease_idx;
            // uint8_t buffer_idx;
        } __attribute__((packed));
        uint64_t cid;
    };
} __attribute__((packed));
static_assert(sizeof(ClientID) == sizeof(uint64_t));

struct AcquireRequest
{
    ClientID cid;
    id_t key;
    term_t require_term;
} __attribute__((packed));
static_assert(sizeof(AcquireRequest) < ReliableConnection::kMessageSize);

struct AcquireResponse
{
    term_t term;
} __attribute__((packed));
static_assert(sizeof(AcquireResponse) < ReliableConnection::kMessageSize);


}

#endif