#pragma once
#ifndef PATRONUS_CONCURRENT_QUEUE_META_H_
#define PATRONUS_CONCURRENT_QUEUE_META_H_

#include <cinttypes>
#include <cstddef>

#include "GlobalAddress.h"

namespace patronus::cqueue
{
struct Meta
{
    uint64_t client_nr{0};
    uint64_t front{0};
    uint64_t rear{0};
    GlobalAddress client_witness;
    GlobalAddress client_finished;
    GlobalAddress entries_gaddr;
    constexpr static size_t size()
    {
        return sizeof(Meta);
    }
    size_t witness_buf_size() const
    {
        return sizeof(uint64_t) * client_nr;
    }
    size_t finished_buf_size() const
    {
        return sizeof(uint64_t) * client_nr;
    }

} __attribute__((packed));

inline std::ostream &operator<<(std::ostream &os, const Meta &meta)
{
    os << "{Meta client_nr: " << meta.client_nr
       << ", witness_gaddr: " << meta.client_witness
       << ", finished_gaddr: " << meta.client_finished
       << ", entries_gaddr: " << meta.entries_gaddr << "}";
    return os;
}
}  // namespace patronus::cqueue

#endif