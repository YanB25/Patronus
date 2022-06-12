#pragma once
#ifndef PATRONUS_CONCURRENT_QUEUE_ENTRY_H_
#define PATRONUS_CONCURRENT_QUEUE_ENTRY_H_

#include <cinttypes>
#include <cstddef>

#include "GlobalAddress.h"

namespace patronus::cqueue
{
template <typename T, size_t kSize>
struct QueueEntry
{
    uint64_t idx{0};
    T entries[kSize];
    const T &entry(size_t idx) const
    {
        DCHECK_LT(idx, kSize);
        return entries[idx];
    }
    T &entry(size_t idx)
    {
        DCHECK_LT(idx, kSize);
        return entries[idx];
    }
    size_t cur_entry_nr() const
    {
        return idx;
    }
} __attribute__((packed));

template <typename T, size_t kSize>
inline std::ostream &operator<<(std::ostream &os,
                                const QueueEntry<T, kSize> &entry)
{
    os << "{QueueEntry with size " << entry.idx << "}";

    return os;
}
}  // namespace patronus::cqueue

#endif