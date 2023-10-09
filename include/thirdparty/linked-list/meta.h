#pragma once
#ifndef PATRONUS_LINKED_LIST_META_H_
#define PATRONUS_LINKED_LIST_META_H_

#include <cinttypes>
#include <cstddef>

#include "GlobalAddress.h"

namespace patronus::list
{
struct Meta
{
    GlobalAddress phead{nullgaddr};
    GlobalAddress ptail{nullgaddr};
    uint64_t push_lock{0};
    uint64_t pop_lock{0};
    static size_t size()
    {
        return sizeof(Meta);
    }
} __attribute__((packed));

inline std::ostream &operator<<(std::ostream &os, const Meta &meta)
{
    os << "{Meta phead: " << meta.phead << ", ptail: " << meta.ptail
       << ", push_lock: " << meta.push_lock << ", pop_lock: " << meta.pop_lock
       << "}";
    return os;
}
}  // namespace patronus::list

#endif