#pragma once
#ifndef __GLOBALADDRESS_H__
#define __GLOBALADDRESS_H__

#include "Common.h"

class GlobalAddress
{
public:
    union {
        struct
        {
            uint64_t offset : 48;
            uint64_t nodeID : 16;
        };
        uint64_t val;
    };

    explicit GlobalAddress() : val(0)
    {
    }
    explicit GlobalAddress(void *addr) : val((uint64_t) addr)
    {
    }
    explicit GlobalAddress(uint16_t node_id, uint64_t offset)
        : offset(offset), nodeID(node_id)
    {
    }
    bool is_null() const
    {
        return offset == 0;
    }

    static GlobalAddress Null()
    {
        static GlobalAddress zero;
        return zero;
    };
    GlobalAddress operator+(ssize_t offset) const
    {
        if constexpr (debug())
        {
            auto old_val = val;
            auto new_val = val + offset;
            auto old_gaddr = GlobalAddress((void *) old_val);
            auto new_gaddr = GlobalAddress((void *) new_val);
            CHECK_EQ(old_gaddr.nodeID, new_gaddr.nodeID)
                << "** gaddr overflow detected. old_gaddr.offset "
                << old_gaddr.offset << ", new_gaddr.offset "
                << new_gaddr.offset;
        }
        return GlobalAddress((char *) val + offset);
    }
    GlobalAddress operator-(ssize_t off) const
    {
        DCHECK_GE(offset, off) << "** gaddr overflow detected";
        return GlobalAddress((char *) val - offset);
    }
} __attribute__((packed));

static_assert(sizeof(GlobalAddress) == sizeof(uint64_t), "XXX");

static inline std::ostream &operator<<(std::ostream &os,
                                       const GlobalAddress &addr)
{
    os << "{gaddr nodeId: " << addr.nodeID << ", off: " << (void *) addr.offset
       << ", local: " << (void *) addr.val << "} ";
    return os;
}

inline GlobalAddress GADD(const GlobalAddress &addr, int off)
{
    auto ret = addr;
    ret.offset += off;
    return ret;
}

inline bool operator==(const GlobalAddress &lhs, const GlobalAddress &rhs)
{
    return (lhs.nodeID == rhs.nodeID) && (lhs.offset == rhs.offset);
}

inline bool operator!=(const GlobalAddress &lhs, const GlobalAddress &rhs)
{
    return !(lhs == rhs);
}
inline bool operator<(const GlobalAddress &lhs, const GlobalAddress &rhs)
{
    return lhs.val < rhs.val;
}

static GlobalAddress nullgaddr;

#endif /* __GLOBALADDRESS_H__ */
