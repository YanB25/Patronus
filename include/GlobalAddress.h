#ifndef __GLOBALADDRESS_H__
#define __GLOBALADDRESS_H__

#include "Common.h"

class GlobalAddress
{
public:
    union
    {
        struct
        {
            uint64_t nodeID : 16;
            uint64_t offset : 48;
        };
        uint64_t val;
    };

    operator uint64_t()
    {
        return val;
    }

    GlobalAddress() : val(0)
    {
    }

    static GlobalAddress Null()
    {
        static GlobalAddress zero;
        return zero;
    };
} __attribute__((packed));

static_assert(sizeof(GlobalAddress) == sizeof(uint64_t), "XXX");

static inline std::ostream &operator<<(std::ostream &os,
                                       const GlobalAddress &addr)
{
    os << "{GA nodeId: " << addr.nodeID << ", off: " << addr.offset << "} ";
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

#endif /* __GLOBALADDRESS_H__ */
