#pragma once
#ifndef SHERMEM_WRID_H_
#define SHERMEM_WRID_H_

#include <cinttypes>
#include <iostream>
#include <limits>

#define WRID_PREFIX_EXMETA 1
#define WRID_PREFIX_RELIABLE_SEND 2
#define WRID_PREFIX_RELIABLE_RECV 3
#define WRID_PREFIX_PATRONUS_RW 4
#define WRID_PREFIX_PATRONUS_BIND_MW 5
#define WRID_PREFIX_PATRONUS_UNBIND_MW 6
// PR: ProtectionRegion
#define WRID_PREFIX_PATRONUS_PR_RW 7
#define WRID_PREFIX_PATRONUS_CAS 8
#define WRID_PREFIX_PATRONUS_PR_CAS 9
#define WRID_PREFIX_PATRONUS_BATCH_RWCAS 10
#define WRID_PREFIX_PATRONUS_GENERATE_FAULT 11
// The outer-most benchmark only prefix
#define WRID_PREFIX_BENCHMARK_ONLY 12
#define WRID_PREFIX_NULLWRID 13
#define WRID_PREFIX_PATRONUS_BIND_MW_MAGIC_ERR 14
#define WRID_PREFIX_PATRONUS_BATCH 15

constexpr uint32_t get_WRID_ID_RESERVED()
{
    return std::numeric_limits<uint32_t>::max();
}

struct pre_wrid_prefix
{
    pre_wrid_prefix(uint64_t p) : prefix(p)
    {
    }
    uint64_t prefix;
};

inline std::ostream &operator<<(std::ostream &os, pre_wrid_prefix p)
{
    switch (p.prefix)
    {
    case WRID_PREFIX_EXMETA:
        os << "EXMETA";
        return os;
    case WRID_PREFIX_RELIABLE_SEND:
        os << "RELIABLE_SEND";
        return os;
    case WRID_PREFIX_RELIABLE_RECV:
        os << "RELIABLE_RECV";
        return os;
    case WRID_PREFIX_PATRONUS_RW:
        os << "PATRONUS_RW";
        return os;
    case WRID_PREFIX_PATRONUS_BIND_MW:
        os << "PATRONUS_BIND_MW";
        return os;
    case WRID_PREFIX_PATRONUS_UNBIND_MW:
        os << "PATRONUS_UNBIND_MW";
        return os;
    case WRID_PREFIX_PATRONUS_PR_RW:
        os << "PATRONUS_PR_RW";
        return os;
    case WRID_PREFIX_PATRONUS_CAS:
        os << "PATRONUS_CAS";
        return os;
    case WRID_PREFIX_PATRONUS_PR_CAS:
        os << "PATRONUS_PR_CAS";
        return os;
    case WRID_PREFIX_PATRONUS_BATCH_RWCAS:
        os << "PATRONUS_BATCH_RWCAS";
        return os;
    case WRID_PREFIX_PATRONUS_GENERATE_FAULT:
        os << "PATRONUS_GENERATE_FAULT";
        return os;
    case WRID_PREFIX_BENCHMARK_ONLY:
        os << "BENCHMARK_ONLY";
        return os;
    case WRID_PREFIX_NULLWRID:
        os << "NULL_WRID";
        return os;
    case WRID_PREFIX_PATRONUS_BIND_MW_MAGIC_ERR:
        os << "PATRONUS_BIND_MW_MAGIC_ERR";
        return os;
    case WRID_PREFIX_PATRONUS_BATCH:
        os << "PATRONUS_BATCH";
        return os;
    default:
        os << "UNKNOWN";
        return os;
    }
    return os;
}

struct WRID
{
    WRID(uint16_t p, uint16_t a16, uint16_t b16, uint16_t c16)
        : prefix(p), u16_a(a16), u16_b(b16), u16_c(c16)
    {
    }
    WRID(uint64_t v) : val(v)
    {
    }
    WRID(uint16_t p, uint32_t id) : prefix(p), u16_a(0), id(id)
    {
    }
    WRID(uint16_t p, uint16_t a16, uint32_t id) : prefix(p), u16_a(a16), id(id)
    {
    }
    bool operator==(const WRID &rhs) const
    {
        return val == rhs.val;
    }
    bool operator!=(const WRID &rhs) const
    {
        return !(val == rhs.val);
    }
    union
    {
        struct
        {
            uint16_t prefix;
            uint16_t u16_a;
            union
            {
                struct
                {
                    uint16_t u16_b;
                    uint16_t u16_c;
                } __attribute__((packed));
                uint32_t id;
            };
        } __attribute__((packed));
        uint64_t val;
    };
} __attribute__((packed));
inline std::ostream &operator<<(std::ostream &os, WRID wrid)
{
    os << "{WRID prefix: " << pre_wrid_prefix(wrid.prefix)
       << ", a: " << wrid.u16_a << ", b: " << wrid.u16_b
       << ", c: " << wrid.u16_c << "/ id: " << wrid.id << "/ val: " << wrid.val
       << "}";
    return os;
}

static WRID nullwrid{WRID_PREFIX_NULLWRID, get_WRID_ID_RESERVED()};

#endif