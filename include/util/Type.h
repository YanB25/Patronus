#pragma once
#ifndef SHERMEM_TYPE_H_
#define SHERMEM_TYPE_H_

#include <cinttypes>
#include <cstddef>
#include <iostream>

struct compound_uint64_t
{
    explicit compound_uint64_t(uint64_t u64) : u64_1(u64)
    {
    }
    explicit compound_uint64_t(uint32_t u32_1, uint32_t u32_2)
        : u32_1(u32_1), u32_2(u32_2)
    {
    }
    explicit compound_uint64_t(uint16_t u16_1,
                               uint16_t u16_2,
                               uint16_t u16_3,
                               uint16_t u16_4)
        : u16_1(u16_1), u16_2(u16_2), u16_3(u16_3), u16_4(u16_4)
    {
    }
    union {
        uint64_t u64_1;
        uint64_t val;
        struct
        {
            uint32_t u32_1;
            uint32_t u32_2;
        } __attribute((packed));
        struct
        {
            uint16_t u16_1;
            uint16_t u16_2;
            uint16_t u16_3;
            uint16_t u16_4;
        } __attribute((packed));
        struct
        {
            uint8_t u8_1;
            uint8_t u8_2;
            uint8_t u8_3;
            uint8_t u8_4;
            uint8_t u8_5;
            uint8_t u8_6;
            uint8_t u8_7;
            uint8_t u8_8;
        } __attribute((packed));
    };
};
static_assert(sizeof(compound_uint64_t) == sizeof(uint64_t));

inline std::ostream &operator<<(std::ostream &os, compound_uint64_t val)
{
    os << "[" << val.val << "; " << val.u32_1 << "," << val.u32_2 << "; "
       << val.u16_1 << "," << val.u16_2 << "," << val.u16_3 << "," << val.u16_4
       << "]";

    return os;
}

inline bool operator==(uint64_t lhs, compound_uint64_t rhs)
{
    return lhs == rhs.val;
}
inline bool operator==(compound_uint64_t lhs, uint64_t rhs)
{
    return lhs.val == rhs;
}
inline bool operator!=(uint64_t lhs, compound_uint64_t rhs)
{
    return !(lhs == rhs);
}
inline bool operator!=(compound_uint64_t lhs, uint64_t rhs)
{
    return !(lhs == rhs);
}

#endif