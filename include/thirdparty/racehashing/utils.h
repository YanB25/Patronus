// https://stackoverflow.com/questions/16198700/using-the-extra-16-bits-in-64-bit-pointers
// a little bit modify

#pragma once
#ifndef PATRONUS_RACEHASHING_UTILS_H_
#define PATRONUS_RACEHASHING_UTILS_H_

#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>

#include "Common.h"
#include "util/Debug.h"

namespace patronus::hash
{
using Key = std::string;
using Value = std::string;

enum RetCode
{
    kOk,
    kNotFound,
    kNoMem,
    // failed by multithread conflict.
    kRetry,
    kInvalid,
};
inline uint64_t round_up_to_next_power_of_2(uint64_t x)
{
    return pow(2, ceil(log(x) / log(2)));
}

union UTaggedPtr {
    uint64_t val;
    struct
    {
        uint8_t unused_1;
        uint8_t unused_2;
        uint32_t unused_3;
        uint8_t u8_l;
        uint8_t u8_h;
    } __attribute__((packed));
    struct
    {
        uint16_t unused_4;
        uint32_t unused_5;
        uint16_t u16;
    } __attribute__((packed));
};

template <typename T>
class TaggedPtrImpl
{
public:
    constexpr static uintptr_t kMask = ~(1ull << 48);
    TaggedPtrImpl(void *ptr, uint8_t _u8_h, uint8_t _u8_l)
    {
        set(ptr, _u8_h, _u8_l);
    }
    TaggedPtrImpl(void *_ptr, uint16_t _u16)
    {
        set(_ptr, _u16);
    }
    TaggedPtrImpl(void *_ptr)
    {
        set(_ptr, 0);
    }
    TaggedPtrImpl()
    {
        set(nullptr, 0);
    }

    T *ptr() const
    {
        // sign extend first to make the pointer canonical
        return (T *) (((intptr_t) utagged_ptr_.val << 16) >> 16);
    }
    void set_ptr(void *_ptr)
    {
        set(_ptr, u16());
    }
    uint16_t u16() const
    {
        return utagged_ptr_.u16;
    }
    void set_u16(uint16_t _u16)
    {
        utagged_ptr_.u16 = _u16;
    }
    /**
     * @brief the higher stolen 8 bit higher
     *
     * @return uint8_t
     */
    uint8_t u8_h() const
    {
        return utagged_ptr_.u8_h;
    }
    /**
     * @brief the lower stolen 8 bit lower
     *
     * @return uint8_t
     */
    uint8_t u8_l() const
    {
        return utagged_ptr_.u8_l;
    }

    void set_u8_h(uint8_t _u8_h)
    {
        utagged_ptr_.u8_h = _u8_h;
    }
    void set_u8_l(uint8_t _u8_l)
    {
        utagged_ptr_.u8_l = _u8_l;
    }
    bool cas(TaggedPtrImpl<T> &expected, const TaggedPtrImpl<T> &desired)
    {
        std::atomic<uint64_t> *atm =
            (std::atomic<uint64_t> *) &utagged_ptr_.val;
        return atm->compare_exchange_strong(expected.utagged_ptr_.val,
                                            desired.utagged_ptr_.val,
                                            std::memory_order_acq_rel);
    }
    uint64_t val() const
    {
        return utagged_ptr_.val;
    }
    void set_val(uint64_t val)
    {
        utagged_ptr_.val = val;
    }
    bool operator==(const TaggedPtrImpl<T> &rhs) const
    {
        return utagged_ptr_.val == rhs.utagged_ptr_.val;
    }

private:
    void set(void *_ptr, uint16_t _u16)
    {
        utagged_ptr_.val = (uint64_t) _ptr;
        utagged_ptr_.u16 = _u16;
        DCHECK_EQ(ptr(), _ptr);
        DCHECK_EQ(u16(), _u16);
    }
    void set(void *_ptr, uint8_t _u8_h, uint8_t _u8_l)
    {
        utagged_ptr_.val = (uint64_t) _ptr;
        utagged_ptr_.u8_h = _u8_h;
        utagged_ptr_.u8_l = _u8_l;
        DCHECK_EQ(ptr(), _ptr);
        DCHECK_EQ(u8_h(), _u8_h);
        DCHECK_EQ(u8_l(), _u8_l);
    }
    UTaggedPtr utagged_ptr_;
};
template <typename T>
inline std::ostream &operator<<(std::ostream &os, const TaggedPtrImpl<T> &ptr)
{
    os << "u8_h: " << std::hex << (int) ptr.u8_h()
       << ", u8_l: " << (int) ptr.u8_l() << ", u_16: " << (int) ptr.u16()
       << ", ptr: " << (void *) ptr.ptr();
    return os;
}

// the number of bits for hash value to locate directory
constexpr static size_t M = 16;
// the number of bits for hash value of fingerprint
constexpr static size_t FP = 8;
// the number of bits for hash value to calculate h1(*) and h2(*)
constexpr static size_t H = (64 - M - FP) / 2;
static_assert((64 - M - FP) % 2 == 0);

constexpr static size_t kLenUnit = 64;

inline uint64_t hash_impl(const char *buf, size_t size, uint64_t seed)
{
    uint64_t hash = seed;
    for (size_t i = 0; i < size; ++i)
    {
        hash = ((hash << 5) + hash) + buf[i]; /* hash * 33 + c */
    }
    return hash;
}

// (higher) [ H2 | H1 | FP | M ]  (lower)

// get the bit range (upper, lower]
inline uint64_t hash_from(uint64_t h, size_t lower, size_t upper)
{
    // get the lower @upper bits
    DCHECK_LE(upper, 64);
    DCHECK_LE(lower, upper);

    if (upper != 64)
    {
        h = h & ((1ull << upper) - 1);
    }
    // get rid of the upper bits
    return h >> lower;
}
inline uint64_t hash_m(uint64_t h)
{  // the lowest M bits
    return hash_from(h, 0, M);
}
inline uint64_t hash_fp(uint64_t h)
{
    return hash_from(h, M, FP + M);
}
inline uint64_t hash_1(uint64_t h)
{
    return hash_from(h, FP + M, FP + M + H);
}
inline uint64_t hash_2(uint64_t h)
{
    return hash_from(h, FP + M + H, FP + M + H + H);
}

constexpr bool is_power_of_two(uint64_t x)
{
    return x != 0 && (x & (x - 1)) == 0;
}

inline Key pre(Key key)
{
    if (key.size() > 4)
    {
        key.resize(4);
    }
    return "`" + key + "`";
}

class pre_fp
{
public:
    pre_fp(uint8_t fp) : fp_(fp)
    {
    }
    friend std::ostream &operator<<(std::ostream &os, const pre_fp &);

private:
    uint8_t fp_{0};
};
inline std::ostream &operator<<(std::ostream &os, const pre_fp &fp)
{
    os << std::hex << (int) fp.fp_ << std::dec;
    return os;
}

using TaggedPtr = TaggedPtrImpl<void>;

inline void hash_table_free([[maybe_unused]] void *addr)
{
    LOG_FIRST_N(WARNING, 1) << "Not actually freeing. Implement rcu here.";
}

struct HashContext
{
    HashContext(size_t tid,
                const std::string &key,
                const std::string &value,
                bool enabled = true)
        : tid(tid), key(key), value(value), enabled(enabled)
    {
    }
    size_t tid;
    std::string key;
    std::string value;
    bool enabled{true};
    std::string op;
};

static HashContext nulldctx(0, "", "", false);

inline std::ostream &operator<<(std::ostream &os, const HashContext &dctx)
{
    if constexpr (debug())
    {
        if (dctx.enabled)
        {
            os << "{dctx tid: " << dctx.tid << ", key: `" << dctx.key
               << "`, value: `" << dctx.value << "`} by op: " << dctx.op;
        }
    }
    return os;
}

};  // namespace patronus::hash

#endif