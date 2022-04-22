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
#include "city.h"
#include "util/Debug.h"
#include "util/RetCode.h"

namespace patronus::hash
{
using Key = std::string;
using Value = std::string;

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

constexpr static uint64_t kRaceHashingHashSeed = 5381;

inline uint64_t hash_impl(const char *buf, size_t size)
{
    // uint64_t hash = kRaceHashingHashSeed;
    // for (size_t i = 0; i < size; ++i)
    // {
    //     hash = ((hash << 5) + hash) + buf[i]; /* hash * 33 + c */
    // }
    // return hash;
    return CityHash64(buf, size);
}

// NOTE: make FP next to M
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
inline uint64_t __hash_1(uint64_t h)
{
    return hash_from(h, FP + M, FP + M + H);
}
inline uint64_t __hash_2(uint64_t h)
{
    return hash_from(h, FP + M + H, FP + M + H + H);
}
inline std::pair<uint64_t, uint64_t> hash_h1_h2(uint64_t hash)
{
    auto h1 = __hash_1(hash);
    auto h2 = __hash_2(hash);
    return {h1, h2};
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
    auto flags = std::cout.flags();
    os << std::hex << (int) fp.fp_;
    std::cout.flags(flags);
    return os;
}

class pre_addr
{
public:
    pre_addr(uint64_t addr) : addr_(addr)
    {
    }
    friend std::ostream &operator<<(std::ostream &os, const pre_addr &);

private:
    uint64_t addr_{0};
};
inline std::ostream &operator<<(std::ostream &os, const pre_addr &addr)
{
    auto flags = std::cout.flags();
    os << std::hex << (void *) addr.addr_;
    std::cout.flags(flags);
    return os;
}

class pre_len
{
public:
    pre_len(uint8_t len) : len_(len)
    {
    }
    friend std::ostream &operator<<(std::ostream &os, const pre_len &);

private:
    uint8_t len_{0};
};
inline std::ostream &operator<<(std::ostream &os, const pre_len &len)
{
    os << (int) len.len_;
    return os;
}
class pre_hash
{
public:
    pre_hash(uint64_t hash) : hash_(hash)
    {
    }
    friend std::ostream &operator<<(std::ostream &os, const pre_hash &);

private:
    uint64_t hash_;
};
inline std::ostream &operator<<(std::ostream &os, const pre_hash &ph)
{
    auto flags = std::cout.flags();
    os << std::hex << ph.hash_;
    std::cout.flags(flags);
    return os;
}

class pre_suffix
{
public:
    pre_suffix(uint32_t suffix, size_t len) : suffix_(suffix), len_(len)
    {
    }
    friend std::ostream &operator<<(std::ostream &os, const pre_suffix &);

private:
    uint32_t suffix_;
    size_t len_;
};
inline std::ostream &operator<<(std::ostream &os, const pre_suffix &sfx)
{
    if (sfx.len_ <= 2)
    {
        os << sfx.suffix_ << "(" << std::bitset<2>(sfx.suffix_) << ")";
    }
    else if (sfx.len_ <= 4)
    {
        os << sfx.suffix_ << "(" << std::bitset<4>(sfx.suffix_) << ")";
    }
    else if (sfx.len_ <= 8)
    {
        os << sfx.suffix_ << "(" << std::bitset<8>(sfx.suffix_) << ")";
    }
    else if (sfx.len_ <= 12)
    {
        os << sfx.suffix_ << "(" << std::bitset<12>(sfx.suffix_) << ")";
    }
    else if (sfx.len_ <= 16)
    {
        os << sfx.suffix_ << "(" << std::bitset<16>(sfx.suffix_) << ")";
    }
    else
    {
        os << sfx.suffix_ << "(" << std::bitset<64>(sfx.suffix_) << ")";
    }
    return os;
}

using TaggedPtr = TaggedPtrImpl<void>;

inline void hash_table_free([[maybe_unused]] void *addr)
{
    LOG_FIRST_N(WARNING, 1) << "Not actually freeing. Implement rcu here.";
}

struct HashContext
{
    HashContext(size_t tid, bool enabled = true) : tid(tid), enabled_(enabled)
    {
    }
    void set_private(void *p)
    {
        private_ = p;
    }
    void *get_private()
    {
        return private_;
    }
    size_t tid;
    std::string key;
    std::string value;
    std::string op;
    bool enabled_;
    void *private_{nullptr};
};

static HashContext nulldctx(0, false);

inline std::ostream &operator<<(std::ostream &os, const HashContext &dctx)
{
    if (dctx.enabled_)
    {
        os << "{dctx tid: " << dctx.tid << ", key: `" << dctx.key
           << "`, value: `" << dctx.value << "`} by op: " << dctx.op;
    }
    return os;
}

class pre_dctx
{
public:
    pre_dctx(HashContext *dctx) : dctx_(dctx)
    {
    }
    HashContext *dctx_;
};
inline std::ostream &operator<<(std::ostream &os, const pre_dctx &pdctx)
{
    if (pdctx.dctx_)
    {
        os << *pdctx.dctx_;
    }
    return os;
}

inline uint64_t round_hash_to_bit(uint32_t h, size_t bit)
{
    if (bit == 64)
    {
        return h;
    }
    return h & ((1ull << bit) - 1);
}

inline uint64_t round_to_bits(uint64_t hash, size_t bits)
{
    if (bits == 64)
    {
        return hash;
    }
    return hash & ((1ull << bits) - 1);
}

inline size_t len_to_ptr_len(size_t len)
{
    return (len + (kLenUnit - 1)) / kLenUnit;
}
inline size_t ptr_len_to_len(size_t ptr_len)
{
    return ptr_len * kLenUnit;
}

};  // namespace patronus::hash

#endif