#pragma once
#ifndef PERTRONUS_RACEHASHING_SLOT_H_
#define PERTRONUS_RACEHASHING_SLOT_H_

#include <cstdint>
#include <iostream>

#include "./utils.h"
#include "GlobalAddress.h"

namespace patronus::hash
{
class SlotView;
class Slot
{
public:
    explicit Slot(uint8_t fp, uint8_t len, void *ptr);

    constexpr static size_t size_bytes()
    {
        return 8;
    }

    friend std::ostream &operator<<(std::ostream &, const Slot &);

    SlotView view() const;
    void *addr() const;
    uint64_t val() const;

private:
    uint8_t fp() const;
    void set_fp(uint8_t fp);
    uint8_t len() const;
    void set_len(uint8_t len);
    void *ptr() const;
    void set_ptr(void *_ptr);
    bool empty() const;
    bool cas(SlotView &expected, const SlotView &desired);

    bool match(uint8_t _fp) const;
    void clear();
    Slot(uint64_t val);

    // TODO: actually it is TaggedPtrImpl<KVBlock>
    TaggedPtr ptr_;
} __attribute__((packed));
static_assert(sizeof(Slot) == sizeof(TaggedPtr));
static_assert(sizeof(Slot) == 8);

inline std::ostream &operator<<(std::ostream &os, const Slot &slot)
{
    os << "{Slot: fp: " << pre_fp(slot.fp()) << ", len: " << (int) slot.len()
       << ", ptr: " << slot.ptr() << "}";
    return os;
}

/**
 * @brief A read-only view of the slot
 */
class SlotView
{
public:
    explicit SlotView(uint64_t val)
    {
        ptr_.set_val(val);
    }
    explicit SlotView(TaggedPtr ptr)
    {
        ptr_ = ptr;
    }
    explicit SlotView(uint8_t fp, uint8_t len, GlobalAddress ptr)
    {
        ptr_.set_u8_h(fp);
        ptr_.set_u8_l(len);
        DCHECK_EQ(ptr.nodeID, 0) << "Expect to get higher 16 bits zero";
        ptr_.set_ptr((void *) ptr.val);
    }
    size_t actual_len_bytes() const
    {
        return ptr_len_to_len(len());
    }
    uint8_t fp() const;
    uint8_t len() const;
    GlobalAddress ptr() const;
    uint64_t val() const;
    bool empty() const;

    bool match(uint8_t _fp) const;
    SlotView view_after_clear() const;

    constexpr static size_t size_bytes()
    {
        return 8;
    }

    friend std::ostream &operator<<(std::ostream &, const SlotView &);
    friend class Slot;

private:
    TaggedPtr ptr_;
} __attribute__((packed));
static_assert(sizeof(SlotView) == sizeof(TaggedPtr));
static_assert(sizeof(SlotView) == 8);

inline std::ostream &operator<<(std::ostream &os, const SlotView &slot_view)
{
    os << "{SlotView: " << slot_view.ptr_ << ", fp: " << pre_fp(slot_view.fp())
       << ", len: " << pre_len(slot_view.len()) << ", ptr: " << slot_view.ptr()
       << "}";
    return os;
}

class SlotHandle
{
public:
    SlotHandle(GlobalAddress addr, SlotView slot)
        : addr_(addr), slot_view_(slot)
    {
    }
    GlobalAddress remote_addr() const
    {
        return addr_;
    }
    bool operator<(const SlotHandle &rhs) const
    {
        return addr_ < rhs.addr_;
    }
    bool operator==(const SlotHandle &rhs) const
    {
        return addr_ == rhs.addr_;
    }
    SlotView slot_view() const
    {
        return slot_view_;
    }
    SlotView view_after_clear() const
    {
        return slot_view_.view_after_clear();
    }
    uint64_t val() const
    {
        return slot_view_.val();
    }
    bool match(uint8_t fp) const
    {
        return !slot_view_.empty() && slot_view_.match(fp);
    }
    GlobalAddress ptr() const
    {
        return slot_view_.ptr();
    }
    uint8_t fp() const
    {
        return slot_view_.fp();
    }
    size_t actual_len_bytes() const
    {
        return slot_view_.actual_len_bytes();
    }
    bool empty() const
    {
        return slot_view_.empty();
    }

    friend std::ostream &operator<<(std::ostream &os, const SlotHandle &handle);

private:
    GlobalAddress addr_;
    SlotView slot_view_;
};
inline std::ostream &operator<<(std::ostream &os, const SlotHandle &handle)
{
    os << "{SlotHandle: remote_addr: " << handle.addr_
       << ", slot_view: " << handle.slot_view() << "}";
    return os;
}

class SlotMigrateHandle
{
public:
    SlotMigrateHandle(SlotHandle slot, uint64_t hash)
        : slot_handle_(slot), hash_(hash)
    {
    }
    GlobalAddress remote_addr() const
    {
        return slot_handle_.remote_addr();
    }
    bool operator<(const SlotMigrateHandle &rhs) const
    {
        return slot_handle_ < rhs.slot_handle_;
    }
    bool operator==(const SlotMigrateHandle &rhs) const
    {
        return slot_handle_ == rhs.slot_handle_;
    }
    SlotHandle slot_handle() const
    {
        return slot_handle_;
    }
    SlotView slot_view() const
    {
        return slot_handle_.slot_view();
    }
    SlotView view_after_clear() const
    {
        return slot_handle_.view_after_clear();
    }
    uint64_t val() const
    {
        return slot_handle_.val();
    }

    uint64_t hash() const
    {
        return hash_;
    }

    friend std::ostream &operator<<(std::ostream &os,
                                    const SlotMigrateHandle &handle);

private:
    uint64_t addr_;
    SlotHandle slot_handle_;
    uint64_t hash_;
};
inline std::ostream &operator<<(std::ostream &os,
                                const SlotMigrateHandle &handle)
{
    os << "{SlotMigrateHandle: remote_addr: " << handle.addr_
       << ", slot_handle: " << handle.slot_handle()
       << ", hash: " << pre_hash(handle.hash()) << "}";
    return os;
}

}  // namespace patronus::hash

namespace std
{
template <>
struct hash<patronus::hash::SlotHandle>
{
    std::size_t operator()(const patronus::hash::SlotHandle &v) const
    {
        return v.remote_addr().val;
    }
};
template <>
struct hash<patronus::hash::SlotMigrateHandle>
{
    std::size_t operator()(const patronus::hash::SlotMigrateHandle &v) const
    {
        return v.hash();
    }
};

}  // namespace std

#endif