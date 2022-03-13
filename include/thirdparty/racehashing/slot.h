#pragma once
#ifndef PERTRONUS_RACEHASHING_SLOT_H_
#define PERTRONUS_RACEHASHING_SLOT_H_

#include <cstdint>
#include <iostream>

#include "./utils.h"

namespace patronus::hash
{
class SlotView;
class SlotWithView;
class Slot
{
public:
    explicit Slot(uint8_t fp, uint8_t len, void *ptr);

    constexpr static size_t size_bytes()
    {
        return 8;
    }

    friend std::ostream &operator<<(std::ostream &, const Slot &);
    friend class SlotWithView;

    SlotView view() const;
    SlotWithView with_view() const;
    void *addr() const;

private:
    uint8_t fp() const;
    void set_fp(uint8_t fp);
    uint8_t len() const;
    void set_len(uint8_t len);
    void *ptr() const;
    void set_ptr(void *_ptr);
    uint64_t val() const;
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
    explicit SlotView(uint8_t fp, uint8_t len, void *ptr)
    {
        ptr_.set_u8_h(fp);
        ptr_.set_u8_l(len);
        ptr_.set_ptr(ptr);
    }
    uint8_t fp() const;
    uint8_t len() const;
    void *ptr() const;
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
    os << "{SlotView: " << std::hex << slot_view.ptr_ << "}";
    return os;
}

/**
 * @brief SlowView is a *snapshot* of a Slot, including its address and the
 * value when read.
 *
 */
class SlotWithView
{
public:
    explicit SlotWithView(Slot *slot, SlotView slot_view);
    explicit SlotWithView();
    Slot *slot() const;
    SlotView view() const;
    SlotView view_after_clear() const;
    bool operator<(const SlotWithView &rhs) const;

    // all the query goes to @view_
    uint8_t fp() const;
    uint8_t len() const;
    void *ptr() const;
    uint64_t val() const;
    bool empty() const;
    bool match(uint8_t fp) const;

    // all the modify goes to @slot_
    bool cas(SlotView &expected, const SlotView &desired);
    void set_fp(uint8_t fp);
    void set_len(uint8_t len);
    void set_ptr(void *_ptr);
    void clear();

    friend std::ostream &operator<<(std::ostream &os, const SlotWithView &view);

private:
    Slot *slot_;
    SlotView slot_view_;
};
inline std::ostream &operator<<(std::ostream &os, const SlotWithView &view)
{
    os << "{SlotWithView: " << view.slot_view_ << " at " << (void *) view.slot_
       << "}";
    return os;
}

}  // namespace patronus::hash

#endif