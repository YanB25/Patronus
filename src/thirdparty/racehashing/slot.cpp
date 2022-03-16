#include "thirdparty/racehashing/hashtable.h"

namespace patronus::hash
{
Slot::Slot(uint64_t val)
{
    ptr_.set_val(val);
}

Slot::Slot(uint8_t fp, uint8_t len, void *ptr)
{
    set_ptr(ptr);
    set_fp(fp);
    set_len(len);
}

uint8_t SlotView::fp() const
{
    return ptr_.u8_h();
}
uint8_t SlotView::len() const
{
    return ptr_.u8_l();
}
void *SlotView::ptr() const
{
    return ptr_.ptr();
}
uint64_t SlotView::val() const
{
    return ptr_.val();
}
bool SlotView::empty() const
{
    return ptr_.ptr() == nullptr;
}
SlotView SlotView::view_after_clear() const
{
    auto ret_ptr = ptr_;
    ret_ptr.set_ptr(nullptr);
    return SlotView(ret_ptr);
}

bool SlotView::match(uint8_t _fp) const
{
    return fp() == _fp;
}

uint8_t Slot::fp() const
{
    return ptr_.u8_h();
}
void Slot::set_fp(uint8_t fp)
{
    ptr_.set_u8_h(fp);
}
uint8_t Slot::len() const
{
    return ptr_.u8_l();
}
void Slot::set_len(uint8_t len)
{
    ptr_.set_u8_l(len);
}
void *Slot::ptr() const
{
    return ptr_.ptr();
}
void *Slot::addr() const
{
    return (void *) &ptr_;
}
void Slot::set_ptr(void *_ptr)
{
    ptr_.set_ptr(_ptr);
}
uint64_t Slot::val() const
{
    return ptr_.val();
}

bool Slot::empty() const
{
    return ptr_.ptr() == nullptr;
}

bool Slot::cas(SlotView &expected, const SlotView &desired)
{
    return ptr_.cas(expected.ptr_, desired.ptr_);
}

bool Slot::match(uint8_t _fp) const
{
    return !empty() && fp() == _fp;
}
void Slot::clear()
{
    ptr_.set_val(0);
}
SlotView Slot::view() const
{
    return SlotView(ptr_);
}

}  // namespace patronus::hash