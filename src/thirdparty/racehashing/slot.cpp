#include "thirdparty/racehashing/hashtable.h"

namespace patronus::hash
{
SlotWithView::SlotWithView(Slot *slot, SlotView slot_view)
    : slot_(slot), slot_view_(slot_view)
{
}
SlotWithView::SlotWithView() : slot_(nullptr), slot_view_(0)
{
}
Slot *SlotWithView::slot() const
{
    return slot_;
}
Slot::Slot(uint64_t val)
{
    ptr_.set_val(val);
}
SlotView SlotWithView::view() const
{
    return slot_view_;
}
SlotWithView Slot::with_view() const
{
    return SlotWithView((Slot *) this, view());
}
SlotView SlotWithView::view_after_clear() const
{
    return slot_view_.view_after_clear();
}
bool SlotWithView::operator<(const SlotWithView &rhs) const
{
    return slot_ < rhs.slot_;
}

uint8_t SlotWithView::fp() const
{
    return slot_view_.fp();
}
uint8_t SlotWithView::len() const
{
    return slot_view_.len();
}
void *SlotWithView::ptr() const
{
    return slot_view_.ptr();
}
uint64_t SlotWithView::val() const
{
    return slot_view_.val();
}
bool SlotWithView::empty() const
{
    return slot_view_.empty();
}
bool SlotWithView::match(uint8_t fp) const
{
    return slot_view_.match(fp) && !empty();
}

void SlotWithView::set_fp(uint8_t fp)
{
    slot_->set_fp(fp);
}
void SlotWithView::set_len(uint8_t len)
{
    slot_->set_len(len);
}
void SlotWithView::set_ptr(void *_ptr)
{
    slot_->set_ptr(_ptr);
}
void SlotWithView::clear()
{
    slot_->clear();
}

bool SlotWithView::cas(SlotView &expected, const SlotView &desired)
{
    return slot_->cas(expected, desired);
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