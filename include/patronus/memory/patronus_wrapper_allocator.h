#pragma once
#ifndef MEMORY_PATRONUS_WRAPPER_ALLOCATOR_H_
#define MEMORY_PATRONUS_WRAPPER_ALLOCATOR_H_

#include "allocator.h"
#include "patronus/Patronus.h"

namespace patronus::mem
{
class PatronusWrapperAllocator : public IAllocator
{
public:
    using pointer = std::shared_ptr<PatronusWrapperAllocator>;
    PatronusWrapperAllocator(Patronus::pointer patronus,
                             uint32_t node_id,
                             uint32_t dir_id,
                             uint64_t hint)
        : patronus_(patronus),
          dsm_(patronus->get_dsm()),
          node_id_(node_id),
          dir_id_(dir_id),
          hint_(hint)
    {
    }
    static pointer new_instance(Patronus::pointer patronus,
                                uint32_t node_id,
                                uint32_t dir_id,
                                uint64_t hint)
    {
        return std::make_shared<PatronusWrapperAllocator>(
            patronus, node_id, dir_id, hint);
    }
    void *alloc(size_t size, CoroContext *ctx = nullptr) override
    {
        auto flag = (uint8_t) AcquireRequestFlag::kNoGc |
                    (uint8_t) AcquireRequestFlag::kOnlyAllocation;

        auto lease = patronus_->get_wlease(GlobalAddress(node_id_, hint_),
                                           dir_id_,
                                           size,
                                           0ns,
                                           flag,
                                           DCHECK_NOTNULL(ctx));
        if (unlikely(!lease.success()))
        {
            DCHECK_EQ(lease.ec(), AcquireRequestStatus::kNoMem)
                << "** unrecognized fail type.";
            return nullptr;
        }
        DCHECK_NE(lease.base_addr(), 0)
            << "** If lease.success(), should not got nullptr";
        auto buffer_offset =
            dsm_->dsm_offset_to_buffer_offset(lease.base_addr());
        return (void *) GlobalAddress(0, buffer_offset).val;
    }
    void free(void *addr, size_t size, CoroContext *ctx = nullptr) override
    {
        auto gaddr = GlobalAddress(node_id_, (uint64_t) addr);
        return patronus_->dealloc(
            gaddr, dir_id_, size, hint_, DCHECK_NOTNULL(ctx));
    }
    void free(void *addr, [[maybe_unused]] CoroContext *ctx = nullptr) override
    {
        CHECK(false) << "** not supported. addr: " << addr
                     << ", ctx: " << pre_coro_ctx(ctx);
    }

private:
    Patronus::pointer patronus_;
    DSM::pointer dsm_;
    uint32_t node_id_;
    uint32_t dir_id_;
    uint64_t hint_;
};

}  // namespace patronus::mem

#endif