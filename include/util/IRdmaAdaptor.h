#pragma once
#ifndef SHERMEM_IRDMA_ADAPTOR_H_
#define SHERMEM_IRDMA_ADAPTOR_H_

#include <chrono>
#include <memory>

#include "Cache.h"
#include "CoroContext.h"
#include "GlobalAddress.h"
#include "patronus/memory/allocator.h"
#include "util/RetCode.h"

/**
 * @brief Stand for the permission over a range of memory
 */
class RemoteMemHandle
{
public:
    RemoteMemHandle(GlobalAddress gaddr,
                    size_t size,
                    patronus::AcquireRequestStatus ec)
        : gaddr_(gaddr), size_(size), valid_(true), ec_(ec)
    {
    }
    RemoteMemHandle()
        : gaddr_(nullgaddr),
          size_(0),
          valid_(false),
          ec_(patronus::AcquireRequestStatus::kReserved)
    {
    }
    GlobalAddress gaddr() const
    {
        return gaddr_;
    }
    size_t size() const
    {
        return size_;
    }
    void set_invalid()
    {
        valid_ = false;
    }
    bool valid() const
    {
        return valid_;
    }
    void *private_data() const
    {
        return private_data_;
    }
    void set_private_data(void *p)
    {
        private_data_ = p;
    }
    // use it with care!
    // not include valid_
    bool operator==(const RemoteMemHandle &rhs) const
    {
        return gaddr_ == rhs.gaddr_ && size_ == rhs.size_ &&
               private_data_ == rhs.private_data_;
    }
    patronus::AcquireRequestStatus ec() const
    {
        return ec_;
    }

private:
    GlobalAddress gaddr_;
    size_t size_;
    bool valid_{false};
    void *private_data_{nullptr};
    patronus::AcquireRequestStatus ec_;
};

inline std::ostream &operator<<(std::ostream &os, const RemoteMemHandle &handle)
{
    os << "{RemoteMemHandle: gaddr: " << handle.gaddr()
       << ", size: " << handle.size() << ", valid: " << handle.valid()
       << ", private_data: " << (void *) handle.private_data() << "}";
    return os;
}

namespace std
{
template <>
struct hash<RemoteMemHandle>
{
    std::size_t operator()(const RemoteMemHandle &rhs) const
    {
        return rhs.gaddr().val;
    }
};
}  // namespace std

/**
 * @brief IRdmaAdaptor has nearly the same API as @see Patronus,
 * except that it remembers the node_id and dir_id,
 * and (may) hold the coroutine internally.
 *
 */
class IRdmaAdaptor
{
public:
    using flag_t = patronus::flag_t;
    using pointer = std::shared_ptr<IRdmaAdaptor>;
    using IAllocator = patronus::mem::IAllocator;
    using hint_t = uint64_t;
    IRdmaAdaptor() = default;
    virtual ~IRdmaAdaptor() = default;

    // only alloc
    [[nodiscard]] virtual GlobalAddress remote_alloc(size_t size, hint_t) = 0;
    // all operations other than only alloc
    [[nodiscard]] virtual RemoteMemHandle acquire_perm(
        GlobalAddress gaddr,
        hint_t alloc_hint,
        size_t size,
        std::chrono::nanoseconds ns,
        flag_t flag) = 0;
    [[nodiscard]] virtual RetCode extend(RemoteMemHandle &,
                                         std::chrono::nanoseconds) = 0;
    // free only
    virtual void remote_free(GlobalAddress, size_t size, hint_t) = 0;
    // all rel operations other than free-only
    virtual void relinquish_perm(RemoteMemHandle &, hint_t, flag_t flag) = 0;

    [[nodiscard]] virtual Buffer get_rdma_buffer(size_t size) = 0;
    // use the put_all_rdma_buffer API.
    // virtual void put_rdma_buffer(void *rdma_buf) = 0;
    [[nodiscard]] virtual RetCode rdma_read(void *rdma_buf,
                                            GlobalAddress gaddr,
                                            size_t size,
                                            RemoteMemHandle &) = 0;
    [[nodiscard]] virtual RetCode rdma_write(GlobalAddress gaddr,
                                             void *rdma_buf,
                                             size_t size,
                                             RemoteMemHandle &) = 0;
    [[nodiscard]] virtual RetCode rdma_cas(GlobalAddress gaddr,
                                           uint64_t expect,
                                           uint64_t desired,
                                           void *rdma_buf,
                                           RemoteMemHandle &) = 0;
    [[nodiscard]] virtual RetCode rdma_faa(GlobalAddress gaddr,
                                           int64_t value,
                                           void *rdma_buf,
                                           RemoteMemHandle &) = 0;
    [[nodiscard]] virtual RetCode commit() = 0;
    virtual void put_all_rdma_buffer() = 0;
    /**
     * @brief register a secondary allocator at the client side
     * Don't confuse with patronus_->reg_allocator, which registers allocators
     * at the server side
     * @param hint
     */
    virtual void reg_overwrite_allocator(
        uint64_t hint, patronus::mem::IAllocator::pointer) = 0;

    // called by server side
    virtual GlobalAddress to_exposed_gaddr(void *addr) = 0;
    virtual void *from_exposed_gaddr(GlobalAddress gaddr) = 0;

    virtual void enable_trace(const void *any_data)
    {
        std::ignore = any_data;
    }
    virtual void end_trace(const void *any_data)
    {
        std::ignore = any_data;
    }
    virtual void trace_pin(const std::string_view name)
    {
        std::ignore = name;
    }
    virtual bool trace_enabled() const
    {
        return false;
    }
};

#endif