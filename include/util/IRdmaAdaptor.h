#pragma once
#ifndef SHERMEM_IRDMA_ADAPTOR_H_
#define SHERMEM_IRDMA_ADAPTOR_H_

#include <memory>

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
    RemoteMemHandle(GlobalAddress gaddr, size_t size)
        : gaddr_(gaddr), size_(size), valid_(true)
    {
    }
    RemoteMemHandle() : gaddr_(nullgaddr), size_(0), valid_(false)
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

private:
    GlobalAddress gaddr_;
    size_t size_;
    bool valid_{false};
    void *private_data_{nullptr};
};

inline std::ostream &operator<<(std::ostream &os, const RemoteMemHandle &handle)
{
    os << "{RemoteMemHandle: gaddr: " << handle.gaddr()
       << ", size: " << handle.size() << ", valid: " << handle.valid() << "}";
    return os;
}

class IRdmaAdaptor
{
public:
    using pointer = std::shared_ptr<IRdmaAdaptor>;
    using IAllocator = patronus::mem::IAllocator;
    using hint_t = uint64_t;
    IRdmaAdaptor() = default;
    virtual ~IRdmaAdaptor() = default;

    // virtual void reg_default_allocator(IAllocator::pointer) = 0;
    // virtual void reg_allocator(hint_t hint, IAllocator::pointer) = 0;

    // alloc and grant permission
    virtual RemoteMemHandle remote_alloc_acquire_perm(
        size_t, hint_t, CoroContext * = nullptr) = 0;
    // only acquire
    virtual RemoteMemHandle acquire_perm(GlobalAddress gaddr,
                                         size_t,
                                         CoroContext * = nullptr) = 0;
    // free
    virtual void remote_free(GlobalAddress,
                             hint_t,
                             CoroContext * = nullptr) = 0;
    // free + relinquish
    virtual void remote_free_relinquish_perm(RemoteMemHandle &,
                                             hint_t,
                                             CoroContext * = nullptr) = 0;
    // only relinquish
    virtual void relinquish_perm(RemoteMemHandle &,
                                 CoroContext * = nullptr) = 0;

    virtual char *get_rdma_buffer(size_t size) = 0;
    virtual void put_rdma_buffer(void *rdma_buf) = 0;
    virtual RetCode rdma_read(void *rdma_buf,
                              GlobalAddress gaddr,
                              size_t size,
                              RemoteMemHandle &,
                              CoroContext * = nullptr) = 0;
    virtual RetCode rdma_write(GlobalAddress gaddr,
                               void *rdma_buf,
                               size_t size,
                               RemoteMemHandle &,
                               CoroContext * = nullptr) = 0;
    virtual RetCode rdma_cas(GlobalAddress gaddr,
                             uint64_t expect,
                             uint64_t desired,
                             void *rdma_buf,
                             RemoteMemHandle &,
                             CoroContext * = nullptr) = 0;
    virtual RetCode commit(CoroContext * = nullptr) = 0;
    virtual RetCode put_all_rdma_buffer() = 0;

    // called by server side
    virtual GlobalAddress to_exposed_gaddr(void *addr) = 0;
    virtual void *from_exposed_gaddr(GlobalAddress gaddr) = 0;
};

#endif