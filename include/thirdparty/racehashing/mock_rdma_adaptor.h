#pragma once
#ifndef PERTRONUS_RACEHASHING_RDMA_H_
#define PERTRONUS_RACEHASHING_RDMA_H_

#include <cinttypes>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <vector>

#include "./conf.h"
#include "./utils.h"
#include "Common.h"
#include "patronus/Type.h"
#include "patronus/memory/allocator.h"
#include "patronus/memory/slab_allocator.h"
#include "util/IRdmaAdaptor.h"
#include "util/Rand.h"

namespace patronus::hash
{
struct MockRdmaOp
{
    void *remote;
    void *buffer;
    int op;  // 0 for read, 1 for write, 2 for cas
    size_t size;
    uint64_t expect;
    uint64_t desired;
};
inline std::ostream &operator<<(std::ostream &os, const MockRdmaOp &op)
{
    os << "{op: remote: " << (void *) op.remote << ", buffer: " << op.buffer
       << ", op: " << op.op << ", size: " << op.size
       << ", expect: " << op.expect << ", desired: " << op.desired << "}";
    return os;
}

class MockRdmaAdaptor : public IRdmaAdaptor
{
public:
    // an offset between client witness Patronus address and the actual vm.
    constexpr static uint64_t kExposedMemOffset = 1ull << 16;
    using pointer = std::shared_ptr<MockRdmaAdaptor>;
    using IAllocator = patronus::mem::IAllocator;
    MockRdmaAdaptor(std::weak_ptr<MockRdmaAdaptor> server_endpoint,
                    HashContext *dctx = nullptr)
        : server_ep_(server_endpoint), dctx_(dctx)
    {
    }
    MockRdmaAdaptor(const MockRdmaAdaptor &) = delete;
    MockRdmaAdaptor &operator=(const MockRdmaAdaptor &) = delete;

    static pointer new_instance(std::weak_ptr<MockRdmaAdaptor> server_endpoint,
                                HashContext *dctx = nullptr)
    {
        return std::make_shared<MockRdmaAdaptor>(server_endpoint, dctx);
    }
    GlobalAddress remote_alloc(size_t size, hint_t hint) override
    {
        auto it = overwrite_allocators_.find(hint);
        if (it != overwrite_allocators_.end())
        {
            return to_exposed_gaddr(it->second->alloc(size));
        }
        return do_remote_alloc(size, hint);
    }
    GlobalAddress do_remote_alloc(size_t size, hint_t hint)
    {
        auto ret = server_ep_.lock()->rpc_alloc(size, hint);
        DLOG_IF(INFO, config::kEnableDebug && dctx_ != nullptr)
            << "[rdma][trace] remote_alloc_acquire_perm: " << ret
            << " for size " << size;
        LOG_FIRST_N(WARNING, 1)
            << "[rdma] mock_rdma_adaptor: remote_alloc: this function is not "
               "checked for correctness: don't know the returned gaddr correct "
               "or not.";
        return ret;
    }
    RetCode extend(RemoteMemHandle &, std::chrono::nanoseconds) override
    {
        return kOk;
    }
    RemoteMemHandle acquire_perm(GlobalAddress gaddr,
                                 hint_t alloc_hint,
                                 size_t size,
                                 std::chrono::nanoseconds ns,
                                 flag_t flag) override
    {
        std::ignore = ns;
        bool with_alloc = flag & (flag_t) AcquireRequestFlag::kWithAllocation;
        bool only_alloc = flag & (flag_t) AcquireRequestFlag::kOnlyAllocation;
        CHECK(!only_alloc) << "use alloc API instead";
        bool alloc_semantics = with_alloc || only_alloc;
        if (alloc_semantics)
        {
            CHECK(gaddr.is_null());
        }
        else
        {
            CHECK(!gaddr.is_null());
            CHECK_EQ(alloc_hint, 0);
        }
        if (!gaddr.is_null())
        {
            CHECK(!alloc_semantics);
            return alloc_handle(
                gaddr, size, patronus::AcquireRequestStatus::kReserved);
        }
        else
        {
            CHECK(alloc_semantics);
            // by semantics, will not hit local overwrite allocators
            return alloc_handle(do_remote_alloc(size, alloc_hint),
                                size,
                                patronus::AcquireRequestStatus::kReserved);
        }
    }
    void reg_default_allocator(IAllocator::pointer allocator)
    {
        allocators_[0] = allocator;
    }
    void reg_allocator(hint_t hint, IAllocator::pointer allocator)
    {
        CHECK_NE(hint, 0) << "hint 0 is reserved for default allocator";
        allocators_[hint] = allocator;
    }
    GlobalAddress rpc_alloc(size_t size, hint_t hint)
    {
        std::lock_guard<std::mutex> lk(rpc_mu_);

        void *ret = nullptr;
        auto it = allocators_.find(hint);
        if (it == allocators_.end())
        {
            // default
            DCHECK_EQ(allocators_.count(0), 1);
            ret = CHECK_NOTNULL(allocators_[0]->alloc(size));
        }
        else
        {
            ret = CHECK_NOTNULL(it->second->alloc(size));
        }

        remote_allocated_buffers_.emplace(ret, size);
        auto remote_gaddr = to_exposed_gaddr(ret);
        DLOG_IF(INFO, config::kEnableDebug && dctx_ != nullptr)
            << "[rdma][trace] rpc_alloc: " << (void *) ret << "("
            << remote_gaddr << ") for size " << size;
        return remote_gaddr;
    }
    void rpc_free(GlobalAddress gaddr, size_t size, hint_t hint)
    {
        std::lock_guard<std::mutex> lk(rpc_mu_);

        auto *addr = from_exposed_gaddr(gaddr);
        CHECK_EQ(remote_allocated_buffers_.count(addr), 1);
        CHECK_EQ(remote_allocated_buffers_[addr], size);
        CHECK_EQ(remote_allocated_buffers_.erase(addr), 1);
        DLOG_IF(INFO, config::kEnableDebug)
            << "[rdma][trace] rpc_free: " << addr;
        // LOG_FIRST_N(WARNING, 1)
        //     << "[race] Will not actually do remote_free here. Otherwise will
        //     "
        //        "segment fault when other clients trying to access the memory
        //        "
        //        "(especially the kv_block)";
        auto it = allocators_.find(hint);
        if (it == allocators_.end())
        {
            DCHECK_NOTNULL(allocators_[0])->free(addr);
        }
        else
        {
            DCHECK_NOTNULL(it->second)->free(addr);
        }
    }
    void remote_free(GlobalAddress gaddr, size_t size, hint_t hint) override
    {
        DLOG_IF(INFO, config::kEnableDebug && dctx_ != nullptr)
            << "[rdma][trace] remote_free: " << gaddr;
        server_ep_.lock()->rpc_free(gaddr, size, hint);
    }
    void relinquish_perm(RemoteMemHandle &handle,
                         hint_t hint,
                         flag_t flag) override
    {
        bool only_dealloc = flag & (flag_t) LeaseModifyFlag::kOnlyDeallocation;
        bool with_dealloc = flag & (flag_t) LeaseModifyFlag::kWithDeallocation;
        CHECK(!only_dealloc) << "use remote_free instead";
        bool dealloc_semantics = only_dealloc || with_dealloc;
        if (dealloc_semantics)
        {
            remote_free(handle.gaddr(), handle.size(), hint);
        }
        else
        {
            // CHECK_EQ(hint, 0);
        }
        free_handle(handle);
        return;
    }
    Buffer get_rdma_buffer(size_t size) override
    {
        void *ret = malloc(size);
        DCHECK_GT(size, 0) << "Make no sense to alloc size with 0";
        allocated_buffers_.insert(ret);
        DLOG_IF(INFO, config::kEnableMemoryDebug && dctx_ != nullptr)
            << "[rdma][trace] get_rdma_buffer: " << (void *) ret << " for size "
            << size;
        return Buffer((char *) ret, size);
    }

    RetCode rdma_read(void *rdma_buf,
                      GlobalAddress gaddr,
                      size_t size,
                      RemoteMemHandle &handle) override
    {
        auto *addr = from_exposed_gaddr(gaddr);
        MockRdmaOp op;
        op.remote = addr;
        op.buffer = rdma_buf;
        op.op = 0;
        op.size = size;
        debug_validate_handle(handle, gaddr, size);

        ops_.emplace_back(std::move(op));
        return kOk;
    }
    RetCode rdma_write(GlobalAddress gaddr,
                       void *rdma_buf,
                       size_t size,
                       RemoteMemHandle &handle) override
    {
        auto *addr = from_exposed_gaddr(gaddr);
        MockRdmaOp op;
        op.remote = addr;
        op.buffer = (char *) rdma_buf;
        op.op = 1;
        op.size = size;
        ops_.emplace_back(std::move(op));

        debug_validate_handle(handle, gaddr, size);

        return kOk;
    }
    RetCode rdma_cas(GlobalAddress gaddr,
                     uint64_t expect,
                     uint64_t desired,
                     void *rdma_buf,
                     RemoteMemHandle &handle) override
    {
        auto *addr = from_exposed_gaddr(gaddr);
        MockRdmaOp op;
        op.op = 2;
        op.remote = addr;
        op.buffer = (char *) rdma_buf;
        op.expect = expect;
        op.desired = desired;
        op.size = 8;
        ops_.emplace_back(std::move(op));

        debug_validate_handle(handle, gaddr, 8);

        return kOk;
    }
    void debug_validate_handle(RemoteMemHandle &handle,
                               GlobalAddress gaddr,
                               size_t size)
    {
        CHECK(handle.valid()) << "** Invalid mem_handle provided: " << handle;
        CHECK_EQ(gaddr.nodeID, handle.gaddr().nodeID)
            << "** Mismatch node detected. gaddr: " << gaddr
            << ", handle.gaddr(): " << handle.gaddr();
        CHECK_GE(gaddr.offset, handle.gaddr().offset)
            << "** Underflow detected. gaddr: " << gaddr << " with offset "
            << gaddr.offset << ", handle.gaddr() " << handle.gaddr()
            << " with offset " << handle.gaddr().offset;
        CHECK_LE((void *) ((char *) gaddr.offset + size),
                 (void *) ((char *) handle.gaddr().offset + handle.size()))
            << "** Overflow detected. gaddr: " << gaddr
            << " with offset to the handle base addr: "
            << (uint64_t) gaddr.offset - handle.gaddr().offset
            << " and r/w size " << size << " overflow handle size "
            << handle.size();
    }
    ~MockRdmaAdaptor()
    {
        for (auto *buf : allocated_buffers_)
        {
            free(buf);
        }
        for (auto *buf : remote_not_freed_buffers_)
        {
            free(buf);
        }
        LOG_IF(WARNING, remote_allocated_buffers_.size() != 0)
            << "[race][rdma] Possible memory leak. May be false positive when "
               "enable rehash.. "
               "Out-going remote buffer nr: "
            << remote_allocated_buffers_.size();
    }

    RetCode commit() override
    {
        for (const auto &op : ops_)
        {
            DCHECK_EQ(allocated_buffers_.count((void *) op.buffer), 1)
                << "Buffer not allocated from this context. buffer: "
                << (void *) op.buffer;
            if (op.op == 0)
            {
                memcpy(op.buffer, (char *) op.remote, op.size);
            }
            else if (op.op == 1)
            {
                memcpy((char *) op.remote, op.buffer, op.size);
            }
            else if (op.op == 2)
            {
                uint64_t expect = op.expect;
                std::atomic<uint64_t> &atm =
                    *(std::atomic<uint64_t> *) op.remote;
                atm.compare_exchange_strong(expect, op.desired);
                DCHECK_EQ(op.size, 8);
                memcpy(op.buffer, (char *) &expect, op.size);
            }
            else
            {
                CHECK(false) << "Unknown op: " << op.op;
            }
        }
        ops_.clear();

        return kOk;
    }

    RetCode put_all_rdma_buffer() override
    {
        for (void *addr : allocated_buffers_)
        {
            free(addr);
            DLOG_IF(INFO, config::kEnableDebug && dctx_ != nullptr)
                << "[rdma][trace] gc: freeing " << (void *) addr;
        }
        allocated_buffers_.clear();
        return kOk;
    }
    GlobalAddress to_exposed_gaddr(void *addr) override
    {
        auto ret =
            GlobalAddress((void *) ((uint64_t) addr + kExposedMemOffset));
        DCHECK_EQ(ret.nodeID, 0);
        if constexpr (debug())
        {
            auto org_gaddr = GlobalAddress(addr);
            auto new_gaddr = ret;
            CHECK_EQ(org_gaddr.nodeID, new_gaddr.nodeID);
        }
        return ret;
    }
    void *from_exposed_gaddr(GlobalAddress gaddr) override
    {
        DCHECK_EQ(gaddr.nodeID, 0);
        auto ret = (void *) (gaddr.val - kExposedMemOffset);
        if constexpr (debug())
        {
            CHECK_GE(gaddr.offset, kExposedMemOffset);
        }

        return ret;
    }
    uint64_t get_handle_id()
    {
        while (true)
        {
            auto hid = fast_pseudo_rand_int();
            if (allocated_handle_.count(hid) == 0)
            {
                allocated_handle_.insert(hid);
                return hid;
            }
        }
    }
    void put_handle_id(uint64_t hid)
    {
        CHECK_EQ(allocated_handle_.count(hid), 1);
        CHECK_EQ(allocated_handle_.erase(hid), 1);
    }

    void reg_overwrite_allocator(uint64_t hint,
                                 mem::IAllocator::pointer allocator) override
    {
        DCHECK_EQ(overwrite_allocators_.count(hint), 0)
            << "** already registered allocator for hint = " << hint;
        DCHECK_NE(hint, 0) << "** try to overwrite default allocator. hint: "
                           << hint;
        overwrite_allocators_[hint] = allocator;
    }

private:
    RemoteMemHandle alloc_handle(GlobalAddress gaddr,
                                 size_t size,
                                 patronus::AcquireRequestStatus ec)
    {
        auto hid = get_handle_id();
        RemoteMemHandle ret(gaddr, size, ec);
        ret.set_private_data((void *) hid);
        return ret;
    }
    void free_handle(RemoteMemHandle &handle)
    {
        auto hid = (uint64_t) handle.private_data();
        put_handle_id(hid);
        handle.set_invalid();
    }
    std::weak_ptr<MockRdmaAdaptor> server_ep_;
    // server-side allocators to handle requests
    std::unordered_map<hint_t, IAllocator::pointer> allocators_;
    HashContext *dctx_{nullptr};
    std::vector<MockRdmaOp> ops_;
    std::unordered_set<void *> allocated_buffers_;
    std::unordered_map<void *, size_t> remote_allocated_buffers_;
    std::unordered_set<void *> remote_not_freed_buffers_;

    std::unordered_set<uint64_t> allocated_handle_;

    // client-side allocators for secondary allocation.
    std::unordered_map<uint64_t, mem::IAllocator::pointer>
        overwrite_allocators_;

    // rpc_alloc, rpc_free is not thread-safe. make it be
    std::mutex rpc_mu_;
};

}  // namespace patronus::hash

#endif