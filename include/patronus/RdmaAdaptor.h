#pragma once
#ifndef PATRONUS_RDMA_ADAPTOR_H_
#define PATRONUS_RDMA_ADAPTOR_H_

#include <array>

#include "patronus/Patronus.h"
#include "util/IRdmaAdaptor.h"

namespace patronus
{
struct RdmaTraceRecord
{
    std::string name;
    size_t rwcas_nr;
    size_t alloc_only_nr;
    size_t acquire_nr;
    size_t alloc_acquire_nr;
    size_t free_only_nr;
    size_t free_rel_nr;
    size_t rel_nr;
    size_t read_nr;
    size_t read_bytes;
    size_t write_nr;
    size_t write_bytes;
    size_t cas_nr;
    size_t allocated_rdma_buf_nr = 0;
    size_t commit_nr = 0;
    size_t indv_one_sided = 0;
    size_t indv_two_sided = 0;
    uint64_t latency_ns;
    ChronoTimer timer;
    ContTimer<::config::kEnableRdmaTrace> trace_timer;

    void init(const std::string &pname)
    {
        name = pname;
        timer.pin();
        trace_timer.init(pname);
    }

    void clear()
    {
        name = "";
        rwcas_nr = 0;
        alloc_only_nr = 0;
        acquire_nr = 0;
        alloc_acquire_nr = 0;
        free_only_nr = 0;
        free_rel_nr = 0;
        read_nr = 0;
        read_bytes = 0;
        rel_nr = 0;
        write_nr = 0;
        write_bytes = 0;
        cas_nr = 0;
        latency_ns = 0;
        commit_nr = 0;
        indv_one_sided = 0;
        indv_two_sided = 0;
        allocated_rdma_buf_nr = 0;
        trace_timer.clear();
    }
    void trace_pin(const std::string_view name)
    {
        trace_timer.pin(std::string(name));
    }
};
inline std::ostream &operator<<(std::ostream &os, const RdmaTraceRecord &r)
{
    os << "{Rdma " << r.name << ", read: " << r.read_nr << " (" << r.read_bytes
       << " B), write: " << r.write_nr << " (" << r.write_bytes
       << " B), cas: " << r.cas_nr << ". alloc: " << r.alloc_only_nr
       << ", acquire: " << r.acquire_nr
       << ", alloc_acquire: " << r.alloc_acquire_nr
       << ", free: " << r.free_only_nr << ", rel: " << r.rel_nr
       << ", free_rel: " << r.free_rel_nr
       << ", used rdma buffer: " << r.allocated_rdma_buf_nr
       << ", latency: " << r.latency_ns << " ns}";
    os << std::endl;
    os << r.trace_timer;
    return os;
}
class RdmaAdaptor : public IRdmaAdaptor
{
public:
    constexpr static size_t kMaxOngoingRdmaBuf = 2048;
    constexpr static size_t kWarningOngoingRdmaBuf = 16;
    /**
     * @brief Construct a new Rdma Adaptor object
     *
     * @param node_id the node_id for client. Not used for server
     * @param patronus
     */
    RdmaAdaptor(uint16_t node_id,
                uint32_t dir_id,
                Patronus::pointer patronus,
                bool is_server,
                CoroContext *ctx)
        : node_id_(node_id),
          dir_id_(dir_id),
          patronus_(patronus),
          coro_ctx_(ctx),
          is_server_(is_server)
    {
        ongoing_rdma_bufs_.reserve(kMaxOngoingRdmaBuf);
    }
    ~RdmaAdaptor()
    {
        if constexpr (debug())
        {
            if (!ongoing_remote_handle_.get().empty())
            {
                const auto &handle = *ongoing_remote_handle_.get().begin();
                LOG(WARNING) << "[rdma-adpt] possible handle leak for "
                             << handle << ". known leak nr: "
                             << ongoing_remote_handle_.get().size()
                             << ". Note: this could be a false positive if "
                                "auto-gc (i.e. lease semantics) is enabled.";
            }
        }
    }
    // for client
    static pointer new_instance(uint16_t node_id,
                                uint32_t dir_id,
                                Patronus::pointer patronus,
                                CoroContext *ctx)
    {
        return std::make_shared<RdmaAdaptor>(
            node_id, dir_id, patronus, false, ctx);
    }
    // for server
    static pointer new_instance(Patronus::pointer patronus)
    {
        return std::make_shared<RdmaAdaptor>(0, 0, patronus, true, nullptr);
    }

    // yes
    /**
     * The manipulation of address:
     * Lease& lease = *(Lease *) handle.private_data();
     * The dsm offset: lease.base_address()
     * The buffer offset: handle.gaddr()
     * What RdmaAdaptor exposes: buffer offset
     * What Patronus accepts: buffer offset
     */
    RemoteMemHandle acquire_perm(GlobalAddress gaddr,
                                 hint_t alloc_hint,
                                 size_t size,
                                 std::chrono::nanoseconds ns,
                                 flag_t flag) override
    {
        if constexpr (::config::kEnableRdmaTrace)
        {
            if (trace_enabled())
            {
                rdma_trace_record_.alloc_acquire_nr++;
                rdma_trace_record_.indv_two_sided++;
            }
        }
        DCHECK(gaddr.nodeID == 0 || gaddr.nodeID == node_id_)
            << "Invalid gaddr.nodeID: " << gaddr.nodeID
            << " with node_id_: " << node_id_ << ". gaddr: " << gaddr;
        gaddr.nodeID = 0;

        DCHECK(!is_server_);

        auto lease = patronus_->get_wlease(
            node_id_, dir_id_, gaddr, alloc_hint, size, ns, flag, coro_ctx_);
        if (likely(lease.success()))
        {
            return alloc_handle(lease_to_exposed_gaddr(lease), size, lease);
        }
        else if (likely(lease.ec() == AcquireRequestStatus::kMagicMwErr))
        {
            // okay, do it again
            return acquire_perm(gaddr, alloc_hint, size, ns, flag);
        }
        else
        {
            if (lease.ec() != AcquireRequestStatus::kNoMem &&
                lease.ec() != AcquireRequestStatus::kNoMw &&
                lease.ec() != AcquireRequestStatus::kLockedErr)
            {
                CHECK(false) << "** Unexpected lease failure: " << lease.ec()
                             << ". expect kNoMem or kNoMw. Lease " << lease
                             << ". Lease success: " << lease.success();
            }
            return alloc_handle(nullgaddr, 0, lease);
        }
    }
    RetCode extend(RemoteMemHandle &handle,
                   std::chrono::nanoseconds ns) override
    {
        auto &lease = *(Lease *) handle.private_data();
        return patronus_->extend(lease, ns, 0 /* flag */, coro_ctx_);
    }
    GlobalAddress lease_to_exposed_gaddr(const Lease &lease) const
    {
        CHECK(lease.success());
        auto gaddr = patronus_->get_gaddr(lease);
        gaddr.nodeID = 0;
        return gaddr;
    }

    // TODO:
    GlobalAddress remote_alloc(size_t size, hint_t hint) override
    {
        if constexpr (::config::kEnableRdmaTrace)
        {
            if (trace_enabled())
            {
                rdma_trace_record_.alloc_only_nr++;
                rdma_trace_record_.indv_two_sided++;
            }
        }
        auto it = overwrite_allocators_.find(hint);
        if (unlikely(it != overwrite_allocators_.end()))
        {
            return GlobalAddress(0,
                                 (uint64_t) it->second->alloc(size, coro_ctx_));
        }

        DCHECK(!is_server_);
        auto gaddr = patronus_->alloc(node_id_, dir_id_, size, hint, coro_ctx_);
        gaddr.nodeID = 0;
        return gaddr;
    }
    void remote_free(GlobalAddress vaddr, size_t size, hint_t hint) override
    {
        if (vaddr.is_null())
        {
            // freeing nullptr is always valid
            return;
        }
        if constexpr (::config::kEnableRdmaTrace)
        {
            if (trace_enabled())
            {
                rdma_trace_record_.free_only_nr++;
                rdma_trace_record_.indv_two_sided++;
            }
        }

        auto gaddr = vaddr_to_gaddr(vaddr);
        patronus_->dealloc(gaddr, dir_id_, size, hint, coro_ctx_);
    }
    void relinquish_perm(RemoteMemHandle &handle,
                         hint_t hint,
                         flag_t flag) override
    {
        CHECK(handle.valid());
        if constexpr (::config::kEnableRdmaTrace)
        {
            if (trace_enabled())
            {
                rdma_trace_record_.free_rel_nr++;
                rdma_trace_record_.indv_two_sided++;
            }
        }
        auto &lease = *(Lease *) handle.private_data();
        patronus_->relinquish(lease, hint, flag, coro_ctx_);
        free_handle(handle);
    }

    // yes
    Buffer get_rdma_buffer(size_t size) override
    {
        if constexpr (::config::kEnableRdmaTrace)
        {
            if (trace_enabled())
            {
                rdma_trace_record_.allocated_rdma_buf_nr++;
            }
        }

        auto ret = patronus_->get_rdma_buffer(size);
        if (ret.buffer == nullptr)
        {
            return Buffer(nullptr, 0);
        }
        DCHECK_GE(ret.size, size);
        ongoing_rdma_bufs_.push_back(ret);
        CHECK_LT(ongoing_rdma_bufs_.size(), kMaxOngoingRdmaBuf)
            << rdma_trace_record_;
        if (unlikely(ongoing_rdma_bufs_.size() == kWarningOngoingRdmaBuf))
        {
            DLOG(WARNING)
                << "** Got too much rdma buffer. Only log once for this."
                << rdma_trace_record_;
        }

        return ret;
    }
    RetCode put_all_rdma_buffer() override
    {
        for (auto buf : ongoing_rdma_bufs_)
        {
            patronus_->put_rdma_buffer(buf);
        }
        ongoing_rdma_bufs_.clear();
        DCHECK_GE(ongoing_rdma_bufs_.capacity(), kMaxOngoingRdmaBuf);
        return kOk;
    }
    RetCode rdma_read(void *rdma_buf,
                      GlobalAddress vaddr,
                      size_t size,
                      RemoteMemHandle &handle) override
    {
        CHECK(handle.valid());
        auto gaddr = vaddr_to_gaddr(vaddr);
        auto &lease = *(Lease *) handle.private_data();
        CHECK_GE(gaddr.offset, handle.gaddr().offset);
        auto offset = gaddr.offset - handle.gaddr().offset;
        auto flag = (flag_t) RWFlag::kNoLocalExpireCheck;
        auto ec = patronus_->prepare_read(
            batch_, lease, (char *) rdma_buf, size, offset, flag, coro_ctx_);
        if (unlikely(ec == kNoMem))
        {
            CHECK_EQ(patronus_->commit(batch_, coro_ctx_), kOk);
            DCHECK(batch_.empty());
            return rdma_read(rdma_buf, vaddr, size, handle);
        }

        if constexpr (::config::kEnableRdmaTrace)
        {
            if (trace_enabled())
            {
                rdma_trace_record_.read_nr++;
                rdma_trace_record_.read_bytes += size;
                rdma_trace_record_.indv_one_sided++;
            }
        }
        return ec;
    }
    // handle.gaddr() => lease.buffer_base (dsm offset)
    // vaddr => is gaddr without node_id => dsm offset
    // patronus_->read/write(@offset) => the offset to lease.buffer_base
    RetCode rdma_write(GlobalAddress vaddr,
                       void *rdma_buf,
                       size_t size,
                       RemoteMemHandle &handle) override
    {
        CHECK(handle.valid());
        auto gaddr = vaddr_to_gaddr(vaddr);
        auto &lease = *(Lease *) handle.private_data();
        CHECK_GE(gaddr.offset, handle.gaddr().offset);
        auto offset = gaddr.offset - handle.gaddr().offset;
        auto flag = (flag_t) RWFlag::kNoLocalExpireCheck;
        auto ec = patronus_->prepare_write(
            batch_, lease, (char *) rdma_buf, size, offset, flag, coro_ctx_);
        if (unlikely(ec == kNoMem))
        {
            CHECK_EQ(patronus_->commit(batch_, coro_ctx_), kOk);
            DCHECK(batch_.empty());
            return rdma_write(vaddr, rdma_buf, size, handle);
        }

        if constexpr (::config::kEnableRdmaTrace)
        {
            if (trace_enabled())
            {
                rdma_trace_record_.write_nr++;
                rdma_trace_record_.write_bytes += size;
                rdma_trace_record_.indv_one_sided++;
            }
        }

        return ec;
    }
    RetCode rdma_cas(GlobalAddress vaddr,
                     uint64_t expect,
                     uint64_t desired,
                     void *rdma_buf,
                     RemoteMemHandle &handle) override
    {
        CHECK(handle.valid());
        if constexpr (::config::kEnableRdmaTrace)
        {
            if (trace_enabled())
            {
                rdma_trace_record_.cas_nr++;
                rdma_trace_record_.indv_one_sided++;
            }
        }
        auto gaddr = vaddr_to_gaddr(vaddr);
        auto &lease = *(Lease *) handle.private_data();
        CHECK_GE(gaddr.offset, handle.gaddr().offset);
        auto offset = gaddr.offset - handle.gaddr().offset;
        auto flag = (flag_t) RWFlag::kNoLocalExpireCheck;
        auto rc = patronus_->prepare_cas(batch_,
                                         lease,
                                         (char *) rdma_buf,
                                         offset,
                                         expect,
                                         desired,
                                         flag,
                                         coro_ctx_);
        if (unlikely(rc == kNoMem))
        {
            CHECK_EQ(patronus_->commit(batch_, coro_ctx_), kOk);
            DCHECK(batch_.empty());
            return rdma_cas(vaddr, expect, desired, rdma_buf, handle);
        }
        return rc;
    }
    RetCode commit() override
    {
        if constexpr (::config::kEnableRdmaTrace)
        {
            if (trace_enabled())
            {
                rdma_trace_record_.commit_nr++;
            }
        }
        return patronus_->commit(batch_, coro_ctx_);
    }
    GlobalAddress to_exposed_gaddr(void *addr) override
    {
        return patronus_->to_exposed_gaddr(addr);
    }
    void *from_exposed_gaddr(GlobalAddress gaddr) override
    {
        return patronus_->from_exposed_gaddr(gaddr);
    }

    friend std::ostream &operator<<(std::ostream &os, const RdmaAdaptor &rdma);

    void enable_trace(const void *name) override
    {
        rdma_trace_record_.clear();
        rdma_trace_record_.init(std::string((const char *) name));

        enable_trace_ = true;
    }
    void end_trace(const void *) override
    {
        auto ns = rdma_trace_record_.timer.pin();
        rdma_trace_record_.latency_ns = ns;
        enable_trace_ = false;
    }
    bool trace_enabled() const override
    {
        return enable_trace_;
    }
    void trace_pin(const std::string_view name) override
    {
        if constexpr (::config::kEnableRdmaTrace)
        {
            if (unlikely(trace_enabled()))
            {
                rdma_trace_record_.trace_pin(name);
            }
        }
    }
    const RdmaTraceRecord &trace_record() const
    {
        return rdma_trace_record_;
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
    uint16_t node_id_;
    uint32_t dir_id_;
    Patronus::pointer patronus_;
    CoroContext *coro_ctx_{nullptr};
    std::vector<Buffer> ongoing_rdma_bufs_;
    bool is_server_;
    bool enable_trace_{false};
    std::string trace_name_;

    std::unordered_map<uint64_t, mem::IAllocator::pointer>
        overwrite_allocators_;

    PatronusBatchContext batch_;

    RdmaTraceRecord rdma_trace_record_;

    Debug<std::unordered_set<RemoteMemHandle>> ongoing_remote_handle_;

    /**
     * The global address returned to the caller (vaddr) is guaranteed to leave
     * the higher 16 bit unused (zeros). However, the internal global address
     * (gaddr) uses the higher 16 bits for the node_id. The conversion is
     * performed by the following two functions.
     */
    GlobalAddress gaddr_to_vaddr(GlobalAddress gaddr)
    {
        auto vaddr = gaddr;
        DCHECK_EQ(gaddr.nodeID, node_id_) << "** Invalid node_id";
        vaddr.nodeID = 0;
        return vaddr;
    }
    GlobalAddress vaddr_to_gaddr(GlobalAddress vaddr)
    {
        auto gaddr = vaddr;
        DCHECK_EQ(vaddr.nodeID, 0) << "** Invalid node_id";
        gaddr.nodeID = node_id_;
        return gaddr;
    }

    void free_handle(RemoteMemHandle &handle)
    {
        // free the lease
        auto lease_guard =
            std::unique_ptr<Lease>((Lease *) handle.private_data());
        if constexpr (debug())
        {
            auto ret = ongoing_remote_handle_.get().erase(handle);
            CHECK_EQ(ret, 1) << "** freeing an not-allocated-handled.";
        }

        handle.set_invalid();
    }
    RemoteMemHandle alloc_handle(GlobalAddress gaddr, size_t size, Lease &lease)
    {
        bool success = lease.success();
        DCHECK_EQ(gaddr.nodeID, 0) << "** gaddr.nodeID here should be zero, "
                                      "because it is client-visible";
        RemoteMemHandle handle(gaddr, size, lease.ec());
        auto stored_lease = std::make_unique<Lease>(std::move(lease));
        handle.set_private_data(stored_lease.release());
        if constexpr (debug())
        {
            CHECK(ongoing_remote_handle_.get().insert(handle).second);
        }
        CHECK(handle.valid());
        if (unlikely(!success))
        {
            handle.set_invalid();
        }
        return handle;
    }
};

inline std::ostream &operator<<(std::ostream &os, const RdmaAdaptor &rdma)
{
    os << "{Rdma " << rdma.ongoing_rdma_bufs_.size() << " buf}";
    return os;
}

}  // namespace patronus

class pre_rdma_adaptor
{
public:
    pre_rdma_adaptor(IRdmaAdaptor::pointer p) : p_(p)
    {
    }

    friend std::ostream &operator<<(std::ostream &os,
                                    const pre_rdma_adaptor &p);

private:
    IRdmaAdaptor::pointer p_;
};
inline std::ostream &operator<<(std::ostream &os, const pre_rdma_adaptor &p)
{
    auto *ptr = dynamic_cast<patronus::RdmaAdaptor *>(p.p_.get());
    if (ptr)
    {
        os << *ptr;
    }
    else
    {
        os << "{not an rdma adaptor}";
    }
    return os;
}

class pre_rdma_adaptor_trace
{
public:
    pre_rdma_adaptor_trace(IRdmaAdaptor::pointer p) : p_(p)
    {
    }

    friend std::ostream &operator<<(std::ostream &os,
                                    const pre_rdma_adaptor_trace &p);

private:
    IRdmaAdaptor::pointer p_;
};
inline std::ostream &operator<<(std::ostream &os,
                                const pre_rdma_adaptor_trace &p)
{
    auto *ptr = dynamic_cast<patronus::RdmaAdaptor *>(p.p_.get());
    if (ptr)
    {
        os << ptr->trace_record();
    }
    else
    {
        os << "{not an rdma adaptor}";
    }
    return os;
}

#endif