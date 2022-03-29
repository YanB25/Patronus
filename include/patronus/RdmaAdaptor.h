#pragma once
#ifndef PATRONUS_RDMA_ADAPTOR_H_
#define PATRONUS_RDMA_ADAPTOR_H_

#include <array>

#include "patronus/Patronus.h"
#include "util/IRdmaAdaptor.h"

namespace patronus
{
class RdmaAdaptor : public IRdmaAdaptor
{
public:
    constexpr static size_t kMaxOngoingRdmaBuf = 16;
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
          dsm_(patronus->get_dsm()),
          coro_ctx_(ctx),
          is_server_(is_server)
    {
        ongoing_rdma_bufs_.reserve(kMaxOngoingRdmaBuf);
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

    virtual ~RdmaAdaptor() = default;
    // yes
    /**
     * The manipulation of address:
     * Lease& lease = *(Lease *) handle.private_data();
     * The dsm offset: lease.base_address()
     * The buffer offset: handle.gaddr()
     * What RdmaAdaptor exposes: buffer offset
     * What Patronus accepts: buffer offset
     */
    RemoteMemHandle remote_alloc_acquire_perm(size_t size, hint_t hint) override
    {
        DCHECK(!is_server_);
        auto flag = (uint8_t) AcquireRequestFlag::kNoGc |
                    (uint8_t) AcquireRequestFlag::kWithAllocation;

        auto lease = patronus_->get_wlease(
            GlobalAddress(node_id_, hint), dir_id_, size, 0ns, flag, coro_ctx_);
        return alloc_handle(lease_to_exposed_gaddr(lease), size, lease);
    }
    GlobalAddress lease_to_exposed_gaddr(const Lease &lease) const
    {
        auto buffer_offset =
            dsm_->dsm_offset_to_buffer_offset(lease.base_addr());
        return GlobalAddress(0, buffer_offset);
    }
    // yes
    RemoteMemHandle acquire_perm(GlobalAddress vaddr, size_t size) override
    {
        auto gaddr = vaddr_to_gaddr(vaddr);
        auto flag = (uint8_t) AcquireRequestFlag::kNoGc;
        DCHECK_EQ(gaddr.nodeID, node_id_);
        auto lease =
            patronus_->get_wlease(gaddr, dir_id_, size, 0ns, flag, coro_ctx_);
        CHECK(lease.success()) << "lease: " << lease << ", ec: " << lease.ec();

        DLOG_IF(INFO, config::kMonitorAddressConversion)
            << "[addr] acquire_perm: gaddr: " << gaddr
            << " (from vaddr: " << vaddr << "), got lease.base_addr() "
            << (void *) lease.base_addr() << ")";

        return alloc_handle(lease_to_exposed_gaddr(lease), size, lease);
    }
    // TODO:
    void remote_free(GlobalAddress vaddr, size_t size, hint_t hint) override
    {
        auto gaddr = vaddr_to_gaddr(vaddr);
        patronus_->dealloc(gaddr, dir_id_, size, hint, coro_ctx_);
    }
    void remote_free_relinquish_perm(RemoteMemHandle &handle,
                                     hint_t hint) override
    {
        auto &lease = *(Lease *) handle.private_data();
        auto flag = (uint8_t) LeaseModifyFlag::kWithDeallocation;
        patronus_->relinquish(lease, hint, flag, coro_ctx_);
        free_handle(handle);
    }
    void relinquish_perm(RemoteMemHandle &handle) override
    {
        auto &lease = *(Lease *) handle.private_data();
        auto flag = 0;
        patronus_->relinquish(lease, 0 /* hint */, flag, coro_ctx_);
        free_handle(handle);
    }
    // yes
    Buffer get_rdma_buffer(size_t size) override
    {
        auto ret = patronus_->get_rdma_buffer(size);
        if (ret.buffer == nullptr)
        {
            return Buffer(nullptr, 0);
        }
        DCHECK_GE(ret.size, size);
        ongoing_rdma_bufs_.push_back(ret);
        CHECK_LT(ongoing_rdma_bufs_.size(), kMaxOngoingRdmaBuf);

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
        auto gaddr = vaddr_to_gaddr(vaddr);
        auto &lease = *(Lease *) handle.private_data();
        CHECK_GE(gaddr.offset, handle.gaddr().offset);
        auto offset = gaddr.offset - handle.gaddr().offset;
        auto flag = (uint8_t) RWFlag::kNoLocalExpireCheck;
        auto ec = patronus_->read(
            lease, (char *) rdma_buf, size, offset, flag, coro_ctx_);
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
        auto gaddr = vaddr_to_gaddr(vaddr);
        auto &lease = *(Lease *) handle.private_data();
        CHECK_GE(gaddr.offset, handle.gaddr().offset);
        auto offset = gaddr.offset - handle.gaddr().offset;
        auto flag = (uint8_t) RWFlag::kNoLocalExpireCheck;
        auto ec = patronus_->write(
            lease, (char *) rdma_buf, size, offset, flag, coro_ctx_);
        return ec;
    }
    RetCode rdma_cas(GlobalAddress vaddr,
                     uint64_t expect,
                     uint64_t desired,
                     void *rdma_buf,
                     RemoteMemHandle &handle) override
    {
        auto gaddr = vaddr_to_gaddr(vaddr);
        auto &lease = *(Lease *) handle.private_data();
        CHECK_GE(gaddr.offset, handle.gaddr().offset);
        auto offset = gaddr.offset - handle.gaddr().offset;
        auto flag = (uint8_t) RWFlag::kNoLocalExpireCheck;
        return patronus_->cas(
            lease, (char *) rdma_buf, offset, expect, desired, flag, coro_ctx_);
    }
    RetCode commit() override
    {
        LOG_FIRST_N(WARNING, 1)
            << "[rdma-adaptor] commit() not use. not batching r/w here. "
            << pre_coro_ctx(coro_ctx_);
        return kOk;
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

private:
    uint16_t node_id_;
    uint32_t dir_id_;
    Patronus::pointer patronus_;
    DSM::pointer dsm_;
    CoroContext *coro_ctx_{nullptr};
    std::vector<Buffer> ongoing_rdma_bufs_;
    bool is_server_;

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
        handle.set_invalid();
    }
    RemoteMemHandle alloc_handle(GlobalAddress gaddr, size_t size, Lease &lease)
    {
        DCHECK_EQ(gaddr.nodeID, 0) << "** gaddr.nodeID here should be zero, "
                                      "because it is client-visible";
        RemoteMemHandle handle(gaddr, size);
        auto stored_lease = std::make_unique<Lease>(std::move(lease));
        handle.set_private_data(stored_lease.release());
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
    patronus::RdmaAdaptor *ptr = (patronus::RdmaAdaptor *) p.p_.get();
    os << *ptr;
    return os;
}

#endif