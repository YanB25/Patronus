#pragma once
#ifndef PATRONUS_RDMA_ADAPTOR_H_
#define PATRONUS_RDMA_ADAPTOR_H_

#include "patronus/Patronus.h"
#include "util/IRdmaAdaptor.h"

namespace patronus
{
class RdmaAdaptor : public IRdmaAdaptor
{
public:
    /**
     * @brief Construct a new Rdma Adaptor object
     *
     * @param node_id the node_id for client. Not used for server
     * @param patronus
     */
    RdmaAdaptor(uint16_t node_id, Patronus::pointer patronus)
        : node_id_(node_id), patronus_(patronus)
    {
    }
    static pointer new_instance(uint16_t node_id, Patronus::pointer patronus)
    {
        return std::make_shared<RdmaAdaptor>(node_id, patronus);
    }

    virtual ~RdmaAdaptor() = default;
    // yes
    RemoteMemHandle remote_alloc_acquire_perm(
        size_t size, hint_t hint, CoroContext *ctx = nullptr) override
    {
        CHECK(false) << "TODO: size " << size << ", ctx: " << *ctx << hint;
        auto ret = nullgaddr;
        auto vaddr = gaddr_to_vaddr(ret);
        return RemoteMemHandle(vaddr, 0);
    }
    // yes
    RemoteMemHandle acquire_perm(GlobalAddress vaddr,
                                 size_t size,
                                 CoroContext *ctx = nullptr) override
    {
        // TODO(patronus): let patronus work at identity_key_to_addressing
        // mapping mode.
        auto gaddr = vaddr_to_gaddr(vaddr);
        CHECK(false) << "TODO: gaddr " << gaddr << size << *ctx;
        RemoteMemHandle(nullgaddr, 0);
    }
    // TODO:
    void remote_free(GlobalAddress vaddr,
                     hint_t hint,
                     CoroContext *ctx = nullptr) override
    {
        auto gaddr = vaddr_to_gaddr(vaddr);
        CHECK(false) << "TODO: gaddr " << gaddr << ", ctx: " << *ctx << hint;
    }
    void remote_free_relinquish_perm(RemoteMemHandle &handle,
                                     hint_t hint,
                                     CoroContext *ctx = nullptr) override
    {
        CHECK(false) << "TODO: handle: " << handle << ", ctx: " << *ctx << hint;
    }
    void relinquish_perm(RemoteMemHandle &handle,
                         CoroContext *ctx = nullptr) override
    {
        CHECK(false) << "TODO:" << handle << *ctx;
    }
    // yes
    char *get_rdma_buffer(size_t size) override
    {
        auto ret = patronus_->get_rdma_buffer();
        CHECK_GE(ret.size, size);
        return ret.buffer;
    }
    // yes
    void put_rdma_buffer(void *rdma_buf) override
    {
        patronus_->put_rdma_buffer(rdma_buf);
    }
    RetCode rdma_read(void *rdma_buf,
                      GlobalAddress vaddr,
                      size_t size,
                      RemoteMemHandle &handle,
                      CoroContext *ctx = nullptr) override
    {
        auto gaddr = vaddr_to_gaddr(vaddr);
        CHECK(false) << rdma_buf << gaddr << size << handle << *ctx;
    }
    RetCode rdma_write(GlobalAddress vaddr,
                       void *rdma_buf,
                       size_t size,
                       RemoteMemHandle &handle,
                       CoroContext *ctx = nullptr) override
    {
        auto gaddr = vaddr_to_gaddr(vaddr);
        CHECK(false) << gaddr << rdma_buf << size << handle << *ctx;
    }
    RetCode rdma_cas(GlobalAddress vaddr,
                     uint64_t expect,
                     uint64_t desired,
                     void *rdma_buf,
                     RemoteMemHandle &,
                     CoroContext *ctx = nullptr) override
    {
        auto gaddr = vaddr_to_gaddr(vaddr);
        CHECK(false) << gaddr << expect << desired << rdma_buf << *ctx;
    }
    RetCode commit(CoroContext *ctx = nullptr) override
    {
        CHECK(false)
            << "TODO: patronus support batching R/W/Cas for each coroutine"
            << pre_coro_ctx(ctx);
    }
    RetCode put_all_rdma_buffer() override
    {
        CHECK(false) << "TODO: Add an unordered_set here";
    }
    GlobalAddress to_exposed_gaddr(void *addr) override
    {
        CHECK(false) << addr;
    }
    void *from_exposed_gaddr(GlobalAddress gaddr) override
    {
        CHECK(false) << gaddr;
    }

private:
    uint16_t node_id_;
    Patronus::pointer patronus_;

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
};
}  // namespace patronus

#endif