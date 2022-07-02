#pragma once
#ifndef THIRD_PARTY_SERVERLESS_PARAMETERS_H_
#define THIRD_PARTY_SERVERLESS_PARAMETERS_H_

#include <iostream>
#include <map>
#include <string>
#include <string_view>

#include "./config.h"
#include "patronus/All.h"
#include "util/Tracer.h"

namespace serverless
{
struct ParameterContext
{
    GlobalAddress gaddr{nullgaddr};
    size_t size{};
    patronus::Lease lease{};
    std::optional<Buffer> cached_data{};
    ParameterContext() = default;
    ParameterContext(GlobalAddress gaddr, size_t size)
        : gaddr(gaddr), size(size)
    {
    }
};
inline std::ostream &operator<<(std::ostream &os, const ParameterContext &p)
{
    os << "{gaddr: " << p.gaddr << ", size: " << p.size << ", ";
    if (p.cached_data.has_value())
    {
        os << "cached_data: " << p.cached_data.value() << ", ";
    }
    os << "lease.valid: " << p.lease.success() << "}";
    return os;
}

struct Parameter
{
    GlobalAddress gaddr;
    size_t size;
};

inline std::ostream &operator<<(std::ostream &os, const Parameter &p)
{
    os << "{gaddr: " << p.gaddr << ", size: " << p.size << "}";
    return os;
}

/**
 * There are two sets of API, namely parameter-wise IO and data-wise IO.
 * parameter-wise IO is used for passing parameters between lambdas, while
 * data-wise IO is used for querying/modifying data in lambdas.
 *
 * If use_step_lambda is ON, we try our best to share resources among dependent
 * lambdas. I.e., parameters are passed by local shared memory. Leases are
 * reused best-efford. On the other hand (flag is OFF), passing parameters uses
 * RM, and leases are not shared.
 *
 * Parameter-wise IO:
 * - get_param
 * - set_param
 * data-wise IO:
 * - read
 * - write
 * - cas
 * - faa
 */
class Parameters
{
    using AcquireRequestFlag = patronus::AcquireRequestFlag;
    using LeaseModifyFlag = patronus::LeaseModifyFlag;
    using TraceView = util::TraceView;
    using ParameterMap = std::map<std::string, Parameter>;
    using Lease = patronus::Lease;

public:
    using pointer = std::shared_ptr<Parameters>;
    Parameters(patronus::Patronus::pointer p,
               size_t server_nid,
               size_t dir_id,
               const Config &config)
        : p_(p), server_nid_(server_nid), dir_id_(dir_id), config_(config)
    {
    }
    static pointer new_instance(patronus::Patronus::pointer p,
                                size_t server_nid,
                                size_t dir_id,
                                const Config &config)
    {
        return std::make_shared<Parameters>(p, server_nid, dir_id, config);
    }

    void *&prv()
    {
        return prv_;
    }

    [[nodiscard]] const Buffer &get_param(const std::string &name,
                                          CoroContext *ctx,
                                          TraceView trace = util::nulltrace);
    Buffer get_buffer(size_t size)
    {
        return p_->get_rdma_buffer(size);
    }
    void put_rdma_buffer(Buffer &rdma_buf)
    {
        p_->put_rdma_buffer(std::move(rdma_buf));
    }

    /**
     * @return true insert
     * @return false overwrite
     */
    bool install_param(const std::string &name,
                       GlobalAddress gaddr,
                       size_t size)
    {
        return contexts_.insert_or_assign(name, ParameterContext(gaddr, size))
            .second;
    }

    void install_params(const ParameterMap &map)
    {
        for (const auto &[name, param] : map)
        {
            CHECK(install_param(name, param.gaddr, param.size))
                << "** Installing " << name
                << " got duplicated. detail: " << util::pre_map(map);
        }
    }

    void set_param(const std::string &name,
                   Buffer &&buf,
                   size_t param_size,
                   CoroContext *ctx,
                   TraceView trace = util::nulltrace);
    [[nodiscard]] Buffer read(const std::string &,
                              CoroContext *ctx,
                              TraceView trace = util::nulltrace);
    void alloc(const std::string &,
               size_t size,
               CoroContext *ctx,
               TraceView trace = util::nulltrace);
    [[nodiscard]] RetCode write(const std::string &name,
                                Buffer &&rdma_buf,
                                CoroContext *ctx,
                                TraceView trace = util::nulltrace)
    {
        auto [it, inserted] = contexts_.try_emplace(name);
        CHECK(!inserted) << "** Failed to locate parameter with name " << name;
        return do_write(it->second, std::move(rdma_buf), ctx, trace);
    }
    [[nodiscard]] RetCode cas(const std::string &,
                              uint64_t compare,
                              uint64_t swap,
                              Buffer &&rdma_buf,
                              CoroContext *ctx,
                              TraceView trace = util::nulltrace);
    [[nodiscard]] RetCode faa(const std::string &,
                              int64_t value,
                              Buffer &&rdma_buf,
                              CoroContext *ctx,
                              TraceView trace = util::nulltrace);

    void next_lambda(CoroContext *ctx, TraceView trace = util::nulltrace)
    {
        if (!config_.use_step_lambda)
        {
            // if not enable step lambdas
            // resources are not shared among them
            for (auto &[name, c] : contexts_)
            {
                std::ignore = name;
                if (c.cached_data.has_value())
                {
                    p_->put_rdma_message_buffer(
                        std::move(c.cached_data.value()));
                    c.cached_data = std::nullopt;
                }
                if (c.lease.success())
                {
                    do_relinquish_lease(c.lease, ctx, trace);
                    DCHECK(!c.lease.success());
                }
            }
        }
    }
    void clear(CoroContext *ctx, TraceView trace = util::nulltrace)
    {
        for (auto &[name, p] : contexts_)
        {
            std::ignore = name;
            if (p.cached_data.has_value())
            {
                DCHECK_NE(p.cached_data.value().size, 0);
                DCHECK_NE(p.cached_data.value().buffer, nullptr);
                p_->put_rdma_buffer(std::move(p.cached_data.value()));
                p.cached_data = std::nullopt;
            }
            if (p.lease.success())
            {
                DCHECK_NE(p.lease.ec(),
                          patronus::AcquireRequestStatus::kReserved);
                do_relinquish_lease(p.lease, ctx, trace);
                DCHECK(!p.lease.success());
            }
        }
        contexts_.clear();
    }

    friend std::ostream &operator<<(std::ostream &os, const Parameters &p);

private:
    void prepare_wlease(ParameterContext &c,
                        CoroContext *ctx,
                        TraceView trace = util::nulltrace)
    {
        DCHECK_NE(c.size, 0);

        if (unlikely(!c.lease.success()))
        {
            c.lease = do_acquire_lease(c.gaddr, c.size, ctx, trace);
            DCHECK(c.lease.success());
        }
    }

    void do_set_param(ParameterContext &,
                      Buffer &&,
                      size_t param_size,
                      CoroContext *ctx,
                      TraceView = util::nulltrace);
    [[nodiscard]] RetCode do_write(ParameterContext &,
                                   Buffer &&rdma_buf,
                                   CoroContext *ctx,
                                   TraceView = util::nulltrace);
    [[nodiscard]] RetCode do_allocate_param(ParameterContext &param,
                                            Buffer &&rdma_buf,
                                            size_t actual_size,
                                            CoroContext *ctx,
                                            TraceView trace = util::nulltrace)
    {
        DCHECK(!param.lease.success());
        DCHECK_EQ(param.size, 0);
        DCHECK(!param.cached_data.has_value());

        param.lease = do_alloc_lease(actual_size, ctx);
        DCHECK(param.lease.success());
        param.gaddr = p_->get_gaddr(param.lease);
        param.size = actual_size;
        param.cached_data = std::move(rdma_buf);
        trace.pin("allocate param");

        return p_->write(param.lease,
                         param.cached_data.value().buffer,
                         actual_size,
                         0 /* offset */,
                         0 /* flag */,
                         ctx,
                         trace);
    }

    [[nodiscard]] RetCode do_overwrite_param(ParameterContext &param,
                                             Buffer &&rdma_buf,
                                             size_t actual_size,
                                             CoroContext *ctx,
                                             TraceView trace)
    {
        if (param.cached_data.has_value())
        {
            p_->put_rdma_buffer(std::move(param.cached_data.value()));
        }
        DCHECK_GT(rdma_buf.size, 0);
        DCHECK_GT(actual_size, 0);
        DCHECK_NE(rdma_buf.buffer, nullptr);
        param.cached_data = std::move(rdma_buf);

        if (!param.lease.success())
        {
            param.lease = do_acquire_lease(param.gaddr, param.size, ctx);
            DCHECK(param.lease.success());
        }
        CHECK_GE(param.lease.buffer_size(), actual_size);
        auto rc = p_->write(param.lease,
                            param.cached_data.value().buffer,
                            actual_size,
                            0 /* offset */,
                            0 /* flag */,
                            ctx);
        trace.pin("write");
        return rc;
    }
    [[nodiscard]] RetCode do_write(ParameterContext &param,
                                   Buffer &&rdma_buf,
                                   size_t actual_size,
                                   TraceView trace = util::nulltrace);

    [[nodiscard]] const Buffer &do_get_data(ParameterContext &param,
                                            GlobalAddress gaddr,
                                            size_t size,
                                            CoroContext *ctx,
                                            TraceView trace = util::nulltrace)
    {
        if (likely(param.cached_data.has_value()))
        {
            return param.cached_data.value();
        }
        DCHECK_GT(size, 0);
        if (unlikely(param.size == 0))
        {
            DCHECK_EQ(param.gaddr, nullgaddr);
            DCHECK(!param.cached_data.has_value());
            DCHECK(!param.lease.success());
            param.size = size;
            param.gaddr = gaddr;
        }
        if (unlikely(param.lease.success() && param.lease.buffer_size() < size))
        {
            do_relinquish_lease(param.lease, ctx, trace);
            DCHECK(!param.lease.success());
        }
        if (unlikely(!param.lease.success()))
        {
            param.lease = do_acquire_lease(gaddr, size, ctx, trace);
            DCHECK(param.lease.success());
            param.size = size;
            param.gaddr = gaddr;
        }
        DCHECK(!param.cached_data.has_value());
        param.cached_data = p_->get_rdma_buffer(size);
        DCHECK_GE(param.cached_data.value().size, size);
        DCHECK_NE(param.cached_data.value().buffer, nullptr);
        DCHECK_EQ(param.gaddr, gaddr)
            << "** Mismatch detected for param: " << param;
        DCHECK_GE(param.size, size);
        p_->read(param.lease,
                 param.cached_data.value().buffer,
                 size,
                 0 /* 0ffset */,
                 0 /* rw flag */,
                 ctx)
            .expect(RC::kOk);
        trace.pin("read");
        return param.cached_data.value();
    }

    [[nodiscard]] const Buffer &do_get_param(ParameterContext &param,
                                             CoroContext *ctx,
                                             TraceView trace)
    {
        return do_get_data(param, param.gaddr, param.size, ctx, trace);
    }

    [[nodiscard]] Lease do_acquire_lease(GlobalAddress gaddr,
                                         size_t size,
                                         CoroContext *ctx,
                                         TraceView trace = util::nulltrace)
    {
        const auto &c = config_.param;
        auto ret = p_->get_wlease(server_nid_,
                                  dir_id_,
                                  gaddr,
                                  c.alloc_hint,
                                  size,
                                  c.ns,
                                  c.acquire_flag,
                                  ctx);
        trace.pin("acquire");
        return ret;
    }
    void do_relinquish_lease(Lease &lease,
                             CoroContext *ctx,
                             TraceView trace = util::nulltrace)
    {
        const auto &c = config_.param;
        p_->relinquish(lease, c.alloc_hint, c.relinquish_flag, ctx);
        trace.pin("relinquish");
    }
    [[nodiscard]] Lease do_alloc_lease(size_t size,
                                       CoroContext *ctx,
                                       TraceView trace = util::nulltrace)
    {
        const auto &c = config_.alloc;
        auto ret = p_->get_wlease(server_nid_,
                                  dir_id_,
                                  nullgaddr,
                                  c.alloc_hint,
                                  size,
                                  c.ns,
                                  c.acquire_flag,
                                  ctx);
        trace.pin("alloc");
        return ret;
    }

    patronus::Patronus::pointer p_{};
    size_t server_nid_{};
    size_t dir_id_{};
    Config config_;
    std::map<std::string, ParameterContext> contexts_;

    void *prv_{nullptr};
};

const Buffer &Parameters::get_param(const std::string &name,
                                    CoroContext *ctx,
                                    TraceView trace)
{
    auto &c = contexts_[name];
    return do_get_param(c, ctx, trace);
}

void Parameters::set_param(const std::string &name,
                           Buffer &&rdma_buf,
                           size_t actual_size,
                           CoroContext *ctx,
                           TraceView trace)
{
    auto &c = contexts_[name];
    return do_set_param(c, std::move(rdma_buf), actual_size, ctx, trace);
}

void Parameters::do_set_param(ParameterContext &c,
                              Buffer &&rdma_buf,
                              size_t actual_size,
                              CoroContext *ctx,
                              TraceView trace)
{
    CHECK_GT(actual_size, 0);
    if (unlikely(c.size == 0))
    {
        // consistent checks
        DCHECK_EQ(c.gaddr, nullgaddr);
        DCHECK(!c.lease.success());
        DCHECK(!c.cached_data.has_value());
    }
    if (c.cached_data.has_value())
    {
        p_->put_rdma_buffer(std::move(c.cached_data.value()));
        c.cached_data = std::nullopt;
    }
    c.cached_data = std::move(rdma_buf);
    // if using step_lambda, we optimize how parameters are passing
    // otherwise, passing parameters require RM
    if (!config_.use_step_lambda)
    {
        if (c.lease.success() && c.lease.buffer_size() < actual_size)
        {
            do_relinquish_lease(c.lease, ctx, trace);
            DCHECK(!c.lease.success());
        }
        if (!c.lease.success())
        {
            c.lease = do_alloc_lease(actual_size, ctx, trace);
            DCHECK(c.lease.success());
            c.gaddr = p_->get_gaddr(c.lease);
            c.size = actual_size;
        }
        DCHECK(c.cached_data.has_value());
        p_->write(c.lease,
                  c.cached_data.value().buffer,
                  actual_size,
                  0 /* offset */,
                  0 /* flag */,
                  ctx,
                  trace)
            .expect(RC::kOk);
    }
    trace.pin("do set param");
}

inline std::ostream &operator<<(std::ostream &os, const Parameters &p)
{
    // os << "{Parameters " << util::pre_map(p.contexts_) << "}";
    std::ignore = p;
    os << "{Parameters TODO: }";
    return os;
}

Buffer Parameters::read(const std::string &name,
                        CoroContext *ctx,
                        TraceView trace)
{
    auto [it, inserted] = contexts_.try_emplace(name);
    CHECK(!inserted) << "** Failed to locate parameter with name " << name;
    auto &c = it->second;
    prepare_wlease(c, ctx, trace);
    auto rdma_buf = p_->get_rdma_buffer(c.size);
    DCHECK(c.lease.success());
    p_->read(c.lease,
             rdma_buf.buffer,
             c.size,
             0 /* offset */,
             0 /* flag */,
             ctx,
             trace)
        .expect(RC::kOk);
    return rdma_buf;
}

void Parameters::alloc(const std::string &name,
                       size_t size,
                       CoroContext *ctx,
                       TraceView trace)
{
    auto [it, inserted] = contexts_.try_emplace(name);
    CHECK(inserted) << "** Duplicated parameter detected for " << name;
    auto &c = it->second;
    c.lease = do_alloc_lease(size, ctx, trace);
    c.size = size;
    c.gaddr = p_->get_gaddr(c.lease);
    DCHECK(!c.cached_data.has_value());
}

RetCode Parameters::do_write(ParameterContext &c,
                             Buffer &&rdma_buf,
                             CoroContext *ctx,
                             TraceView trace)
{
    prepare_wlease(c, ctx, trace);

    // dont pollute cache
    DCHECK_GE(rdma_buf.size, c.size);
    auto rc = p_->write(c.lease,
                        rdma_buf.buffer,
                        c.size,
                        0 /* offset */,
                        0 /* flag */,
                        ctx,
                        trace);
    p_->put_rdma_buffer(std::move(rdma_buf));
    return rc;
}

RetCode Parameters::cas(const std::string &name,
                        uint64_t compare,
                        uint64_t swap,
                        Buffer &&rdma_buf,
                        CoroContext *ctx,
                        TraceView trace)
{
    auto [it, inserted] = contexts_.try_emplace(name);
    CHECK(!inserted) << "** Failed to locate param with name " << name;
    auto &c = it->second;
    prepare_wlease(c, ctx, trace);

    DCHECK_GE(rdma_buf.size, sizeof(uint64_t));
    auto rc = p_->cas(c.lease,
                      rdma_buf.buffer,
                      0 /* offset */,
                      compare,
                      swap,
                      0 /* flag */,
                      ctx,
                      trace);
    p_->put_rdma_buffer(std::move(rdma_buf));
    return rc;
}

RetCode Parameters::faa(const std::string &name,
                        int64_t value,
                        Buffer &&rdma_buf,
                        CoroContext *ctx,
                        TraceView trace)
{
    auto [it, inserted] = contexts_.try_emplace(name);
    CHECK(!inserted) << "** Failed to locate param with name " << name;
    auto &c = it->second;
    prepare_wlease(c, ctx, trace);

    auto rc = p_->faa(c.lease,
                      rdma_buf.buffer,
                      0 /* offset */,
                      value,
                      0 /* flag */,
                      ctx,
                      trace);
    p_->put_rdma_buffer(std::move(rdma_buf));
    return rc;
}

}  // namespace serverless

#endif