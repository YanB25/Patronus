#pragma once
#ifndef THIRD_PARTY_SERVERLESS_PARAMETERS_H_
#define THIRD_PARTY_SERVERLESS_PARAMETERS_H_

#include <iostream>
#include <map>
#include <string>
#include <string_view>
#include <unordered_map>

#include "./config.h"
#include "patronus/All.h"
#include "patronus/Lease.h"
#include "util/Tracer.h"

namespace serverless
{
struct ParameterContext
{
    GlobalAddress gaddr{nullgaddr};
    size_t size{};
    std::unordered_map<uint64_t, patronus::Lease> leases;
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
    os << ", leases: " << util::pre_umap(p.leases) << "}";
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
    using Lease = patronus::Lease;

public:
    using pointer = std::shared_ptr<Parameters>;
    using ParameterMap = std::map<std::string, Parameter>;
    Parameters(patronus::Patronus::pointer p,
               size_t server_nid,
               size_t dir_id,
               const Config &config,
               double trace_rate = 0)
        : p_(p),
          server_nid_(server_nid),
          dir_id_(dir_id),
          config_(config),
          tm_(trace_rate)
    {
        trace_ = tm_.trace("serverless");
    }
    static pointer new_instance(patronus::Patronus::pointer p,
                                size_t server_nid,
                                size_t dir_id,
                                const Config &config,
                                double trace_rate = 0)
    {
        return std::make_shared<Parameters>(
            p, server_nid, dir_id, config, trace_rate);
    }

    using void_ptr_t = void *;
    void_ptr_t &prv()
    {
        return prv_;
    }
    const void_ptr_t &prv() const
    {
        return prv_;
    }

    [[nodiscard]] std::pair<const Buffer &, size_t> get_param(
        const std::string &name,
        CoroContext *ctx,
        TraceView trace = util::nulltrace);

    Buffer get_buffer(size_t size)
    {
        auto ret = p_->get_rdma_buffer(size);
        DCHECK_NE(size, 0);
        DCHECK_GE(ret.size, 0) << "** Possible run out of buffer. got: " << ret;
        return ret;
    }
    void put_rdma_buffer(Buffer &&rdma_buf)
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
    [[nodiscard]] std::pair<Buffer, size_t> read(
        const std::string &,
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
                              const Buffer &rdma_buf,
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
                auto cid = DCHECK_NOTNULL(ctx)->coro_id();
                auto &lease = c.leases[cid];
                if (lease.success())
                {
                    // LOG(INFO) << "[debug] !! relinquishing " << lease;
                    do_relinquish_lease(lease, ctx, trace);
                    DCHECK(!lease.success());
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
            for (auto &[coro_id, lease] : p.leases)
            {
                std::ignore = coro_id;
                if (lease.success())
                {
                    DCHECK_NE(lease.ec(),
                              patronus::AcquireRequestStatus::kReserved);
                    do_relinquish_lease(lease, ctx, trace);
                    trace.pin("relinquish");
                    DCHECK(!lease.success());
                }
            }
        }
        contexts_.clear();
        prv_ = nullptr;
        trace.pin("done clear");

        LOG_IF(INFO, trace_.enabled()) << trace_;
        trace_ = tm_.trace("serverless");
    }
    TraceView trace() const
    {
        return trace_;
    }
    friend std::ostream &operator<<(std::ostream &os, const Parameters &p);

private:
    Lease &prepare_wlease(ParameterContext &c,
                          CoroContext *ctx,
                          TraceView trace = util::nulltrace)
    {
        DCHECK_NE(c.size, 0);
        auto &lease = get_lease(c, ctx);

        if (unlikely(!lease.success()))
        {
            lease = do_acquire_lease(c.gaddr, c.size, ctx, trace);
            DCHECK(lease.success());
        }
        return lease;
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
        auto &lease = get_lease(param, ctx);

        DCHECK(!lease.success());
        DCHECK_EQ(param.size, 0);
        DCHECK(!param.cached_data.has_value());

        lease = do_alloc_lease(actual_size, ctx);
        DCHECK(lease.success());
        param.gaddr = p_->get_gaddr(lease);
        param.size = actual_size;
        param.cached_data = std::move(rdma_buf);
        trace.pin("allocate param");

        return p_->write(lease,
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
        auto &lease = get_lease(param, ctx);

        if (param.cached_data.has_value())
        {
            p_->put_rdma_buffer(std::move(param.cached_data.value()));
        }
        DCHECK_GT(rdma_buf.size, 0);
        DCHECK_GT(actual_size, 0);
        DCHECK_NE(rdma_buf.buffer, nullptr);
        param.cached_data = std::move(rdma_buf);

        if (!lease.success())
        {
            lease = do_acquire_lease(param.gaddr, param.size, ctx);
            DCHECK(lease.success());
        }
        CHECK_GE(lease.buffer_size(), actual_size);
        auto rc = p_->write(lease,
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
    /**
     * if use_step, we shared lease across lambdas
     * if not use_step, lambdas use their own leases.
     * This function distinguish these behaviour
     */
    Lease &get_lease(ParameterContext &param, CoroContext *ctx)
    {
        if (config_.use_step_lambda)
        {
            return param.leases[0];
        }
        else
        {
            auto cid = DCHECK_NOTNULL(ctx)->coro_id();
            return param.leases[cid];
        }
    }
    [[nodiscard]] std::pair<const Buffer &, size_t> do_get_data(
        ParameterContext &param,
        GlobalAddress gaddr,
        size_t size,
        CoroContext *ctx,
        TraceView trace = util::nulltrace)
    {
        auto &lease = get_lease(param, ctx);

        if (likely(param.cached_data.has_value()))
        {
            DCHECK_GE(param.cached_data.value().size, param.size);
            return {param.cached_data.value(), param.size};
        }
        DCHECK_GT(size, 0);
        if (unlikely(param.size == 0))
        {
            DCHECK_EQ(param.gaddr, nullgaddr);
            DCHECK(!param.cached_data.has_value());
            DCHECK(!lease.success());
            param.size = size;
            param.gaddr = gaddr;
        }
        if (unlikely(lease.success() && lease.buffer_size() < size))
        {
            do_relinquish_lease(lease, ctx, trace);
            DCHECK(!lease.success());
        }
        if (unlikely(!lease.success()))
        {
            lease = do_acquire_lease(gaddr, size, ctx, trace);
            DCHECK(lease.success());
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
        p_->read(lease,
                 param.cached_data.value().buffer,
                 size,
                 0 /* 0ffset */,
                 0 /* rw flag */,
                 ctx)
            .expect(RC::kOk);
        trace.pin("read");
        DCHECK_GE(param.cached_data.value().size, param.size);
        return {param.cached_data.value(), param.size};
    }

    [[nodiscard]] std::pair<const Buffer &, size_t> do_get_param(
        ParameterContext &param, CoroContext *ctx, TraceView trace)
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
        // LOG(INFO) << "[debug] !!! get_wlease get: " << ret
        //           << " for gaddr: " << gaddr << ", size: " << size
        //           << ", ctx: " << pre_coro_ctx(ctx);
        DCHECK(ret.success());
        acquire_nr_++;
        trace.pin("acquire");
        return ret;
    }
    void do_relinquish_lease(Lease &lease,
                             CoroContext *ctx,
                             TraceView trace = util::nulltrace)
    {
        const auto &c = config_.param;
        p_->relinquish(lease, c.alloc_hint, c.relinquish_flag, ctx);
        relinquish_nr_++;
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
        acquire_nr_++;
        return ret;
    }

    using Context = std::map<std::string, ParameterContext>;
    patronus::Patronus::pointer p_{};
    size_t server_nid_{};
    size_t dir_id_{};
    Config config_;
    Context contexts_;
    util::TraceManager tm_;
    util::TraceView trace_{util::nulltrace};

    void *prv_{nullptr};

    ssize_t acquire_nr_{0};
    ssize_t relinquish_nr_{0};
};

std::pair<const Buffer &, size_t> Parameters::get_param(const std::string &name,
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
    auto &lease = get_lease(c, ctx);
    if (unlikely(c.size == 0))
    {
        // consistent checks
        DCHECK_EQ(c.gaddr, nullgaddr);
        DCHECK(!lease.success());
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
        if (lease.success() && lease.buffer_size() < actual_size)
        {
            do_relinquish_lease(lease, ctx, trace);
            DCHECK(!lease.success());
        }
        if (!lease.success())
        {
            lease = do_alloc_lease(actual_size, ctx, trace);
            DCHECK(lease.success());
            c.gaddr = p_->get_gaddr(lease);
            c.size = actual_size;
        }
        DCHECK(c.cached_data.has_value());
        p_->write(lease,
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
    os << "{Parameters " << util::pre_map(p.contexts_) << ", prv: " << p.prv()
       << "}";
    return os;
}

std::pair<Buffer, size_t> Parameters::read(const std::string &name,
                                           CoroContext *ctx,
                                           TraceView trace)
{
    auto [it, inserted] = contexts_.try_emplace(name);
    CHECK(!inserted) << "** Failed to locate parameter with name " << name;
    auto &c = it->second;
    auto &lease = prepare_wlease(c, ctx, trace);
    auto rdma_buf = p_->get_rdma_buffer(c.size);
    DCHECK(lease.success());
    DCHECK_NE(rdma_buf.buffer, nullptr);
    DCHECK_GE(rdma_buf.size, c.size);
    p_->read(lease,
             rdma_buf.buffer,
             c.size,
             0 /* offset */,
             0 /* flag */,
             ctx,
             trace)
        .expect(RC::kOk);

    return {std::move(rdma_buf), c.size};
}

void Parameters::alloc(const std::string &name,
                       size_t size,
                       CoroContext *ctx,
                       TraceView trace)
{
    auto [it, inserted] = contexts_.try_emplace(name);
    CHECK(inserted) << "** Duplicated parameter detected for " << name;
    auto &c = it->second;
    auto &lease = get_lease(c, ctx);
    DCHECK(!lease.success());
    lease = do_alloc_lease(size, ctx, trace);
    c.size = size;
    c.gaddr = p_->get_gaddr(lease);
    DCHECK(!c.cached_data.has_value());
}

RetCode Parameters::do_write(ParameterContext &c,
                             Buffer &&rdma_buf,
                             CoroContext *ctx,
                             TraceView trace)
{
    auto &lease = prepare_wlease(c, ctx, trace);

    // dont pollute cache
    DCHECK_GE(rdma_buf.size, c.size);
    auto rc = p_->write(lease,
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
    auto &lease = prepare_wlease(c, ctx, trace);

    DCHECK_GE(rdma_buf.size, sizeof(uint64_t));
    auto rc = p_->cas(lease,
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
                        const Buffer &rdma_buf,
                        CoroContext *ctx,
                        TraceView trace)
{
    auto [it, inserted] = contexts_.try_emplace(name);
    CHECK(!inserted) << "** Failed to locate param with name " << name;
    auto &c = it->second;
    auto &lease = prepare_wlease(c, ctx, trace);

    auto rc = p_->faa(lease,
                      rdma_buf.buffer,
                      0 /* offset */,
                      value,
                      0 /* flag */,
                      ctx,
                      trace);
    return rc;
}

}  // namespace serverless

#endif