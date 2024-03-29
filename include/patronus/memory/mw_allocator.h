
#pragma once
#ifndef MEMORY_MW_ALLOCATOR_H_
#define MEMORY_MW_ALLOCATOR_H_

#include <queue>
#include <stack>

#include "DSM.h"
#include "Rdma.h"
#include "allocator.h"
#include "slab_allocator.h"

namespace patronus::mem
{
class MWPool
{
public:
    MWPool(DSM::pointer dsm,
           size_t dir_id,
           size_t allocate_nr,
           bool enable_locality)
        : dsm_(dsm),
          dir_id_(dir_id),
          allocated_(2 * allocate_nr),
          enable_locality_(enable_locality)
    {
        DVLOG(::config::verbose::kSystem) << "[mw_pool] alloc " << allocate_nr;
        for (size_t i = 0; i < allocate_nr; ++i)
        {
            mw_pool_.push(CHECK_NOTNULL(dsm_->alloc_mw(dir_id_)));
            mw_pool_loc_.push(CHECK_NOTNULL(dsm_->alloc_mw(dir_id_)));
        }
    }
    bool enabled_locality() const
    {
        return enable_locality_;
    }
    void set_enable_locality(bool enable)
    {
        enable_locality_ = enable;
    }
    ibv_mw *alloc()
    {
        if (likely(enable_locality_))
        {
            if (unlikely(mw_pool_loc_.empty()))
            {
                return nullptr;
            }
            auto *ret = mw_pool_loc_.top();
            mw_pool_loc_.pop();
            return DCHECK_NOTNULL(ret);
        }
        else
        {
            if (unlikely(mw_pool_.empty()))
            {
                return nullptr;
            }
            auto *ret = mw_pool_.front();
            mw_pool_.pop();
            return DCHECK_NOTNULL(ret);
        }
    }
    void free(ibv_mw *mw)
    {
        if (unlikely(mw == nullptr))
        {
            return;
        }

        if (likely(enable_locality_))
        {
            mw_pool_loc_.push(mw);
        }
        else
        {
            mw_pool_.push(mw);
        }
    }
    bool empty() const
    {
        if (likely(enable_locality_))
        {
            return mw_pool_loc_.empty();
        }
        else
        {
            return mw_pool_.empty();
        }
    }
    ~MWPool()
    {
        size_t free_nr = 0;
        while (!mw_pool_.empty())
        {
            dsm_->free_mw(mw_pool_.front());
            mw_pool_.pop();
            free_nr++;
        }
        while (!mw_pool_loc_.empty())
        {
            dsm_->free_mw(mw_pool_loc_.top());
            mw_pool_loc_.pop();
            free_nr++;
        }
        LOG_IF(WARNING, free_nr != allocated_)
            << "[mw_pool] Possible memory leak: expect freeing " << allocated_
            << ", actual " << free_nr;
    }

private:
    DSM::pointer dsm_;
    size_t dir_id_{0};
    std::queue<ibv_mw *> mw_pool_;
    std::stack<ibv_mw *> mw_pool_loc_;
    size_t allocated_{0};
    bool enable_locality_;
};

struct MWAllocatorConfig
{
    std::shared_ptr<IAllocator> allocator;
    std::shared_ptr<MWPool> mw_pool;
    DSM::pointer dsm;
    size_t dir_id;
    size_t node_id;
    size_t thread_id;
};

/**
 * @brief Allocator wrapper for memory window
 * This allocator is not convenient to use, so its purpose is for performance
 * benchmarking.
 *
 * Use the raw allocator and bind the MW with your own code.
 *
 */
class MWAllocator : public IAllocator
{
public:
    MWAllocator(MWAllocatorConfig config) : conf_(config)
    {
        constexpr static size_t kMwNr = 1000;
        for (size_t i = 0; i < kMwNr; ++i)
        {
        }
    }

    void *alloc(size_t size,
                [[maybe_unused]] CoroContext *ctx = nullptr) override
    {
        auto *ret = conf_.allocator->alloc(size);
        if (ret != nullptr)
        {
            bind_mw(ctx, ret, size, nullptr);
        }
        DVLOG(10) << "[mw-alloc][allocation] allocate for size " << size
                  << ", ret: " << (void *) ret
                  << ". coro: " << pre_coro_ctx(ctx);
        return ret;
    }
    void free(void *addr, [[maybe_unused]] CoroContext *ctx = nullptr) override
    {
        if (addr != nullptr)
        {
            unbind_mw(ctx, addr);
        }
        DVLOG(10) << "[mw-alloc][allocation] free " << (void *) addr
                  << ". coro: " << pre_coro_ctx(ctx);
        conf_.allocator->free(addr);
    }
    void free(void *addr,
              size_t size,
              [[maybe_unused]] CoroContext *ctx = nullptr) override
    {
        if (addr != nullptr)
        {
            unbind_mw(ctx, addr);
        }
        DVLOG(10) << "[mw-alloc][allocation] free " << (void *) addr
                  << " for size " << size << ". coro: " << pre_coro_ctx(ctx);
        conf_.allocator->free(addr, size);
    }
    std::shared_ptr<IAllocator> get_internal_allocator()
    {
        return conf_.allocator;
    }

private:
    void bind_mw(CoroContext *ctx, void *addr, size_t size, ibv_mw *mw)
    {
        auto dir_id = conf_.dir_id;
        if (mw == nullptr)
        {
            mw = CHECK_NOTNULL(conf_.mw_pool->alloc());
        }
        ibv_qp *qp =
            conf_.dsm->get_dir_qp(conf_.node_id, conf_.thread_id, dir_id);
        auto *dsm_mr = conf_.dsm->get_dir_mr(dir_id);
        size_t coro_id = ctx == nullptr ? kNotACoro : ctx->coro_id();
        auto wr_id = WRID(WRID_PREFIX_PATRONUS_BIND_MW, coro_id);

        uint32_t rkey = rdmaAsyncBindMemoryWindow(
            qp, mw, dsm_mr, (uint64_t) addr, size, true, wr_id.val);
        CHECK_NE(rkey, 0);

        if (ctx != nullptr)
        {
            DVLOG(8) << "[mw-alloc] yield to master from " << *ctx;
            ctx->yield_to_master();
        }
        else
        {
            struct ibv_wc wc;
            int ret = pollWithCQ(conf_.dsm->get_dir_cq(dir_id), 1, &wc);
            CHECK_GE(ret, 0);
        }

        addr_to_mw_[addr] = mw;
    }
    void unbind_mw(CoroContext *ctx, void *addr)
    {
        auto it = addr_to_mw_.find(addr);
        if (it == addr_to_mw_.end())
        {
            LOG(FATAL) << "[mw-alloc] failed to free addr " << addr
                       << ", not allocated by me.";
        }
        bind_mw(ctx, addr, 1, it->second);
        conf_.mw_pool->free(it->second);
        addr_to_mw_.erase(it);
    }

    MWAllocatorConfig conf_;

    std::unordered_map<void *, ibv_mw *> addr_to_mw_;
};
}  // namespace patronus::mem

#endif