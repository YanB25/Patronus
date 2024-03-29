#pragma once
#ifndef MEMORY_BLOCK_ALLOCATOR_H
#define MEMORY_BLOCK_ALLOCATOR_H

#include <algorithm>
#include <cstddef>
#include <memory>
#include <unordered_set>

#include "Common.h"
#include "Pool.h"
#include "allocator.h"

using namespace util::literals;
namespace patronus::mem
{
struct SlabAllocatorConfig
{
    std::vector<size_t> block_class;
    std::vector<double> block_ratio;
    // If enable_recycle is true, the allocator will simulate infinite memory
    // capacity by re-allocate out the same piece of memory multiple times.
    // Be aware: this may cause correctness problem for users
    bool enable_recycle{false};
};

class Pool
{
public:
    Pool(void *pool, size_t size, size_t buffer_size)
        : pool_addr_(pool), buffer_size_(buffer_size)
    {
        buffer_nr_ = size / buffer_size_;
        for (size_t i = 0; i < buffer_nr_; ++i)
        {
            void *addr = (char *) pool_addr_ + i * buffer_size_;
            debug_validity_check(addr);
            pool_.push(addr);

            if constexpr (debug())
            {
                DCHECK_EQ(inner_.get().count(addr), 0);
                inner_.get().insert(addr);
            }
        }
    }
    ~Pool()
    {
        if (pool_.size() != buffer_nr_)
        {
            DLOG(WARNING) << "Possible memory leak for Pool at "
                          << (void *) this << ", expect " << buffer_nr_
                          << ", got " << pool_.size()
                          << ". Possible leak: " << buffer_nr_ - pool_.size();
            //  << pool_.size() << util::stack_trace;
        }
    }
    void debug_validity_get(void *addr)
    {
        if constexpr (debug())
        {
            DCHECK(outer_.get().insert(addr).second)
                << "Inserting addr: " << (void *) addr;
            auto ret = inner_.get().erase(addr);
            DCHECK_EQ(ret, 1)
                << "erasing " << (void *) addr << " should hit and succeed ";
        }
    }
    void debug_validity_put(void *addr)
    {
        DCHECK_EQ(outer_.get().erase(addr), 1);
        DCHECK(inner_.get().insert(addr).second);
    }
    void *get()
    {
        if (unlikely(pool_.empty()))
        {
            return nullptr;
        }
        void *ret = pool_.front();
        pool_.pop();
        on_going_++;
        if constexpr (debug())
        {
            debug_validity_check(ret);
            debug_validity_get(ret);
        }
        return ret;
    }
    size_t size() const
    {
        return pool_.size();
    }
    size_t onging_size() const
    {
        return on_going_;
    }
    void put(void *buf)
    {
        pool_.push(buf);
        on_going_--;
        if constexpr (debug())
        {
            debug_validity_check(buf);
            debug_validity_put(buf);
        }
    }

    uint64_t buf_to_id(void *buf)
    {
        debug_validity_check(buf);
        auto idx = ((uint64_t) buf - (uint64_t) pool_addr_) / buffer_size_;
        return idx;
    }
    void *id_to_buf(uint64_t id)
    {
        void *ret = (char *) pool_addr_ + buffer_size_ * id;
        debug_validity_check(ret);
        return ret;
    }

    void debug_validity_check(const void *buf)
    {
        if constexpr (debug())
        {
            DCHECK_NOTNULL(buf);
            [[maybe_unused]] ssize_t diff =
                (uint64_t) buf - (uint64_t) pool_addr_;
            [[maybe_unused]] size_t idx = diff / buffer_size_;
            DCHECK_GE(buf, pool_addr_)
                << "The buf at " << (void *) buf
                << " does not start from pool start addr "
                << (void *) pool_addr_;
            DCHECK_EQ(diff % buffer_size_, 0)
                << "The buf at " << (void *) buf
                << " does not aligned with buffer size " << buffer_size_;
            DCHECK_LT(idx, buffer_nr_)
                << "The buf at " << (void *) buf << " overflow buffer length "
                << buffer_nr_ << ". idx: " << idx;
            DCHECK_GE(on_going_, 0);
            DCHECK_LE(on_going_, buffer_nr_);
        }
    }

private:
    void *pool_addr_{nullptr};
    size_t buffer_nr_{0};
    size_t buffer_size_{0};

    std::queue<void *> pool_;

    int64_t on_going_{0};

    Debug<std::set<const void *>> outer_;
    Debug<std::set<const void *>> inner_;
};

struct ClassInformation
{
    ClassInformation(void *s, size_t l, size_t nr)
        : start_addr(s), len(l), slab_nr(nr)
    {
    }
    ClassInformation(const ClassInformation &rhs) = default;
    void *start_addr;
    size_t len;
    size_t slab_nr;
};
inline std::ostream &operator<<(std::ostream &os, const ClassInformation &info)
{
    os << "addr: " << (void *) info.start_addr << ", len: " << info.len
       << ", slab_nr: " << info.slab_nr;
    return os;
}

class pre_class_info
{
public:
    pre_class_info(const std::map<size_t, ClassInformation> &ci) : ci_(ci)
    {
    }

    friend std::ostream &operator<<(std::ostream &os, const pre_class_info &p);

private:
    const std::map<size_t, ClassInformation> &ci_;
};
inline std::ostream &operator<<(std::ostream &os, const pre_class_info &p)
{
    for (const auto &[size, class_info] : p.ci_)
    {
        os << "size: " << size << " info: " << class_info << std::endl;
    }
    return os;
}
class pre_alloc_dist
{
public:
    pre_alloc_dist(const std::unordered_map<size_t, size_t> &dist) : dist_(dist)
    {
    }

    friend std::ostream &operator<<(std::ostream &os, const pre_alloc_dist &p);

private:
    const std::unordered_map<size_t, size_t> &dist_;
};
inline std::ostream &operator<<(std::ostream &os, const pre_alloc_dist &p)
{
    for (const auto &[size, times] : p.dist_)
    {
        os << "Size " << size << " allocated " << times << " times"
           << std::endl;
    }
    return os;
}

class SlabAllocator : public IAllocator
{
public:
    using pointer = std::shared_ptr<SlabAllocator>;
    static pointer new_instance(void *addr,
                                size_t len,
                                SlabAllocatorConfig config)
    {
        return std::make_shared<SlabAllocator>(addr, len, config);
    }
    SlabAllocator(void *addr, size_t len, SlabAllocatorConfig config)
        : config_(config)
    {
        auto [aligned_addr, aligned_len] = align_address(addr, len, 4_KB);
        pool_start_addr_ = aligned_addr;
        wasted_len_ += ((uint64_t) aligned_addr - (uint64_t) addr);

        double ratio_sum = 0;
        for (auto d : config.block_ratio)
        {
            ratio_sum += d;
        }

        CHECK_LT(abs(ratio_sum - 1), 0.01)
            << "block_ratio does not sum up to 1";
        CHECK_EQ(config.block_class.size(), config.block_ratio.size());

        void *begin_addr = aligned_addr;

        for (size_t i = 0; i < config.block_class.size(); ++i)
        {
            auto cur_class = config.block_class[i];
            auto cur_ratio = config.block_ratio[i];
            auto cur_len = 1.0 * aligned_len * cur_ratio;
            auto cur_addr = begin_addr;
            auto [a, l] =
                align_address(cur_addr, cur_len, std::min(cur_class, 4_KB));
            blocks_[cur_class] = std::make_unique<Pool>(a, l, cur_class);

            end_addr_to_class_[(void *) ((uint64_t) a + l)] = cur_class;

            begin_addr = (void *) ((uint64_t) a + l);

            class_info_.emplace(cur_class,
                                ClassInformation(a, l, l / cur_class));
            wasted_len_ += ((uint64_t) a - (uint64_t) cur_addr);
        }
    }
    ~SlabAllocator()
    {
        if constexpr (::config::kMonitorSlabAllocator)
        {
            DLOG_IF(INFO, !allocated_distribution.empty())
                << "[slab][report] Allocation distribution: "
                << pre_alloc_dist(allocated_distribution);
        }
    }

    void *alloc(size_t size,
                [[maybe_unused]] CoroContext *ctx = nullptr) override
    {
        auto it = blocks_.lower_bound(size);
        if (it == blocks_.end())
        {
            // no such large block
            CHECK(false) << "[slab] no such large block. Requested: " << size
                         << ". Class: " << pre_class_info(class_info_);
            return nullptr;
        }
        auto *ret = it->second->get();
        if (unlikely(config_.enable_recycle))
        {
            it->second->put(ret);
        }
        if constexpr (::config::kMonitorSlabAllocator)
        {
            if (ret)
            {
                class_allocated_nr_[it->first]++;
                if constexpr (::config::kEnableSlabAllocatorStrictChecking)
                {
                    CHECK(debug_ongoing_bufs_.insert(ret).second)
                        << "The returned pair.second denotes whether insertion "
                           "succeeds. Expect to insert a new element";
                }
            }
            else
            {
                LOG(ERROR) << "Failed to allocate size " << size
                           << ". allocated from this class: "
                           << class_allocated_nr_[it->first]
                           << ". class: " << std::endl
                           << pre_class_info(class_info_);
            }

            if constexpr (::config::kMonitorSlabAllocator)
            {
                allocated_distribution[size]++;
            }
        }

        if constexpr (::config::kMonitorSlabAllocator)
        {
            LOG_IF(WARNING, ret == nullptr)
                << "Failed to allocate size " << size << ": "
                << pre_class_info(class_info_)
                << ". Allocated: " << class_allocated_nr_[it->first]
                << ", free: " << class_freed_nr_[it->first];
        }
        else
        {
            LOG_IF(WARNING, ret == nullptr)
                << "Failed to allocate size " << size
                << ". Slab info: " << std::endl
                << pre_class_info(class_info_);
        }

        return ret;
    }
    void free(void *addr, [[maybe_unused]] CoroContext *ctx = nullptr) override
    {
        if (unlikely(config_.enable_recycle))
        {
            return;
        }

        CHECK_GE(addr, pool_start_addr_);
        auto it = end_addr_to_class_.upper_bound(addr);
        if (it == end_addr_to_class_.end())
        {
            CHECK(false) << "[alloc] can not find class for addr " << addr
                         << ". Begin address: "
                         << end_addr_to_class_.begin()->first
                         << ", end: " << (--end_addr_to_class_.end())->first;
        }
        size_t ptr_class = it->second;
        blocks_[ptr_class]->put(addr);

        if constexpr (::config::kMonitorSlabAllocator)
        {
            class_freed_nr_[ptr_class]++;
            if constexpr (::config::kEnableSlabAllocatorStrictChecking)
            {
                CHECK_EQ(debug_ongoing_bufs_.erase(addr), 1)
                    << "Expect addr " << (void *) addr
                    << " found in the set. It is not allocated by me.";
            }
        }
        DVLOG(20) << "[slab-alloc] freeing " << addr;
    }
    void free(void *addr,
              [[maybe_unused]] size_t size,
              [[maybe_unused]] CoroContext *ctx = nullptr) override
    {
        // TODO(patronus): when debug is ON, try to validate the size
        DVLOG(20) << "[slab-alloc] freeing " << addr << " with size " << size;
        return free(addr);
    }
    size_t debug_allocated_bytes() const
    {
        size_t ret = 0;
        for (const auto &[clz, alloc_nr] : class_allocated_nr_)
        {
            ret += clz * alloc_nr;
        }
        return ret;
    }
    size_t debug_freed_bytes() const
    {
        size_t ret = 0;
        for (const auto &[clz, freed_nr] : class_freed_nr_)
        {
            ret += clz * freed_nr;
        }
        return ret;
    }
    ssize_t debug_effective_allocated_bytes() const
    {
        return debug_allocated_bytes() - debug_allocated_bytes();
    }
    friend std::ostream &operator<<(std::ostream &os,
                                    const SlabAllocator &allocator);

private:
    void *pool_start_addr_{nullptr};
    SlabAllocatorConfig config_;

    std::map<size_t, std::unique_ptr<Pool>> blocks_;
    std::map<void *, size_t> end_addr_to_class_;

    std::map<size_t, ClassInformation> class_info_;
    size_t wasted_len_{0};
    std::unordered_map<size_t, size_t> class_allocated_nr_;
    std::unordered_map<size_t, size_t> class_freed_nr_;
    std::unordered_set<void *> debug_ongoing_bufs_;

    // alloc_size => alloc_num
    std::unordered_map<size_t, size_t> allocated_distribution;
};

inline std::ostream &operator<<(std::ostream &os,
                                const SlabAllocator &allocator)
{
    os << "{SlabAllocator: ";
    for (auto [cls, info] : allocator.class_info_)
    {
        os << "[" << cls << ", " << info << "]; ";
    }
    os << "wasted: " << allocator.wasted_len_ << "; ";
    if constexpr (::config::kMonitorSlabAllocator)
    {
        os << "Allocated: [";
        for (const auto &[cls, nr] : allocator.class_allocated_nr_)
        {
            auto free_nr = allocator.class_freed_nr_.find(cls)->second;
            os << "(" << cls << ", "
               << "alloc: " << nr << ", freed: " << free_nr << "), ";
        }
        os << "]; ";
    }
    os << "}";
    return os;
}

struct RefillableSlabAllocatorConfig
{
    std::vector<size_t> block_class;
    std::vector<double> block_ratio;
    IAllocator::pointer refill_allocator;
    size_t refill_block_size;
};
/**
 * TODO: RefillableSlabAllocator never actual frees anything.
 *
 */
class RefillableSlabAllocator : public IAllocator
{
public:
    using pointer = std::shared_ptr<RefillableSlabAllocator>;
    RefillableSlabAllocator(const RefillableSlabAllocatorConfig &config)
        : config_(config)
    {
    }
    static pointer new_instance(const RefillableSlabAllocatorConfig &config)
    {
        return std::make_shared<RefillableSlabAllocator>(config);
    }
    void *alloc(size_t size, CoroContext *ctx = nullptr) override
    {
        return do_alloc(size, true, ctx);
    }
    void free(void *addr, CoroContext *ctx = nullptr) override
    {
        DCHECK_NOTNULL(allocator_)->free(addr, ctx);
    }
    void free(void *addr, size_t size, CoroContext *ctx = nullptr) override
    {
        DCHECK_NOTNULL(allocator_)->free(addr, size, ctx);
    }
    ~RefillableSlabAllocator()
    {
        LOG_IF(WARNING, refill_nr_ > 1)
            << "[refill-slab] RefillableSlabAllocator refilled " << refill_nr_
            << " times.";
    }

private:
    void *do_alloc(size_t size, bool retry, CoroContext *ctx)
    {
        if (unlikely(allocator_ == nullptr))
        {
            if (retry)
            {
                refill(ctx);
                return do_alloc(size, false, ctx);
            }
            return nullptr;
        }
        auto *ret = DCHECK_NOTNULL(allocator_)->alloc(size, ctx);
        if (unlikely(ret == nullptr))
        {
            if (retry)
            {
                refill(ctx);
                return do_alloc(size, false, ctx);
            }
            return nullptr;
        }
        return ret;
    }
    void refill(CoroContext *ctx)
    {
        DLOG(INFO) << "[refill-slab] refill: RefillableSlabAllocator triggered "
                      "refill to allocate size "
                   << config_.refill_block_size
                   << ". coro: " << pre_coro_ctx(ctx);
        SlabAllocatorConfig slab_conf;
        slab_conf.block_class = config_.block_class;
        slab_conf.block_ratio = config_.block_ratio;
        void *alloc_buffer =
            config_.refill_allocator->alloc(config_.refill_block_size, ctx);
        if (unlikely(alloc_buffer == nullptr))
        {
            // can not allocate more
            return;
        }
        allocator_ = SlabAllocator::new_instance(
            alloc_buffer, config_.refill_block_size, slab_conf);
        refill_nr_++;
    }
    RefillableSlabAllocatorConfig config_;
    IAllocator::pointer allocator_;
    size_t refill_nr_{0};
};

}  // namespace patronus::mem

#endif