#pragma once
#ifndef MEMORY_BLOCK_ALLOCATOR_H
#define MEMORY_BLOCK_ALLOCATOR_H

#include <algorithm>
#include <cstddef>
#include <memory>
#include <unordered_set>

#include "Pool.h"
#include "allocator.h"

namespace patronus::mem
{
using namespace define::literals;

struct SlabAllocatorConfig
{
    std::vector<size_t> block_class;
    std::vector<double> block_ratio;
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
            LOG(WARNING) << "Possible memory leak for Pool at " << (void *) this
                         << ", expect " << buffer_nr_ << ", got "
                         << pool_.size()
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

class SlabAllocator : public IAllocator
{
public:
    SlabAllocator(void *addr, size_t len, SlabAllocatorConfig config)
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
            auto cur_len = aligned_len * cur_ratio;
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

    void *alloc(size_t size,
                [[maybe_unused]] CoroContext *ctx = nullptr) override
    {
        auto it = blocks_.lower_bound(size);
        if (it == blocks_.end())
        {
            // no such large block
            return nullptr;
        }
        auto *ret = it->second->get();
        if constexpr (debug())
        {
            if (ret)
            {
                debug_class_allocated_nr_[it->first]++;
                if constexpr (config::kEnableSlabAllocatorStrictChecking)
                {
                    CHECK(debug_ongoing_bufs_.insert(ret).second)
                        << "The returned pair.second denotes whether insertion "
                           "succeeds. Expect to insert a new element";
                }
            }
        }
        DVLOG(20) << "[slab-alloc] allocating size " << size << " from class "
                  << it->first << ". ret: " << ret;
        return ret;
    }
    void free(void *addr, [[maybe_unused]] CoroContext *ctx = nullptr) override
    {
        CHECK_GE(addr, pool_start_addr_);
        auto it = end_addr_to_class_.upper_bound(addr);
        if (it == end_addr_to_class_.end())
        {
            DCHECK(false) << "[alloc] can not find class for addr " << addr;
        }
        size_t ptr_class = it->second;
        blocks_[ptr_class]->put(addr);
        if constexpr (debug())
        {
            debug_class_freed_nr_[ptr_class]++;
            if constexpr (config::kEnableSlabAllocatorStrictChecking)
            {
                CHECK_EQ(debug_ongoing_bufs_.erase(addr), 1)
                    << "Expect addr " << (void *) addr << " found in the set";
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
        for (const auto &[clz, alloc_nr] : debug_class_allocated_nr_)
        {
            ret += clz * alloc_nr;
        }
        return ret;
    }
    size_t debug_freed_bytes() const
    {
        size_t ret = 0;
        for (const auto &[clz, freed_nr] : debug_class_freed_nr_)
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

    std::map<size_t, std::unique_ptr<Pool>> blocks_;
    std::map<void *, size_t> end_addr_to_class_;

    std::map<size_t, ClassInformation> class_info_;
    size_t wasted_len_{0};
    std::unordered_map<size_t, size_t> debug_class_allocated_nr_;
    std::unordered_map<size_t, size_t> debug_class_freed_nr_;
    std::unordered_set<void *> debug_ongoing_bufs_;
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
    if constexpr (debug())
    {
        os << "Allocated: [";
        for (const auto &[cls, nr] : allocator.debug_class_allocated_nr_)
        {
            auto free_nr = allocator.debug_class_freed_nr_.find(cls)->second;
            os << "(" << cls << ", "
               << "alloc: " << nr << ", freed: " << free_nr << "), ";
        }
        os << "]; ";
    }
    os << "}";
    return os;
}
}  // namespace patronus::mem

#endif