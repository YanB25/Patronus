#pragma once
#ifndef POOL_H_
#define POOL_H_

#include <atomic>
#include <queue>
#include <set>

#include "util/Debug.h"
#include "util/stacktrace.h"

template <size_t kBufferSize>
class ThreadUnsafeBufferPool
{
public:
    ThreadUnsafeBufferPool(void *pool, size_t size)
        : pool_addr_(pool), pool_length_(size)
    {
        CHECK_EQ(size % kBufferSize, 0);
        buffer_nr_ = size / kBufferSize;
        for (size_t i = 0; i < buffer_nr_; ++i)
        {
            void *addr = (char *) pool_addr_ + i * kBufferSize;
            debug_validity_check(addr);
            pool_.push(addr);

            if constexpr (debug())
            {
                DCHECK_EQ(inner_.get().count(addr), 0);
                inner_.get().insert(addr);
            }
        }
    }
    ~ThreadUnsafeBufferPool()
    {
        if constexpr (debug())
        {
            if (pool_.size() != buffer_nr_)
            {
                LOG(WARNING)
                    << "Possible memory leak for ThreadUnsafeBufferPool at "
                    << (void *) this << ", expect " << buffer_nr_ << ", got "
                    << pool_.size() << util::stack_trace;
            }
        }
    }
    void debug_validity_get(void *addr)
    {
        DCHECK(outer_.get().insert(addr).second);
        DCHECK_EQ(inner_.get().erase(addr), 1);
    }
    void debug_validity_put(void *addr)
    {
        DCHECK_EQ(outer_.get().erase(addr), 1)
            << "addr " << addr << " not out-going buffer.";
        DCHECK(inner_.get().insert(addr).second)
            << "internal err: addr " << addr << " insert failed.";
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
            debug_validity_get(ret);
            debug_validity_check(ret);
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
            debug_validity_put(buf);
            debug_validity_check(buf);
        }
    }

    uint64_t buf_to_id(void *buf)
    {
        debug_validity_check(buf);
        auto idx = ((uint64_t) buf - (uint64_t) pool_addr_) / kBufferSize;
        return idx;
    }
    void *id_to_buf(uint64_t id)
    {
        void *ret = (char *) pool_addr_ + kBufferSize * id;
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
            [[maybe_unused]] size_t idx = diff / kBufferSize;
            DCHECK_GE(buf, pool_addr_)
                << "The buf at " << (void *) buf
                << " does not start from pool start addr "
                << (void *) pool_addr_;
            DCHECK_EQ(diff % kBufferSize, 0)
                << "The buf at " << (void *) buf
                << " does not aligned with buffer size " << kBufferSize;
            DCHECK_LT(idx, buffer_nr_)
                << "The buf at " << (void *) buf << " overflow buffer length "
                << buffer_nr_ << ". idx: " << idx;
            DCHECK_GE(on_going_, 0);
            DCHECK_LE(on_going_, buffer_nr_);
        }
    }

private:
    void *pool_addr_{nullptr};
    size_t pool_length_{0};
    size_t buffer_nr_{0};

    std::queue<void *> pool_;

    int64_t on_going_{0};

    Debug<std::set<const void *>> outer_;
    Debug<std::set<const void *>> inner_;
};
template <typename T, size_t kSize>
class ThreadUnsafePool
{
public:
    ThreadUnsafePool()
    {
        for (size_t i = 0; i < kSize; ++i)
        {
            debug_validity_check(&buffer_[i]);
            pool_.push(&buffer_[i]);
            if constexpr (debug())
            {
                DCHECK(inner_.get().insert(&buffer_[i]).second);
            }
        }
    }
    ~ThreadUnsafePool()
    {
        if (pool_.size() != kSize)
        {
            LOG(WARNING) << "** Possible memory leak for ThreadUnsafePool at "
                         << (void *) this << ", expect " << kSize << ", got "
                         << pool_.size() << util::stack_trace;
        }
    }

    T *get()
    {
        if (unlikely(pool_.empty()))
        {
            return nullptr;
        }
        T *ret = pool_.front();
        pool_.pop();
        on_going_++;
        if constexpr (debug())
        {
            debug_validity_get(ret);
            debug_validity_check(ret);
        }
        return ret;
    }
    void put(T *obj)
    {
        pool_.push(obj);
        on_going_--;
        if constexpr (debug())
        {
            debug_validity_put(obj);
            debug_validity_check(obj);
        }
    }
    uint64_t obj_to_id(T *obj)
    {
        debug_validity_check(obj);
        auto idx = obj - buffer_;
        return idx;
    }
    T *id_to_obj(uint64_t id)
    {
        T *ret = buffer_ + id;
        debug_validity_check(ret);
        return ret;
    }

    size_t ongoing_size() const
    {
        return on_going_;
    }
    size_t size() const
    {
        return kSize - on_going_;
    }
    void debug_validity_get(const T *obj)
    {
        DCHECK_EQ(inner_.get().erase(obj), 1);
        DCHECK(outer_.get().insert(obj).second);
    }
    void debug_validity_put(const T *obj)
    {
        DCHECK(inner_.get().insert(obj).second);
        DCHECK_EQ(outer_.get().erase(obj), 1);
    }
    void debug_validity_check(const T *obj)
    {
        if constexpr (debug())
        {
            DCHECK_NOTNULL(obj);
            [[maybe_unused]] ssize_t diff = (uint64_t) obj - (uint64_t) buffer_;
            [[maybe_unused]] size_t idx = diff / sizeof(T);
            DCHECK_EQ(diff % sizeof(T), 0)
                << "The obj at " << (void *) obj
                << " does not align to pool start addr " << (void *) buffer_;
            DCHECK_GE(obj, buffer_)
                << "The obj at " << (void *) obj
                << " does not start from pool start addr " << (void *) buffer_;
            DCHECK_LT(idx, kSize) << "The obj at " << (void *) obj
                                  << " overflow from buffer. idx " << idx
                                  << " greater than " << kSize;
            DCHECK_GE(on_going_, 0);
            DCHECK_LE(on_going_, kSize);
        }
    }

private:
    std::queue<T *> pool_;
    T buffer_[kSize];

    size_t on_going_{0};

    Debug<std::set<const T *>> outer_;
    Debug<std::set<const T *>> inner_;
};

template <size_t kObjSize>
class Pool
{
public:
    Pool(void *base, size_t length) : base_(base), length_(length)
    {
        CHECK_GT(length, kObjSize)
            << "length should at least be greater than ONE object size";
        allocated_.resize(obj_nr());
    }

    void *get()
    {
        size_t tried = 0;
        while (true)
        {
            size_t my_idx =
                (idx_.fetch_add(1, std::memory_order_relaxed) % obj_nr());
            if (allocated_[my_idx])
            {
                if (tried++ > obj_nr())
                {
                    return nullptr;
                }
                continue;
            }
            allocated_[my_idx] = true;
            return (char *) base_ + kObjSize * my_idx;
        }
    }
    void put(const void *buf)
    {
        char *buf_addr = (char *) buf;
        DCHECK_GT(buf_addr, base_);
        DCHECK_EQ((buf_addr - (char *) base_) % kObjSize, 0);
        size_t idx = (buf_addr - (char *) base_) / kObjSize;
        DCHECK(allocated_[idx]);
        allocated_[idx] = false;
    }

    size_t obj_nr() const
    {
        return length_ / kObjSize;
    }

private:
    void *base_;
    size_t length_;
    std::atomic<size_t> idx_{0};
    std::vector<bool> allocated_;
};

#endif