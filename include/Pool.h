#pragma once
#ifndef POOL_H_
#define POOL_H_

#include <atomic>
#include <queue>

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
            validity_check(addr);
            pool_.push(addr);
        }
    }
    void *get()
    {
        if (unlikely(pool_.empty()))
        {
            return nullptr;
        }
        void *ret = pool_.back();
        validity_check(ret);
        pool_.pop();
        return ret;
    }
    void put(void *buf)
    {
        validity_check(buf);
        pool_.push(buf);
    }

    uint64_t buf_to_id(void *buf)
    {
        validity_check(buf);
        auto idx = (buf - pool_addr_) / kBufferSize;
        return idx;
    }
    void *id_to_buf(uint64_t id)
    {
        void *ret = (char *) pool_addr_ + kBufferSize * id;
        validity_check(ret);
        return ret;
    }

private:
    void validity_check(void *buf)
    {
        DCHECK_NOTNULL(buf);
        DCHECK_GE(buf, pool_addr_)
            << "The buf at " << (void *) buf
            << " does not start from pool start addr " << (void *) pool_addr_;
        DCHECK_EQ(((char *) buf - (char *) pool_addr_) % kBufferSize, 0)
            << "The buf at " << (void *) buf
            << " does not aligned with buffer size " << kBufferSize;
        [[maybe_unused]] size_t idx =
            ((char *) buf - (char *) pool_addr_) / kBufferSize;
        DCHECK_LT(idx, buffer_nr_)
            << "The buf at " << (void *) buf << " overflow buffer length "
            << buffer_nr_ << ". idx: " << idx;
    }
    void *pool_addr_{nullptr};
    size_t pool_length_{0};
    size_t buffer_nr_{0};

    std::queue<void *> pool_;
};
template <typename T, size_t kSize>
class ThreadUnsafePool
{
public:
    ThreadUnsafePool()
    {
        for (size_t i = 0; i < kSize; ++i)
        {
            validity_check(&buffer_[i]);
            pool_.push(&buffer_[i]);
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
        validity_check(ret);
        return ret;
    }
    void put(T *obj)
    {
        validity_check(obj);
        pool_.push(obj);
    }
    uint64_t obj_to_id(T *obj)
    {
        validity_check(obj);
        auto idx = (obj - buffer_) / sizeof(T);
        return idx;
    }
    T *id_to_obj(uint64_t id)
    {
        T *ret = buffer_ + sizeof(T) * id;
        validity_check(ret);
        return ret;
    }

private:
    void validity_check(T *obj)
    {
        DCHECK_NOTNULL(obj);
        [[maybe_unused]] size_t idx = (obj - buffer_) / sizeof(T);
        DCHECK_EQ((obj - buffer_) % sizeof(T), 0)
            << "The obj at " << (void *) obj
            << " does not align to pool start addr " << (void *) buffer_;
        DCHECK_GE(obj, buffer_)
            << "The obj at " << (void *) obj
            << " does not start from pool start addr " << (void *) buffer_;
        DCHECK_LT(idx, kSize)
            << "The obj at " << (void *) obj << " overflow from buffer. idx "
            << idx << " greater than " << kSize;
    }

    std::queue<T *> pool_;
    T buffer_[kSize];
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
            return base_ + kObjSize * my_idx;
        }
    }
    void put(const void *buf)
    {
        char *buf_addr = (char *) buf;
        DCHECK_GT(buf_addr, base_);
        DCHECK_EQ((buf_addr - base_) % kObjSize, 0);
        size_t idx = (buf_addr - base_) / kObjSize;
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