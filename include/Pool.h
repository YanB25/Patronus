#pragma once
#ifndef POOL_H_
#define POOL_H_

#include <atomic>
#include <queue>
#include <set>
#include <stack>

#include "util/Debug.h"
#include "util/stacktrace.h"

class LocalityPoolImpl
{
public:
    constexpr static bool debug_pool()
    {
        return false;
    }
    constexpr static bool ndbg_pool()
    {
        return !debug_pool();
    }
    LocalityPoolImpl(void *addr, size_t buffer_nr, size_t buffer_size)
        : addr_(addr), buffer_nr_(buffer_nr), buffer_size_(buffer_size)
    {
        if constexpr (ndbg_pool())
        {
            for (size_t i = 0; i < buffer_nr; ++i)
            {
                void *buf_addr = ((char *) addr) + i * buffer_size;
                pool_loc_.push(buf_addr);
            }
        }
    }
    void *get()
    {
        if constexpr (ndbg_pool())
        {
            return ndbg_get();
        }
        else
        {
            return debug_get();
        }
    }
    size_t size() const
    {
        if constexpr (ndbg_pool())
        {
            return ndbg_size();
        }
        else
        {
            return debug_size();
        }
    }
    void put(void *addr)
    {
        if constexpr (ndbg_pool())
        {
            return ndbg_put(addr);
        }
        else
        {
            return debug_put(addr);
        }
    }
    uint64_t obj_to_id(void *addr)
    {
        if constexpr (ndbg_pool())
        {
            return ndbg_obj_to_id(addr);
        }
        else
        {
            return debug_obj_to_id(addr);
        }
    }
    void *id_to_obj(uint64_t id)
    {
        if constexpr (ndbg_pool())
        {
            return ndbg_id_to_obj(id);
        }
        else
        {
            return debug_id_to_obj(id);
        }
    }
    size_t ongoing_size() const
    {
        return ongoing_;
    }

private:
    void *addr_;
    size_t buffer_nr_;
    size_t buffer_size_;
    // below is regular
    std::stack<void *> pool_loc_;
    // below for debug
    std::unordered_map<uint64_t, void *> id_addr_;
    std::unordered_map<void *, uint64_t> addr_id_;
    ssize_t ongoing_{0};

    void *debug_get()
    {
        void *ret = (void *) malloc(buffer_size_);
        for (size_t i = 0; i < buffer_nr_; ++i)
        {
            if (id_addr_.count(i) == 0)
            {
                id_addr_[i] = ret;
                addr_id_[ret] = i;
                break;
            }
        }
        ongoing_++;
        DLOG_IF(FATAL, (size_t) ongoing_ > buffer_nr_)
            << "** allocating " << ongoing_ << "-th >= allowed " << buffer_nr_;

        CHECK_EQ(addr_id_.count(ret), 1);
        auto id = addr_id_[ret];
        CHECK_EQ(id_addr_.count(id), 1);
        CHECK_EQ(id_addr_[id], ret);
        return ret;
    }
    void *ndbg_get()
    {
        if (unlikely(pool_loc_.empty()))
        {
            return nullptr;
        }
        void *ret = pool_loc_.top();
        pool_loc_.pop();
        ongoing_++;
        return ret;
    }
    size_t debug_size() const
    {
        return buffer_nr_ - ongoing_;
    }
    size_t ndbg_size() const
    {
        return pool_loc_.size();
    }
    void ndbg_put(void *addr)
    {
        ongoing_--;
        pool_loc_.push(CHECK_NOTNULL(addr));
    }
    void debug_put(void *addr)
    {
        CHECK_EQ(addr_id_.count(addr), 1);
        auto id = addr_id_[addr];
        CHECK_EQ(id_addr_.count(id), 1);
        CHECK_EQ(id_addr_[id], addr);
        id_addr_.erase(id);
        addr_id_.erase(addr);
        ongoing_--;
        ::free(addr);
    }
    uint64_t debug_obj_to_id(void *addr)
    {
        CHECK_EQ(addr_id_.count(addr), 1);
        auto ret = addr_id_[addr];
        CHECK_EQ(id_addr_.count(ret), 1);
        CHECK_EQ(id_addr_[ret], addr);
        return ret;
    }
    uint64_t ndbg_obj_to_id(void *addr)
    {
        DCHECK_GE((char *) addr, (char *) addr_);
        auto offset = (char *) addr - (char *) addr_;
        DCHECK_EQ(offset % buffer_size_, 0);
        auto ret = offset / buffer_size_;
        DCHECK_GE(ret, 0);
        DCHECK_LT(ret, buffer_nr_);
        return ret;
    }
    void *debug_id_to_obj(uint64_t id)
    {
        CHECK_EQ(id_addr_.count(id), 1);
        void *ret = id_addr_[id];
        CHECK_EQ(addr_id_.count(ret), 1);
        CHECK_EQ(addr_id_[ret], id);
        return ret;
    }
    void *ndbg_id_to_obj(uint64_t id)
    {
        DCHECK_LT(id, buffer_nr_);
        return ((char *) addr_) + id * buffer_size_;
    }
};

template <typename T,
          std::enable_if_t<std::is_trivially_copyable_v<T>, bool> = true>
class LocalityObjectPool
{
public:
    LocalityObjectPool(size_t object_nr)
        : storage_(object_nr), pool_(storage_.data(), object_nr, sizeof(Object))
    {
    }
    LocalityObjectPool(void *buf, size_t buffer_size)
        : pool_(buf, buffer_size / sizeof(Object), sizeof(Object))
    {
    }
    T *get()
    {
        return (T *) pool_.get();
    }
    size_t size() const
    {
        return pool_.size();
    }
    void put(T *addr)
    {
        return pool_.put((void *) addr);
    }
    uint64_t obj_to_id(T *addr)
    {
        return pool_.obj_to_id((void *) addr);
    }
    T *id_to_obj(uint64_t id)
    {
        return (T *) pool_.id_to_obj(id);
    }
    size_t ongoing_size() const
    {
        return pool_.ongoing_size();
    }

private:
    using Object = std::aligned_storage_t<sizeof(T), alignof(T)>;
    static_assert(sizeof(Object) == sizeof(T),
                  "wrong use of std::aligned_storage_t");
    std::vector<Object> storage_;
    LocalityPoolImpl pool_;
};

class LocalityBufferPool
{
public:
    LocalityBufferPool(size_t buffer_nr, size_t buffer_size)
        : storage_(buffer_nr * buffer_size),
          pool_(storage_.data(), buffer_nr, buffer_size)
    {
    }
    LocalityBufferPool(void *addr, size_t addr_size, size_t buffer_size)
        : pool_(addr, addr_size / buffer_size, buffer_size)
    {
    }
    char *get()
    {
        return (char *) pool_.get();
    }
    size_t size() const
    {
        return pool_.size();
    }
    void put(void *addr)
    {
        return pool_.put((void *) addr);
    }
    uint64_t buf_to_id(void *addr)
    {
        return pool_.obj_to_id((void *) addr);
    }
    char *id_to_buf(uint64_t id)
    {
        return (char *) pool_.id_to_obj(id);
    }
    size_t ongoing_size() const
    {
        return pool_.ongoing_size();
    }

private:
    std::vector<char> storage_;
    LocalityPoolImpl pool_;
};

template <size_t kBufferSize, bool kLocality = false>
class ThreadUnsafeBufferPool
{
public:
    ThreadUnsafeBufferPool(void *pool, size_t size)
        : pool_addr_(pool), pool_length_(size)
    {
        buffer_nr_ = size / kBufferSize;
        for (size_t i = 0; i < buffer_nr_; ++i)
        {
            void *addr = (char *) pool_addr_ + i * kBufferSize;
            debug_validity_check(addr);
            if constexpr (kLocality)
            {
                pool_loc_.push(addr);
            }
            else
            {
                pool_.push(addr);
            }

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
            if (pool_.size() + pool_loc_.size() != buffer_nr_)
            {
                LOG(WARNING)
                    << "Possible memory leak for ThreadUnsafeBufferPool at "
                    << (void *) this << ", expect " << buffer_nr_ << ", got "
                    << pool_.size() << " + " << pool_loc_.size()
                    << util::stack_trace;
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
        void *ret = nullptr;
        if constexpr (kLocality)
        {
            if (unlikely(pool_loc_.empty()))
            {
                return nullptr;
            }
            ret = pool_loc_.top();
            pool_loc_.pop();
            on_going_++;
        }
        else
        {
            if (unlikely(pool_.empty()))
            {
                return nullptr;
            }
            ret = pool_.front();
            pool_.pop();
            on_going_++;
        }
        if constexpr (debug())
        {
            debug_validity_get(ret);
            debug_validity_check(ret);
        }
        return ret;
    }
    size_t size() const
    {
        if constexpr (kLocality)
        {
            return pool_loc_.size();
        }
        else
        {
            return pool_.size();
        }
    }
    size_t onging_size() const
    {
        return on_going_;
    }
    void put(void *buf)
    {
        if constexpr (debug())
        {
            debug_validity_put(buf);
            debug_validity_check(buf);
        }
        if constexpr (kLocality)
        {
            pool_loc_.push(buf);
        }
        else
        {
            pool_.push(buf);
        }
        on_going_--;
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
    std::stack<void *> pool_loc_;

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
        buffer_ = new T[kSize];
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
        delete[] buffer_;
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
    T *buffer_;

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