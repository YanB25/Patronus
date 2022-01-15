#pragma once
#ifndef PER_THREAD_H_
#define PER_THREAD_H_

#include "Common.h"

template <typename T, size_t kAligned>
class Aligned
{
public:
    Aligned(T t) : t_(t)
    {
    }
    Aligned(const Aligned &rhs) : t_(rhs)
    {
    }
    Aligned() : t_({})
    {
    }
    T &operator=(const Aligned &rhs)
    {
        t_ = rhs.t_;
        return *this;
    }
    Aligned(Aligned &&rhs) : t_(std::move(rhs.t_))
    {
    }
    T &operator=(Aligned &&rhs)
    {
        t_ = std::move(rhs.t_);
        return *this;
    }
    T &get()
    {
        return *pget();
    }
    const T &get() const
    {
        return *pget();
    }

private:
    T *pget()
    {
        return (T *) &t_;
    }
    const T *pget() const
    {
        return (const T *) &t_;
    }

    using aligned_t = typename std::aligned_storage<sizeof(T), kAligned>::type;
    aligned_t t_;
};

template <typename T>
class Perthread
{
public:
    using value_type = T;
    using size_type = std::size_t;
    using pointer = value_type *;
    using const_pointer = const value_type *;

    value_type &operator[](size_t idx)
    {
        return per_thread_[idx].get();
    }
    const value_type &operator[](size_t idx) const
    {
        return per_thread_[idx].get();
    }
    size_type size() const
    {
        return per_thread_.size();
    }
    bool empty() const
    {
        return per_thread_.empty();
    }
    template <typename U>
    void fill(const U &value)
    {
        for (size_t i = 0; i < per_thread_.size(); ++i)
        {
            per_thread_[i].get() = value;
        }
    }

private:
    constexpr static size_t kCachelineSize = 64;
    using aligned_t = Aligned<T, kCachelineSize>;
    std::array<aligned_t, MAX_APP_THREAD> per_thread_;
};
#endif