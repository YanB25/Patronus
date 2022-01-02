#pragma once
#ifndef SHERMAN_DEBUG_H
#define SHERMAN_DEBUG_H

#include <glog/logging.h>

#ifdef NDEBUG
template <typename T>
class Debug
{
public:
    Debug()
    {
    }
    Debug(const T &)
    {
        LOG(FATAL) << "** accessing debug data in release mode.";
    }
    Debug<T> &operator=(const Debug<T> &) = default;
    Debug(const Debug<T> &) = default;
    Debug<T> &operator=(const T &)
    {
        LOG(FATAL) << "** accessing debug data in release mode.";
        return *this;
    }
    void set(const T &)
    {
        LOG(FATAL) << "** accessing debug data in release mode.";
    }
    T &get()
    {
        LOG(FATAL) << "** accessing debug data in release mode.";
    }
    const T &get() const
    {
        LOG(FATAL) << "** accessing debug data in release mode.";
    }
    ~Debug() = default;
    bool operator==(const Debug<T> &) const
    {
        LOG(FATAL) << "** accessing debug data in release mode.";
        return false;
    }
    bool operator==(const T &) const
    {
        LOG(FATAL) << "** accessing debug data in release mode.";
        return false;
    }
    template <typename U>
    friend std::ostream &operator<<(std::ostream &os, const Debug<U> &debug);

private:
};
static_assert(sizeof(Debug<int>) == 1);

template <typename T>
std::ostream &operator<<(std::ostream &os, const Debug<T> &)
{
    LOG(FATAL) << "** accessing debug data in release mode.";
    return os;
}

#else

template <typename T>
class Debug
{
public:
    Debug() = default;
    Debug(const T &t) : t_(t)
    {
    }
    Debug(const Debug<T> &rhs)
    {
        t_ = rhs.t_;
    }
    Debug<T> &operator=(const Debug<T> &rhs)
    {
        t_ = rhs.t_;
        return *this;
    }
    ~Debug() = default;

    void set(const T &t)
    {
        t_ = t;
    }
    T &get()
    {
        return t_;
    }
    const T &get() const
    {
        return t_;
    }
    bool operator==(const Debug<T> &rhs) const
    {
        return t_ == rhs.t_;
    }
    bool operator==(const T &rhs) const
    {
        return t_ == rhs;
    }
    Debug<T> &operator=(const T &t)
    {
        t_ = t;
        return *this;
    }
    template <typename U>
    friend std::ostream &operator<<(std::ostream &, const Debug<U> &);

private:
    T t_;
};
static_assert(sizeof(Debug<int>) == sizeof(int));

template <typename T>
std::ostream &operator<<(std::ostream &os, const Debug<T> &debug)
{
    os << debug.t_;
    return os;
}
#endif

#endif