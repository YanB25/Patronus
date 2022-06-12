#pragma once
#ifndef SHERMEM_UTIL_PRE_H_
#define SHERMEM_UTIL_PRE_H_

#include <iostream>
#include <set>
#include <vector>

namespace util
{
template <typename T>
struct pre_vec
{
    pre_vec(const std::vector<T> &vec,
            size_t limit = std::numeric_limits<size_t>::max())
        : v(vec), limit_(limit)
    {
    }
    size_t limit() const
    {
        return limit_;
    }
    const std::vector<T> &v;
    size_t limit_;
};

template <typename T>
inline std::ostream &operator<<(std::ostream &os, pre_vec<T> vec)
{
    const auto &v = vec.v;
    bool ellipsis = v.size() > vec.limit();
    auto limit = std::min(vec.limit(), v.size());
    auto end_size = limit / 2;
    auto before_size = limit - end_size;
    size_t ommitted = vec.v.size() - limit;

    os << "[";
    for (size_t i = 0; i < before_size; ++i)
    {
        os << v[i] << ", ";
    }
    if (ellipsis)
    {
        os << "...";
        if (end_size > 0)
        {
            os << ", ";
        }
    }
    for (size_t i = 0; i < end_size; ++i)
    {
        auto idx = v.size() - end_size + i;
        bool last = i + 1 == end_size;
        if (!last)
        {
            os << v[idx] << ", ";
        }
        else
        {
            os << v[idx];
        }
    }
    os << "] (sz: " << v.size() << ", ommitted " << ommitted << ")";
    return os;
}

template <typename T, typename U>
struct pre_pair
{
    pre_pair(const std::pair<T, U> &pa) : p(pa)
    {
    }
    const std::pair<T, U> &p;
};
template <typename T, typename U>
inline std::ostream &operator<<(std::ostream &os, const pre_pair<T, U> &p)
{
    os << "(" << p.p.first << ", " << p.p.second << ")";
    return os;
}

template <typename T, typename U>
struct pre_make_pair
{
    pre_make_pair(const T &t, const U &u) : t_(t), u_(u)
    {
    }
    const T &t_;
    const U &u_;
};
template <typename T, typename U>
inline std::ostream &operator<<(std::ostream &os, const pre_make_pair<T, U> &p)
{
    os << "(" << p.t_ << ", " << p.u_ << ")";
    return os;
}

template <typename K, typename V>
struct pre_map
{
    pre_map(const std::map<K, V> &m,
            size_t limit = std::numeric_limits<size_t>::max())
        : m_(m), limit_(limit)
    {
    }
    size_t limit() const
    {
        return limit_;
    }
    const std::map<K, V> m_;
    size_t limit_;
};

template <typename K, typename V>
inline std::ostream &operator<<(std::ostream &os, const pre_map<K, V> &m)
{
    os << "{";
    bool ellipsis = m.m_.size() > m.limit();
    auto limit = std::min(m.limit(), m.m_.size());
    auto end_size = limit / 2;
    auto before_size = limit - end_size;
    size_t omitted = m.m_.size() - limit;

    if (!ellipsis)
    {
        before_size = limit;
        end_size = 0;
    }

    auto it = m.m_.begin();
    for (size_t i = 0; i < before_size; ++i)
    {
        os << it->first << ": " << it->second << ", ";
        it++;
    }
    if (ellipsis)
    {
        os << "...";
        if (end_size > 0)
        {
            os << ", ";
        }
    }

    auto rit = m.m_.rbegin();
    for (size_t i = 0; i < end_size; ++i)
    {
        bool last = i + 1 == end_size;
        os << rit->first << ": " << rit->second;
        if (!last)
        {
            os << ", ";
        }
        rit++;
    }
    os << "} (sz: " << m.m_.size() << ", omitted " << omitted << ")";

    return os;
}

template <typename T>
struct pre_set
{
    pre_set(const std::set<T> &s,
            size_t limit = std::numeric_limits<size_t>::max())
        : s_(s), limit_(limit)
    {
    }
    size_t limit() const
    {
        return limit_;
    }
    const std::set<T> &s_;
    size_t limit_;
};
template <typename T>
inline std::ostream &operator<<(std::ostream &os, const pre_set<T> &s)
{
    os << "{";
    bool ellipsis = s.s_.size() > s.limit();
    auto limit = std::min(s.limit(), s.s_.size());
    auto end_size = limit / 2;
    auto before_size = limit - end_size;
    size_t omitted = s.s_.size() - limit;

    if (!ellipsis)
    {
        before_size = limit;
        end_size = 0;
    }

    auto it = s.s_.begin();
    for (size_t i = 0; i < before_size; ++i)
    {
        os << *it << ", ";
        it++;
    }
    if (ellipsis)
    {
        os << "...";
        if (end_size > 0)
        {
            os << ", ";
        }
    }

    auto rit = s.s_.rbegin();
    for (size_t i = 0; i < end_size; ++i)
    {
        bool last = i + 1 == end_size;
        os << *rit;
        if (!last)
        {
            os << ", ";
        }
        rit++;
    }
    os << "} (sz: " << s.s_.size() << ", omitted " << omitted << ")";
    return os;
}

template <typename T>
struct pre_iter
{
    pre_iter(const T &iter, size_t limit = std::numeric_limits<size_t>::max())
        : iter(iter), limit_(limit)
    {
    }
    size_t limit() const
    {
        return limit_;
    }
    const T &iter;
    size_t limit_;
};

template <typename T>
inline std::ostream &operator<<(std::ostream &os, pre_iter<T> iterable)
{
    const auto &iter = iterable.iter;
    bool ellipsis = iter.size() > iterable.limit();
    auto limit = std::min(iterable.limit(), iter.size());
    auto end_size = limit / 2;
    auto before_size = limit - end_size;
    size_t ommitted = iter.size() - limit;

    os << "[";
    auto front_iter = iter.cbegin();
    for (size_t i = 0; i < before_size; ++i)
    {
        os << *front_iter << ", ";
        front_iter++;
    }
    if (ellipsis)
    {
        os << "...";
        if (end_size > 0)
        {
            os << ", ";
        }
    }
    auto back_iter = iter.cend();
    for (size_t i = 0; i < end_size; ++i)
    {
        bool last = i + 1 == end_size;
        if (!last)
        {
            os << *back_iter << ", ";
        }
        else
        {
            os << *back_iter;
        }
        back_iter++;
    }
    os << "] (sz: " << iter.size() << ", ommitted " << ommitted << ")";
    return os;
}

}  // namespace util

#endif