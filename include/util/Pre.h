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
    const std::map<K, V> &m_;
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
    auto back_iter = iter.rbegin();
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

template <typename K, typename V>
struct pre_umap
{
    pre_umap(const std::unordered_map<K, V> &m,
             size_t limit = std::numeric_limits<size_t>::max())
        : m_(m), limit_(limit)
    {
    }
    size_t limit() const
    {
        return limit_;
    }
    const std::unordered_map<K, V> &m_;
    size_t limit_;
};

template <typename K, typename V>
inline std::ostream &operator<<(std::ostream &os, const pre_umap<K, V> &m)
{
    os << "{";
    bool ellipsis = m.m_.size() > m.limit();
    auto limit = std::min(m.limit(), m.m_.size());
    auto before_size = limit;
    size_t omitted = m.m_.size() - limit;

    auto it = m.m_.begin();
    for (size_t i = 0; i < before_size; ++i)
    {
        bool last = i + 1 == before_size;
        os << it->first << ": " << it->second;
        if (!last)
        {
            os << ", ";
        }
        it++;
    }
    if (ellipsis)
    {
        os << ", ...";
    }

    os << "} (sz: " << m.m_.size() << ", omitted " << omitted << ")";

    return os;
}

namespace pre
{
template <typename K, typename V>
inline std::ostream &operator<<(std::ostream &os,
                                const std::unordered_map<K, V> &map)
{
    os << util::pre_umap(map);
    return os;
}
template <typename K, typename V>
inline std::ostream &operator<<(std::ostream &os, const std::map<K, V> &map)
{
    os << util::pre_map(map);
    return os;
}
template <typename T>
inline std::ostream &operator<<(std::ostream &os, const std::vector<T> &vec)
{
    os << util::pre_iter(vec);
    return os;
}
template <typename U, typename V>
inline std::ostream &operator<<(std::ostream &os, const std::pair<U, V> &p)
{
    os << util::pre_pair(p);
    return os;
}

}  // namespace pre

class pre_ns
{
public:
    pre_ns(uint64_t ns) : ns_(ns)
    {
    }

    uint64_t ns_;
};
inline std::ostream &operator<<(std::ostream &os, pre_ns pns)
{
    if (pns.ns_ < 1_K)
    {
        os << pns.ns_ << " ns";
    }
    else if (pns.ns_ < 1_M)
    {
        os << 1.0 * pns.ns_ / 1_K << " us";
    }
    else if (pns.ns_ < 1_G)
    {
        os << 1.0 * pns.ns_ / 1_M << " ms";
    }
    else
    {
        os << 1.0 * pns.ns_ / 1_G << " s";
    }
    return os;
}

class pre_op
{
public:
    pre_op(uint64_t op) : op_(op)
    {
    }
    uint64_t op_;
};

inline std::ostream &operator<<(std::ostream &os, pre_op p)
{
    uint64_t op = p.op_;
    if (op < 1_K)
    {
        os << op;
    }
    else if (op < 1_M)
    {
        os << op / 1_K << " K";
    }
    else if (op < 1_G)
    {
        os << op / 1_M << " M";
    }
    else
    {
        os << op / 1_G << " G";
    }
    return os;
}

class pre_ops
{
public:
    pre_ops(uint64_t op, uint64_t ns, bool verbose = false)
        : op_(op), ns_(ns), verbose_(verbose)
    {
    }
    uint64_t op_;
    uint64_t ns_;
    bool verbose_;
};

inline std::ostream &operator<<(std::ostream &os, pre_ops p)
{
    double ops = 1e9 * p.op_ / p.ns_;
    double lat_ns = 1.0 * p.ns_ / p.op_;
    if (ops < 1_K)
    {
        os << ops << " ops";
    }
    else if (ops < 1_M)
    {
        os << ops / 1_K << " Kops";
    }
    else if (ops < 1_G)
    {
        os << ops / 1_M << " Mops";
    }
    else
    {
        os << ops / 1_G << " Gops";
    }
    if (p.verbose_)
    {
        os << "[" << p.op_ << " in " << pre_ns(p.ns_) << " (" << pre_ns(lat_ns)
           << "/op)]";
    }
    return os;
}

class pre_byte
{
public:
    pre_byte(uint64_t byte) : byte_(byte)
    {
    }
    uint64_t byte_;
};
inline std::ostream &operator<<(std::ostream &os, pre_byte b)
{
    if (b.byte_ < 1_KB)
    {
        os << b.byte_ << " B";
    }
    else if (b.byte_ < 1_MB)
    {
        os << 1.0 * b.byte_ / 1_KB << " KB";
    }
    else if (b.byte_ < 1_GB)
    {
        os << 1.0 * b.byte_ / 1_MB << " MB";
    }
    else if (b.byte_ < 1_TB)
    {
        os << 1.0 * b.byte_ / 1_GB << " GB";
    }
    else
    {
        os << 1.0 * b.byte_ / 1_TB << " TB";
    }
    return os;
}

}  // namespace util

#endif