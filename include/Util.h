#pragma once
#ifndef UTIL_H_
#define UTIL_H_
#include <string>
#include <vector>

#include "Cache.h"
#include "Common.h"

namespace smart
{
inline std::string smartSize(uint64_t size)
{
    if (size < 1 * define::KB)
    {
        return std::to_string(size) + " B";
    }
    else if (size < 1 * define::MB)
    {
        return std::to_string(size / define::KB) + " KB";
    }
    else if (size < 1 * define::GB)
    {
        return std::to_string(size / define::MB) + " MB";
    }
    else if (size < 1 * define::TB)
    {
        return std::to_string(size / define::GB) + " GB";
    }
    else
    {
        return std::to_string(size / define::TB) + " TB";
    }
}
inline std::string smartOps(uint64_t ops)
{
    if (ops < 1 * define::K)
    {
        return std::to_string(ops) + " ops";
    }
    else if (ops < 1 * define::M)
    {
        return std::to_string(ops / define::K) + " Kops";
    }
    else if (ops < 1 * define::G)
    {
        return std::to_string(ops / define::M) + " Mops";
    }
    else if (ops < 1 * define::T)
    {
        return std::to_string(ops / define::G) + " Gops";
    }
    else
    {
        return std::to_string(ops / define::T) + " Tops";
    }
}
}  // namespace smart

inline void validate_buffer_not_overlapped(Buffer lhs, Buffer rhs)
{
    // https://stackoverflow.com/questions/325933/determine-whether-two-date-ranges-overlap
    auto start_1 = (uint64_t) lhs.buffer;
    auto end_1 = (uint64_t) start_1 + lhs.size;
    auto start_2 = (uint64_t) rhs.buffer;
    auto end_2 = (uint64_t) start_2 + rhs.size;
    // exclusive
    bool overlap = (start_1 < end_2) && (start_2 < end_1);
    CHECK(!overlap) << "Buffer_1 [" << (void *) start_1 << ", "
                    << (void *) end_1 << ") v.s. buffer_2 [" << (void *) start_2
                    << ", " << end_2 << "). Overlapped.";
}
inline void validate_buffer_not_overlapped(const std::vector<Buffer> &buffers)
{
    for (size_t i = 0; i < buffers.size(); ++i)
    {
        for (size_t j = i + 1; j < buffers.size(); ++j)
        {
            validate_buffer_not_overlapped(buffers[i], buffers[j]);
        }
    }
}
#endif