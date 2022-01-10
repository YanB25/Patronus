#pragma once
#ifndef UTIL_H_
#define UTIL_H_
#include <string>

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
#endif