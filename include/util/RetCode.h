#pragma once
#ifndef SHERMEM_ERRCODE_H_
#define SHERMEM_ERRCODE_H_

#include <iostream>

enum RetCode
{
    kOk,
    kNotFound,
    kNoMem,
    // failed by multithread conflict.
    kRetry,
    kCacheStale,
    kInvalid,
    kRdmaErr
};
inline std::ostream &operator<<(std::ostream &os, RetCode rc)
{
    switch (rc)
    {
    case kOk:
        os << "kOk";
        break;
    case kNotFound:
        os << "kNotFound";
        break;
    case kNoMem:
        os << "kNoMem";
        break;
    case kRetry:
        os << "kRetry";
        break;
    case kCacheStale:
        os << "kCacheStale";
        break;
    case kInvalid:
        os << "kInvalid";
        break;
    case kRdmaErr:
        os << "kRdmaErr";
        break;
    default:
        LOG(FATAL) << "Unknown return code " << (int) rc;
    }
    return os;
}

#endif