#pragma once
#ifndef SHERMEM_ERRCODE_H_
#define SHERMEM_ERRCODE_H_

#include <iostream>

enum RetCode
{
    kOk,
    kNotFound,
    kNoMem,
    kRetry,  // failed by multithread conflict.
    kCacheStale,
    kInvalid,
    kRdmaProtectionErr,
    kRdmaExecutionErr,
    kLeaseLocalExpiredErr,
    kMockCrashed,
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
    case kRdmaProtectionErr:
        os << "kRdmaProtectionErr";
        break;
    case kRdmaExecutionErr:
        os << "kRdmaExecutionErr";
        break;
    case kLeaseLocalExpiredErr:
        os << "kLeaseLocalExpiredErr";
        break;
    case kMockCrashed:
        os << "kMockCrashed";
        break;
    default:
        LOG(FATAL) << "Unknown return code " << (int) rc;
    }
    return os;
}

#endif