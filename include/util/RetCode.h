#pragma once
#ifndef SHERMEM_ERRCODE_H_
#define SHERMEM_ERRCODE_H_

#include <iostream>

#include "glog/logging.h"

enum RC
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
    kReserved,
};
inline std::ostream &operator<<(std::ostream &os, RC rc)
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
    case kReserved:
        os << "kReserved";
        break;
    default:
        LOG(FATAL) << "Unknown return code " << (int) rc;
    }
    return os;
}

class RetCode
{
public:
    RetCode() : rc_(RC::kReserved)
    {
    }
    RetCode(RC rc) : rc_(rc)
    {
    }
    bool operator==(RC rc) const
    {
        return rc_ == rc;
    }
    bool operator!=(RC rc) const
    {
        return rc_ != rc;
    }
    void expect(RC expect_rc)
    {
        CHECK_EQ(rc_, expect_rc);
    }

    friend std::ostream &operator<<(std::ostream &os, const RetCode &rc);

private:
    RC rc_;
};

inline std::ostream &operator<<(std::ostream &os, const RetCode &rc)
{
    os << rc.rc_;
    return os;
}

#endif