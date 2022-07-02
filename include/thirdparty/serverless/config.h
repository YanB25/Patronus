#pragma once
#ifndef THIRD_PARTY_SERVERLESS_CONFIG_H_
#define THIRD_PARTY_SERVERLESS_CONFIG_H_

#include <iostream>

#include "util/IRdmaAdaptor.h"

namespace serverless
{
struct Config
{
    bool use_step_lambda;
    MemHandleDecision alloc;
    MemHandleDecision param;
};
inline std::ostream &operator<<(std::ostream &os, const Config &c)
{
    os << "{Config use_step: " << c.use_step_lambda << ", alloc: " << c.alloc
       << ", param: " << c.param << "}";
    return os;
}
}  // namespace serverless

#endif