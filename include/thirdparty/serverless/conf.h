#pragma once
#ifndef THIRD_PARTY_SERVERLESS_CONFIG_H_
#define THIRD_PARTY_SERVERLESS_CONFIG_H_

#include <iostream>

#include "./config.h"
#include "util/IRdmaAdaptor.h"

namespace serverless
{
struct Config
{
    std::string name;
    bool use_step_lambda;
    MemHandleDecision alloc;
    MemHandleDecision param;

    static Config get_basic(const std::string &name, bool use_step_lambda)
    {
        Config conf;
        conf.name = name;
        conf.use_step_lambda = use_step_lambda;
        return conf;
    }
    static Config get_mw(const std::string &name, bool use_step_lambda)
    {
        auto conf = get_basic(name, use_step_lambda);
        conf.alloc.use_mw()
            .wo_expire()
            .with_alloc(::config::serverless::kAllocHint)
            .no_bind_pr();
        conf.param.use_mw().wo_expire().no_bind_pr();
        return conf;
    }
};

inline std::ostream &operator<<(std::ostream &os, const Config &c)
{
    os << "{Config name: " << c.name << ", use_step: " << c.use_step_lambda
       << ", alloc: " << c.alloc << ", param: " << c.param << "}";
    return os;
}
}  // namespace serverless

#endif