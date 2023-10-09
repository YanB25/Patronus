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
    bool bypass_prot{false};
    bool use_rpc{false};

    double scale_factor{1.0};

    static Config get_basic(const std::string &name, bool use_step_lambda)
    {
        Config conf;
        conf.name = name;
        conf.use_step_lambda = use_step_lambda;
        return conf;
    }
    static Config get_unprot(const std::string &name, bool use_step_lambda)
    {
        auto conf = get_basic(name, use_step_lambda);
        conf.alloc.only_alloc(::config::serverless::kAllocHint);
        conf.param.no_rpc();
        conf.bypass_prot = true;
        return conf;
    }
    static Config get_rpc(const std::string &name, bool use_step_lambda)
    {
        auto conf = get_basic(name, use_step_lambda);
        conf.alloc.only_alloc(::config::serverless::kAllocHint);
        conf.param.no_rpc();
        conf.use_rpc = true;
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
    static Config get_mr(const std::string &name, bool use_step_lambda)
    {
        auto conf = get_basic(name, use_step_lambda);
        conf.alloc.use_mr()
            .wo_expire()
            .with_alloc(::config::serverless::kAllocHint)
            .no_bind_pr();
        conf.param.use_mr().wo_expire().no_bind_pr();
        conf.scale_factor = 0.1;
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