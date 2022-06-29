#pragma once
#ifndef THIRD_PARTY_SERVERLESS_SERVERLESS_H_
#define THIRD_PARTY_SERVERLESS_SERVERLESS_H_

#include <cinttypes>
#include <functional>
#include <optional>

#include "Common.h"
#include "util/RetCode.h"

namespace serverless
{
class ServerlessManager
{
    constexpr static size_t kMaxLambdaNr = define::kMaxCoroNr;

public:
    using lambda_t = uint64_t;
    using Lambda = std::function<RetCode()>;
    lambda_t add_lambda(const Lambda &lambda,
                        std::optional<lambda_t> dependent_lambda)
    {
        auto ret = lambda_nr_++;
        if (dependent_lambda.has_value())
        {
            CHECK_LT(dependent_lambda.value(), lambda_nr_)
                << "** invalid dependent lambda";
        }
    }

private:
    lambda_t lambda_nr_{0};
    std::array<std::optional<lambda_t>, kMaxLambdaNr> depend_on_;
};
}  // namespace serverless

#endif