#pragma once
#ifndef UTIL_DATAFRAME_F_H_
#define UTIL_DATAFRAME_F_H_

#include "DataFrame/DataFrame.h"

namespace hmdf
{
using StrDataFrame = StdDataFrame<std::string>;
/**
 * @brief generate the F function used by hmdf::DataFrame::consolidate
 * Should pass in a function (@f) that (A, B) -> R
 * to reduce columns std::vector<A> and std::vector<B> to std::vector<R>
 *
 * @tparam A lhs vector data type
 * @tparam B rhs vector data type
 * @tparam R result vector data type
 * @param f the reducer
 * @return auto
 */
template <typename A, typename B, typename R>
auto gen_F(std::function<R(A, B)> f)
{
    return
        [f]([[maybe_unused]] StrDataFrame::IndexVecType::const_iterator
                idx_begin,
            [[maybe_unused]] StrDataFrame::IndexVecType::const_iterator idx_end,
            typename std::vector<A>::const_iterator lhs_b,
            typename std::vector<A>::const_iterator lhs_e,
            typename std::vector<B>::const_iterator rhs_b,
            typename std::vector<B>::const_iterator rhs_e) -> std::vector<R> {
            const std::size_t col_s = std::min(
                {std::distance(lhs_b, lhs_e), std::distance(rhs_b, rhs_e)});
            std::vector<R> result(col_s);
            for (size_t i = 0; i < col_s; ++i)
            {
                result[i] = f(*(lhs_b + i), *(rhs_b + i));
            }
            return result;
        };
}

template <typename A, typename B, typename R>
auto gen_F_div()
{
    return gen_F<A, B, R>([](A a, B b) {
        if (unlikely(b == 0))
        {
            LOG(WARNING) << "[dataframe] get invalid data: divide by zero.";
            return (R) 0;
        }
        return (R)((R) a / b);
    });
}
template <typename A, typename B, typename R>
auto gen_F_mul()
{
    return gen_F<A, B, R>([](A a, B b) { return (R)(a * b); });
}
template <typename A, typename B, typename R>
auto gen_F_plus()
{
    return gen_F<A, B, R>([](A a, B b) { return (R)(a + b); });
}
template <typename A, typename B, typename R>
auto gen_F_sub()
{
    return gen_F<A, B, R>([](A a, B b) { return (R)(a + b); });
}

// A is the type of operation_nr
// B is the type of nanoseconds
template <typename A, typename B, typename R>
auto gen_F_ops()
{
    return gen_F<A, B, R>([](A a, B b) { return (R)(1e9 * a / b); });
}

template <typename T>
auto gen_replace_F_mul(const T &t)
{
    return [t](const std::string &, T &val) {
        val *= t;
        return true;
    };
}
template <typename T>
auto gen_replace_F_div(const T &t)
{
    return [t](const std::string &, T &val) {
        val /= t;
        return true;
    };
}
template <typename T>
auto gen_replace_F_lambda(const std::function<void(T &)> &lambda)
{
    return [lambda](const std::string &, T &val) {
        lambda(val);
        return true;
    };
}

}  // namespace hmdf

#endif