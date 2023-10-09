#pragma once
#ifndef UTIL_CORO_H_
#define UTIL_CORO_H_

#include <boost/coroutine/all.hpp>
#include <cinttypes>

using CoroYield =
    typename boost::coroutines::symmetric_coroutine<void>::yield_type;
using CoroCall =
    typename boost::coroutines::symmetric_coroutine<void>::call_type;

using coro_t = uint8_t;
static constexpr coro_t kMasterCoro = coro_t(-1);
static constexpr coro_t kNotACoro = coro_t(-2);

#endif
