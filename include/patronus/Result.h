#pragma once
#ifndef PATRONUS_RESULT_H
#define PATRONUS_RESULT_H

#include <glog/logging.h>

#include <atomic>

#include "Common.h"

namespace patronus
{
// class Patronus;
// struct Void
// {
// };

// enum class ResultType
// {
//     kError,
//     kOK,
// };

// template <typename R, typename E>
// class Result
// {
// public:
//     Result(const R &r, CoroContext *ctx = nullptr) : result_(r),
//     coro_ctx_(ctx)
//     {
//     }
//     Result(const E &e, CoroContext *ctx = nullptr) : error_(e),
//     coro_ctx_(ctx)
//     {
//     }

//     bool ready() const
//     {
//         return ready_.load(std::memory_order_relaxed);
//     }
//     bool is_err() const
//     {
//         wait_for_ready();
//         return result_type_ == ResultType::kError;
//     }
//     bool is_ok() const
//     {
//         wait_for_ready();
//         return result_type_ == ResultType::kOK;
//     }
//     const R &unwrap() const
//     {
//         wait_for_ready();
//         CHECK_EQ(result_type_, ResultType::kOK);
//         return result_;
//     }
//     R &unwarp()
//     {
//         wait_for_ready();
//         CHECK_EQ(result_type_, ResultType::kOK);
//         return result_;
//     }
//     const R &unwrap_or(const R &otherwise) const
//     {
//         wait_for_ready();
//         if (result_type_ == ResultType::kOK)
//         {
//             return result_;
//         }
//         return otherwise;
//     }

// private:
//     friend class patronus::Patronus;

//     void set_result(const R &r)
//     {
//         DCHECK(!ready_);
//         result_ = r;
//         result_type_ = ResultType::kOK;
//         bool expect = false;
//         CHECK(ready_.compare_exchange_strong(
//             expect, true, std::memory_order_release))
//             << "** concurrent set_result detected.";
//     }
//     void set_error(const E &e)
//     {
//         DCHECK(!ready_);
//         error_ = e;
//         result_type_ = ResultType::kError;
//         bool expect = false;
//         CHECK(ready_.compare_exchange_strong(
//             expect, true, std::memory_order_release))
//             << "** concurrent set_result detected.";
//     }
//     void wait_for_ready() const
//     {
//         if (likely(ready_.load(std::memory_order_relaxed)))
//         {
//             return;
//         }
//         while (unlikely(!ready_.load(std::memory_order_relaxed)))
//         {
//             LOG(WARNING) << "TODO: should yield here, if possible";
//         }
//     }
//     std::atomic<bool> ready_{false};
//     std::atomic<ResultType> result_type_;
//     R result_;
//     E error_;
//     CoroContext *coro_ctx_{nullptr};
// };

}  // namespace patronus

#endif