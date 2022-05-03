#pragma once
#ifndef UNRELIABLE_RECEIVER_H_
#define UNRELIABLE_RECEIVER_H_

namespace config::umsg
{
constexpr static size_t kRecvBuffer = 1024;
// post 2 batch in advance. must be 2.
constexpr static size_t kPostRecvBufferAdvanceBatch = 2;
constexpr static size_t kPostRecvBufferBatch =
    kRecvBuffer / kPostRecvBufferAdvanceBatch;
constexpr static size_t kApplicationMessageSize = 64;
constexpr static size_t kMessageSize = kApplicationMessageSize + 40;

constexpr static size_t kSenderBatchSize = 32;
constexpr static size_t kRecvLimit = kPostRecvBufferBatch;
constexpr static size_t kMaxRecvBuffer = kMessageSize * kRecvLimit;
}  // namespace config::umsg

#endif