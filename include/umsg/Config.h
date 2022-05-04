#pragma once
#ifndef SHERMEM_RMSG_CONFIG_H_
#define SHERMEM_RMSG_CONFIG_H_

#include "Common.h"

namespace config::umsg
{
// constexpr static size_t kExpectInFlightMessageNr = 128;  // max
constexpr static size_t kExpectInFlightMessageNr = 64;
constexpr static size_t kPostRecvBufferBatch =
    MAX_MACHINE * kMaxAppThread * kExpectInFlightMessageNr;
constexpr static size_t kPostRecvBufferAdvanceBatch = 2;
constexpr static size_t kRecvBuffer =
    kPostRecvBufferBatch * kPostRecvBufferAdvanceBatch;
static_assert(kRecvBuffer <= 32768,
              "In this device, the max WR for one QP is 32768. If you are not "
              "sure, please refer to the actual manual");

// better be cahceline alinged. e.g. multiple of 64
constexpr static size_t kUserMessageSize = 64;
constexpr static size_t kPostMessageSize = kUserMessageSize + 40;
/**
 * @brief how much send # before a signal
 */
constexpr static size_t kSenderBatchSize = 16;
// so that, server can get maximum messages by ONE poll.
constexpr static size_t kRecvLimit = kPostRecvBufferBatch;
constexpr static size_t kMaxRecvBuffer = kPostMessageSize * kRecvLimit;

constexpr static size_t kMaxInlinedSize = 32;

namespace sender
{
// already in a per-client-thread, per-machine and per-dir mode
// empirically set, propotional to coroutine_nr * active_req_per_coro
constexpr static size_t kMaxSendWr = 128;
// one iCon only has one CQ
// each QP (machine_nr * directory_nr) may generate CQEs
// each QP at most generate (wr / batch) CQEs.
constexpr static size_t kMaxCQE =
    MAX_MACHINE * NR_DIRECTORY * (kMaxSendWr / kSenderBatchSize);
}  // namespace sender

namespace recv
{
constexpr static size_t kMaxRecvWr = kRecvBuffer;
constexpr static size_t kMaxCQE = MAX_MACHINE * kMaxAppThread * (kMaxRecvWr) -1;
static_assert(kMaxCQE <= 4194303, "In this device, the max CQE is 4194303");
constexpr static size_t kRecvMessagePoolSize =
    kMaxAppThread * MAX_MACHINE * kRecvBuffer * kPostMessageSize;
}  // namespace recv

}  // namespace config::umsg
#endif