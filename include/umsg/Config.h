#pragma once
#ifndef SHERMEM_RMSG_CONFIG_H_
#define SHERMEM_RMSG_CONFIG_H_

#include "Common.h"

namespace config::umsg
{
constexpr static size_t kRecvBuffer = MAX_MACHINE * kMaxAppThread * 4;
// post 2 batch in advance. must be 2.
constexpr static size_t kPostRecvBufferAdvanceBatch = 2;
constexpr static size_t kPostRecvBufferBatch =
    kRecvBuffer / kPostRecvBufferAdvanceBatch;
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

namespace sender
{
// already in a per-client-thread, per-machine and per-dir mode
// empirically set, propotional to coroutine_nr * active_req_per_coro
constexpr static size_t kMaxSendWr = 128;
// never post recv
constexpr static size_t kMaxRecvWr = 0;
// one iCon only has one CQ
// each QP (machine_nr * directory_nr) may generate CQEs
// each QP at most generate (wr / batch) CQEs.
constexpr static size_t kMaxCQWr =
    MAX_MACHINE * NR_DIRECTORY * (kMaxSendWr / kSenderBatchSize);
}  // namespace sender

namespace recv
{
// never post send
constexpr static size_t kMaxSendWr = 0;
constexpr static size_t kMaxRecvWr = kRecvBuffer;
constexpr static size_t kMaxCQWr = MAX_MACHINE * kMaxAppThread * (kMaxRecvWr);
constexpr static size_t kRecvMessagePoolSize =
    kMaxAppThread * MAX_MACHINE * kRecvBuffer * kPostMessageSize;
}  // namespace recv

}  // namespace config::umsg
#endif