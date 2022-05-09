#pragma once
#ifndef PATRONUS_CONFIG_H_
#define PATRONUS_CONFIG_H_

#include "Common.h"
#include "umsg/Config.h"
#include "umsg/UnreliableConnection.h"

namespace config::patronus
{
constexpr static size_t kMaxCoroNr = define::kMaxCoroNr;
constexpr static size_t kMessageSize = config::umsg::kUserMessageSize;
constexpr static size_t kClientRdmaBufferSize = 8 * define::KB;
constexpr static size_t kHandleRequestBatchLimit = 32;

constexpr static size_t kMaxResponseBatchSize = 8;
}  // namespace config::patronus

#endif
