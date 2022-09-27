#pragma once
#ifndef PATRONUS_CONFIG_H_
#define PATRONUS_CONFIG_H_

#include "Common.h"
#include "umsg/Config.h"
#include "umsg/UnreliableConnection.h"

namespace config::patronus
{
constexpr static size_t kMaxCoroNr = define::kMaxCoroNr;
constexpr static size_t kMessageSize = ::config::umsg::kUserMessageSize;
constexpr static size_t kSmallMessageSize = 64_B;
constexpr static size_t kClientRdmaBufferSize = 8_KB;
constexpr static size_t kHandleRequestBatchLimit = 32;
constexpr static bool kEnableMWLocality = false;

constexpr static size_t kClientThreadPerServerThread =
    kMaxAppThread / NR_DIRECTORY;
}  // namespace config::patronus

#endif
