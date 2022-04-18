#pragma once
#ifndef PATRONUS_CONFIG_H_
#define PATRONUS_CONFIG_H_

#include "Common.h"
#include "ReliableMessageConnection.h"

namespace config::patronus
{
constexpr static size_t kMaxCoroNr = define::kMaxCoroNr;
constexpr static size_t kMessageSize = ReliableConnection::kMessageSize;
constexpr static size_t kClientRdmaBufferSize = 8 * define::KB;
}  // namespace config::patronus

#endif
