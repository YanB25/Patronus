#pragma once
#ifndef PATRONUS_RACEHASHING_CONF_H_
#define PATRONUS_RACEHASHING_CONF_H_

#include "Common.h"
using namespace define::literals;

namespace patronus::hash::config
{
constexpr static bool kEnableDebug = false;
constexpr static bool kEnableMemoryDebug = false;
constexpr static bool kEnableLocateDebug = false;
constexpr static bool kEnableExpandDebug = false;

constexpr static bool kMonitorRdma = true;

constexpr static uint64_t kAllocHintDefault = 0;
constexpr static uint64_t kAllocHintKVBlock = 1;
constexpr static uint64_t kAllocHintSubtable = 2;

constexpr static size_t kKVBlockAllocBatchSize = 16_MB;
constexpr static size_t kKVBlockExpectSize = 64;
}  // namespace patronus::hash::config

#endif