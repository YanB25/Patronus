#pragma once
#ifndef PATRONUS_RACEHASHING_CONF_H_
#define PATRONUS_RACEHASHING_CONF_H_

#include "Common.h"
using namespace define::literals;

namespace patronus::hash::config
{
constexpr static bool kEnableDebug = true;
constexpr static bool kEnableMemoryDebug = true;
constexpr static bool kEnableLocateDebug = true;
constexpr static bool kEnableExpandDebug = true;

constexpr static bool kMonitorRdma = false;

constexpr static uint64_t kAllocHintDefault = 0;
constexpr static uint64_t kAllocHintKVBlock = 1;
constexpr static uint64_t kAllocHintDirSubtable = 0;  // subtable uses default

constexpr static size_t kKVBlockAllocBatchSize = 16_MB;
constexpr static size_t kKVBlockExpectSize = 64;
}  // namespace patronus::hash::config

#endif