#pragma once
#ifndef PATRONUS_RACEHASHING_CONF_H_
#define PATRONUS_RACEHASHING_CONF_H_

#include "Common.h"
using namespace util::literals;

namespace patronus::hash::config
{
constexpr static bool kEnableDebug = false;
constexpr static bool kEnableMemoryDebug = false;
constexpr static bool kEnableLocateDebug = false;
constexpr static bool kEnableExpandDebug = false;

constexpr static bool kMonitorRdma = false;

constexpr static uint64_t kAllocHintDefault = 0;
constexpr static uint64_t kAllocHintKVBlock = 1;
constexpr static uint64_t kAllocHintKVBlockOverMR = 2;
constexpr static uint64_t kAllocHintDirSubtable = 0;  // subtable uses default
constexpr static uint64_t kAllocHintDirSubtableOverMR = 3;  // the mr version

constexpr static size_t kKVBlockAllocBatchSize = 2_MB;
}  // namespace patronus::hash::config

#endif