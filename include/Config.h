#pragma once
#ifndef __CONFIG_H__
#define __CONFIG_H__

#include "Common.h"

constexpr static size_t kCacheSize = 1 * define::GB;
constexpr static size_t kDSMCacheSize = 16 * define::GB;
constexpr static size_t kDefaultMachineNr = 2;

class CacheConfig
{
public:
    uint64_t cacheSize;

    CacheConfig(uint64_t cacheSize = kCacheSize) : cacheSize(cacheSize)
    {
    }
};
class DSMConfig
{
public:
    DSMConfig(const CacheConfig &cacheConfig = CacheConfig(),
              uint32_t machineNR = kDefaultMachineNr,
              uint64_t dsmSize = kDSMCacheSize,
              size_t dsmReserveSize = 0)
        : cacheConfig(cacheConfig),
          machineNR(machineNR),
          dsmSize(dsmSize),
          dsmReserveSize(dsmReserveSize)
    {
    }

    CacheConfig cacheConfig;
    uint32_t machineNR;
    uint64_t dsmSize;
    size_t dsmReserveSize;  // how much size dsm should reserve for its user
};

#endif /* __CONFIG_H__ */
