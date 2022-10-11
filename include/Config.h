#pragma once
#ifndef __CONFIG_H__
#define __CONFIG_H__

#include "Common.h"
#include "util/Literals.h"

using namespace util::literals;

class CacheConfig
{
public:
    uint64_t cacheSize;

    CacheConfig(uint64_t cacheSize = ::config::kDefaultCacheSize)
        : cacheSize(cacheSize)
    {
    }
};
class DSMConfig
{
public:
    DSMConfig(const CacheConfig &cacheConfig = CacheConfig(),
              uint32_t machineNR = ::config::kMachineNr,
              uint64_t dsmSize = ::config::kDefaultDSMSize,
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
