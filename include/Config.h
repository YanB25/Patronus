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
              uint64_t dsmSize = kDSMCacheSize)
        : cacheConfig(cacheConfig), machineNR(machineNR), dsmSize(dsmSize)
    {
    }

    CacheConfig cacheConfig;
    uint32_t machineNR;
    uint64_t dsmSize;
};

#endif /* __CONFIG_H__ */
