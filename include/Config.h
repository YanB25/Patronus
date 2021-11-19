#ifndef __CONFIG_H__
#define __CONFIG_H__

#include "Common.h"

class CacheConfig
{
public:
    uint64_t cacheSize;

    CacheConfig(uint64_t cacheSize = 1 * define::GB) : cacheSize(cacheSize)
    {
    }
};

class ConfigRegister
{
public:
    ConfigRegister() = default;
    bool operator=(const ConfigRegister&) = delete;
    ConfigRegister(const ConfigRegister&) = delete;
    static ConfigRegister& ins()
    {
        static ConfigRegister ins;
        return ins;
    }
    void reg_conf_file(const std::string& conf)
    {
        conf_ = conf;
    }
    std::string conf_file()
    {
        return conf_;
    }
private:
    std::string conf_;
};
class DSMConfig
{
public:
    DSMConfig(const CacheConfig &cacheConfig = CacheConfig(),
              uint32_t machineNR = 2,
              uint64_t dsmSize = 16 * define::GB)
        : cacheConfig(cacheConfig), machineNR(machineNR), dsmSize(dsmSize)
    {
    }

    CacheConfig cacheConfig;
    uint32_t machineNR;
    uint64_t dsmSize;
};

#endif /* __CONFIG_H__ */
