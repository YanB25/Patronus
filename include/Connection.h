#ifndef __CONNECTION_H__
#define __CONNECTION_H__

#include "Common.h"
#include "RawMessageConnection.h"

#include "ThreadConnection.h"
#include "DirectoryConnection.h"

struct RemoteConnection {
    // directory
    uint64_t dsmBase;

    uint32_t dsmRKey[NR_DIRECTORY];
    uint32_t dirMessageQPN[NR_DIRECTORY];
    // ibv_ah *cacheToDirAh[NR_CACHE_AGENT][NR_DIRECTORY];
    ibv_ah *appToDirAh[MAX_APP_THREAD][NR_DIRECTORY];

    // cache agent
    uint64_t cacheBase;

    // uint32_t cacheRKey[NR_CACHE_AGENT];
    // uint32_t cacheMessageQPN[NR_CACHE_AGENT];
    // ibv_ah *dirToCacheAh[NR_DIRECTORY][NR_CACHE_AGENT];

    // app thread
    uint32_t appRKey[MAX_APP_THREAD];
    uint32_t appMessageQPN[MAX_APP_THREAD];
    ibv_ah *dirToAppAh[NR_DIRECTORY][MAX_APP_THREAD];
};

#endif /* __CONNECTION_H__ */
