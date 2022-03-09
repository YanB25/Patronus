#pragma once
#ifndef __CONNECTION_H__
#define __CONNECTION_H__

#include <glog/logging.h>

#include "Common.h"
#include "DirectoryConnection.h"
#include "RawMessageConnection.h"
#include "ThreadConnection.h"

/**
 * @brief RemoteConnection is a combination of @see ThreadConnection
 * and @see DirectoryConnection
 */
struct RemoteConnection
{
    // directory
    uint64_t dsmBase;

    // [NR_DIRECTORY]
    uint32_t dsmRKey[NR_DIRECTORY];
    uint32_t dirMessageQPN[NR_DIRECTORY];
    ibv_ah *appToDirAh[kMaxAppThread][NR_DIRECTORY];

    // cache
    uint64_t cacheBase;

    // device memory
    uint64_t dmBase;
    uint32_t dmRKey[NR_DIRECTORY];

    // app thread
    uint32_t appRKey[kMaxAppThread];
    uint32_t appMessageQPN[kMaxAppThread];
    ibv_ah *dirToAppAh[NR_DIRECTORY][kMaxAppThread];
    void destroy()
    {
        for (size_t i = 0; i < NR_DIRECTORY; ++i)
        {
            for (size_t j = 0; j < kMaxAppThread; ++j)
            {
                if (dirToAppAh[i][j])
                {
                    PLOG_IF(ERROR, ibv_destroy_ah(dirToAppAh[i][j]))
                        << "failed to destroy ah";
                    dirToAppAh[i][j] = nullptr;
                }
                if (appToDirAh[j][i])
                {
                    PLOG_IF(ERROR, ibv_destroy_ah(appToDirAh[j][i]))
                        << "failed to destroy ah";
                    appToDirAh[j][i] = nullptr;
                }
            }
        }
    }
};

#endif /* __CONNECTION_H__ */
