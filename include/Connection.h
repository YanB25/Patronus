#ifndef __CONNECTION_H__
#define __CONNECTION_H__

#include "Common.h"
#include "DirectoryConnection.h"
#include "RawMessageConnection.h"
#include "ThreadConnection.h"
#include <glog/logging.h>

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
    ibv_ah *appToDirAh[MAX_APP_THREAD][NR_DIRECTORY];

    // cache
    uint64_t cacheBase;

    // device memory
    uint64_t dmBase;
    uint32_t dmRKey[NR_DIRECTORY];

    // app thread
    uint32_t appRKey[MAX_APP_THREAD];
    uint32_t appMessageQPN[MAX_APP_THREAD];
    ibv_ah *dirToAppAh[NR_DIRECTORY][MAX_APP_THREAD];
    void destroy()
    {
        for (size_t i = 0; i < NR_DIRECTORY; ++i)
        {
            for (size_t j = 0; j < MAX_APP_THREAD; ++j)
            {
                if (ibv_destroy_ah(dirToAppAh[i][j]))
                {
                    PLOG(ERROR) << "failed to destroy ah";
                }
                if (ibv_destroy_ah(appToDirAh[j][i]))
                {
                    PLOG(ERROR) << "failed to destroy ah";
                }
            }
        }
    }
};

#endif /* __CONNECTION_H__ */
