#pragma once
#if !defined(_LOCAL_LOCK_QUEUE_H_)
#define _LOCAL_LOCK_QUEUE_H_

#include "Common.h"
#include "WRLock.h"

class LocalLockQueue
{
    const static int kMaxQueueSize = 512;
    static_assert(kMaxQueueSize > kMaxAppThread * define::kMaxCoroNr, "XX");

public:
private:
};

#endif  // _LOCAL_LOCK_QUEUE_H_
