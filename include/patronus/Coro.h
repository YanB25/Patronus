#ifndef PATRONUS_CORO_H_
#define PATRONUS_CORO_H_

#include "Common.h"
#include "Pool.h"

namespace patronus
{
struct ServerCoroTask
{
    const char *buf{nullptr};  // msg buffer
    size_t msg_nr{0};
    size_t fetched_nr{0};
    size_t finished_nr{0};
    // because we need to call put_rdma_buffer(buf) when all the things get
    // done.
    std::function<void()> call_back_on_finish;
};

struct ServerCoroCommunication
{
    bool finished[define::kMaxCoroNr];
    std::queue<ServerCoroTask *> task_queue;
};

struct ServerCoroContext
{
    CoroCall server_workers[define::kMaxCoroNr];
    CoroCall server_master;
    ServerCoroCommunication comm;
    ThreadUnsafePool<ServerCoroTask, define::kMaxCoroNr * MAX_MACHINE>
        task_pool;
};

};  // namespace patronus
#endif