#pragma once
#ifndef PATRONUS_DDL_MANAGER_H_
#define PATRONUS_DDL_MANAGER_H_

#include <algorithm>
#include <functional>
#include <queue>
#include <vector>

#include "Common.h"
#include "CoroContext.h"
#include "patronus/Type.h"

namespace patronus
{
// yes, you have to give me CoroContext
using Task = std::function<void(CoroContext *)>;
class DDLTask
{
public:
    DDLTask(time::term_t ddl, const Task &task) : ddl_(ddl), task_(task)
    {
    }
    bool operator<(const DDLTask &rhs) const
    {
        return ddl_ > rhs.ddl_;
    }
    time::term_t ddl() const
    {
        return ddl_;
    }
    Task &task()
    {
        return task_;
    }
    const Task &task() const
    {
        return task_;
    }

private:
    time::term_t ddl_;
    Task task_;
};
class DDLManager
{
public:
    constexpr static size_t kLimit = std::numeric_limits<size_t>::max();
    using value_type = DDLTask;
    DDLManager() = default;
    DDLManager(const DDLManager &) = delete;
    DDLManager &operator=(const DDLManager &) = delete;

    void push(time::term_t ddl, const Task &task)
    {
        pqueue_.push(DDLTask(ddl, task));
    }
    void push(const std::chrono::time_point<std::chrono::steady_clock> &tp,
              const Task &task)
    {
        auto ddl = to_ddl(tp);
        push(ddl, task);
    }
    size_t do_task(const std::chrono::time_point<std::chrono::steady_clock> &tp,
                   CoroContext *ctx,
                   size_t limit = kLimit)
    {
        auto ddl = to_ddl(tp);
        return do_task(ddl, ctx, limit);
    }
    size_t do_task(time::term_t until, CoroContext *ctx, size_t limit = kLimit)
    {
        size_t done = 0;
        while (!pqueue_.empty())
        {
            auto &front = pqueue_.top();
            if (unlikely(front.ddl() > until))
            {
                return done;
            }
            DVLOG(::config::verbose::kVerbose)
                << "[DDLManager] protronus_now: " << time::PatronusTime(until)
                << ", ns_diff: " << until - front.ddl();
            // NOTE: we have to do the copy
            // because the following pop will invalidate the reference
            auto task = front.task();
            /**
             * NOTE: pop from the queue BEFORE executing the task,
             * to resolve the following RC
             * Coroutine 1:
             * - do_task(ddl, ctx, 1)
             *   - task(ctx)
             *     - task_gc_lease => ctx.yield_to_master()
             *
             * Coroutine master:
             * yield to Coroutine 2
             *
             * Coroutine 2:
             * - ** do_task(ddl, ctx, 1)
             * ** re-enter the same task_gc_lease!
             */

            pqueue_.pop();
            task(ctx);
            done++;
            if (unlikely(done >= limit))
            {
                return done;
            }
        }
        return done;
    }
    size_t size() const
    {
        return pqueue_.size();
    }
    bool empty() const
    {
        return pqueue_.empty();
    }

    friend std::ostream &operator<<(std::ostream &os, const DDLManager &m);

private:
    time::term_t to_ddl(
        const std::chrono::time_point<std::chrono::steady_clock> &tp)
    {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   tp.time_since_epoch())
            .count();
    }

    std::priority_queue<value_type> pqueue_;
};

inline std::ostream &operator<<(std::ostream &os, const DDLManager &m)
{
    os << "{DDLManager: task_nr: " << m.size();
    if (m.pqueue_.empty())
    {
        os << ", Empty";
    }
    else
    {
        os << ", front DDL: " << m.pqueue_.top().ddl();
    }
    os << "}";
    return os;
}
}  // namespace patronus

#endif