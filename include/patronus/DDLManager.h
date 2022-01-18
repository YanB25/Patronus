#pragma once
#ifndef PATRONUS_DDL_MANAGER_H_
#define PATRONUS_DDL_MANAGER_H_

#include <algorithm>
#include <functional>
#include <queue>
#include <vector>

#include "Common.h"
#include "patronus/Type.h"

namespace patronus
{
using Task = std::function<void()>;
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
                   size_t limit = kLimit)
    {
        auto ddl = to_ddl(tp);
        return do_task(ddl, limit);
    }
    size_t do_task(time::term_t until, size_t limit = kLimit)
    {
        size_t done = 0;
        while (!pqueue_.empty())
        {
            auto &front = pqueue_.top();
            if (unlikely(front.ddl() > until))
            {
                return done;
            }
            DVLOG(4) << "[DDLManager] protronus_now: "
                     << time::PatronusTime(until)
                     << ", ns_diff: " << until - front.ddl();
            auto &task = front.task();
            task();
            done++;
            pqueue_.pop();
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
}  // namespace patronus

#endif