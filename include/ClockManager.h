#pragma once
#ifndef __CLOCK_MANNAGER__
#define __CLOCK_MANNAGER__
#include <memory>

#include "util/Util.h"

class DSM;

struct SyncTimeMessage
{
    int32_t node_id;
    uint64_t clock;
} __attribute__((packed));

class ClockManager
{
public:
    ClockManager(const ClockManager &) = delete;
    ClockManager &operator=(const ClockManager &) = delete;
    ClockManager(DSM *dsm);

    uint64_t rdtsc()
    {
        uint32_t lo = 0;
        uint32_t hi = 0;
        // We cannot use "=A", since this would use %rax on x86_64
        __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
        return ((uint64_t) hi << 32) | lo;
    }
    uint64_t clock()
    {
        return util::rdtsc() + offset_;
    }

    void init_sync_clock();
    void sync_clock();

private:
    void primary_init_sync_clock();
    void backup_init_sync_clock();
    void primary_sync_clock();
    void backup_sync_clock();

    void set_offset(int64_t offset)
    {
        offset_ = offset;
    }
    void set_clock(uint64_t target_clock)
    {
        auto my_clock = clock();
        int64_t offset = target_clock - my_clock;
        set_offset(offset);
    }
    DSM *dsm_;
    int64_t offset_{0};
};

#endif