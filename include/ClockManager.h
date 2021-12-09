#pragma once
#ifndef __CLOCK_MANNAGER__
#define __CLOCK_MANNAGER__
#include <memory>

class DSM;

struct SyncTimeMessage
{
    int32_t node_id;
    uint64_t clock;
} __attribute__((packed));

class ClockManager
{
public:
    ClockManager(const ClockManager&) = delete;
    ClockManager& operator=(const ClockManager&) = delete;
    ClockManager(DSM* dsm);

    inline uint64_t rdtsc();
    inline uint64_t clock();

    void init_sync_clock();
    void sync_clock();
private:
    void primary_init_sync_clock();
    void backup_init_sync_clock();
    void primary_sync_clock();
    void backup_sync_clock();

    inline void set_offset(int64_t offset);
    inline void set_clock(uint64_t target_clock)
    {
        auto my_clock = clock();
        int64_t offset = target_clock - my_clock;
        set_offset(offset);
    }
    DSM* dsm_;
    int64_t offset_{0};
};

#endif