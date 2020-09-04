#include "DSM.h"
#include "Tree.h"

#include "Timer.h"

#include <thread>

DSM *dsm;
Tree *tree;

extern uint64_t cache_miss[MAX_APP_THREAD][8];
extern uint64_t cache_hit[MAX_APP_THREAD][8];
extern bool enter_debug;

const uint64_t kSpace = 1000024000ull;
const int kThread = 20;

inline Key to_key(uint64_t k)
{
    return (CityHash64((char *)&k, sizeof(k)) + 1) % kSpace;
}

void cal_hit()
{
    uint64_t all = 0;
    uint64_t hit = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i)
    {
        all += (cache_hit[i][0] + cache_miss[i][0]);
        hit += cache_hit[i][0];

        cache_hit[i][0] = 0;
        cache_miss[i][0] = 0;
    }
    printf("hit: %f\n", hit * 1.0 / all);
}

void run_warmup(int id)
{

    dsm->registerThread();
    for (uint64_t i = 1; i < kSpace; ++i)
    {
        if (i % kThread == id)
        {
            tree->insert(to_key(i), i * 2);
        }
    }
}

int main()
{

    DSMConfig config;
    config.machineNR = 2;
    dsm = DSM::getInstance(config);

    dsm->registerThread();

    tree = new Tree(dsm);

    if (dsm->getMyNodeID() != 0)
    {
        while (true)
            ;
    }

    std::thread *th[kThread];

    for (int i = 0; i < kThread; ++i)
    {
        th[i] = new std::thread(run_warmup, i);
    }
    Timer timer;

    timer.begin();
    for (int i = 0; i < kThread; ++i)
    {
        th[i]->join();
    }
    uint64_t ns = timer.end();

    cal_hit();
    tree->index_cache_statistics();

    printf("Hello %lds\n", ns / 1000 / 1000 / 1000);

    while (true)
        ;
}