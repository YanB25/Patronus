#include <cassert>
#include <thread>

#include "btree.h"

const int kThread = 12;
std::thread th[kThread];
const uint64_t kKeyRange = 121355 * kThread;
btree *bt;

void thread_insert(int id)
{
    uint64_t gap = kKeyRange / kThread;
    uint64_t from = id * gap;
    uint64_t to = from + gap;

    //  uint64_t from = 0;
    // uint64_t to = kKeyRange;

    for (uint64_t i = from; i < to; ++i)
    {
        bt->btree_insert(i, (char *) (i * 2));
    }

    printf("insert OK\n");

    for (uint64_t i = from; i < to; ++i)
    {
        bt->btree_delete(i);
    }

    printf("delete OK\n");

    // for (uint64_t i = from; i < to; ++i) {
    //   bt->btree_insert(i, (char *)(i * 3));
    // }

    //  printf("update OK\n");
}

void check()
{
    for (uint64_t i = 0; i < kKeyRange; ++i)
    {
        auto v = bt->btree_search(i);
        if (v)
        {
            printf("%ld %ld\n", i, uint64_t(v));
            assert(false);
        }
        // assert((uint64_t)v == (i * 3));
    }
}

// MAIN
int main(int argc, char **argv)
{
    bt = new btree();

    for (int i = 0; i < kThread; ++i)
    {
        th[i] = std::thread(thread_insert, i);
    }

    for (int i = 0; i < kThread; ++i)
    {
        th[i].join();
    }

    check();

    return 0;
}
