#include "Tree.h"

#include "DSM.h"

int main()
{
    DSMConfig config;
    config.machineNR = 2;
    auto dsm = DSM::getInstance(config);

    dsm->registerThread();

    auto tree = new Tree(dsm);

    Value v;

    if (dsm->getMyNodeID() != 0)
    {
        while (true)
            ;
    }

    for (uint64_t i = 1; i < 10240; ++i)
    {
        tree->insert(i, i * 2);
    }

    for (uint64_t i = 10240 - 1; i >= 1; --i)
    {
        tree->insert(i, i * 3);
    }

    for (uint64_t i = 1; i < 10240; ++i)
    {
        [[maybe_unused]] auto res = tree->search(i, v);
        assert(res && v == i * 3);
    }

    for (uint64_t i = 10240 - 1; i >= 1; --i)
    {
        tree->del(i);
    }

    for (uint64_t i = 1; i < 10240; ++i)
    {
        [[maybe_unused]] auto res = tree->search(i, v);
        assert(!res);
    }

    [[maybe_unused]] auto res = tree->search(23333, v);
    assert(!res);

    printf("Hello\n");

    while (true)
        ;
}