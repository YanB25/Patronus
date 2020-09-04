#include "IndexCache.h"
#include <iostream>

int main()
{
    auto cache = new IndexCache(1);

    InternalPage *page = (InternalPage *)11;
    assert(cache->add_entry(1, 100, page));

    assert(cache->add_entry(1, 43, page));

    assert(cache->add_entry(43, 100, page));

    auto node = cache->find_entry(77);
    std::cout << *node << std::endl;

    node = cache->find_entry(30);
    std::cout << *node << std::endl;

    node = cache->find_entry(1);
    std::cout << *node << std::endl;

    node = cache->find_entry(43);
    std::cout << *node << std::endl;

    node = cache->find_entry(100);
    assert(!node);
    // std::cout << *node << std::endl;

    const uint64_t Space = 100000ull;
    const int loop = 10000;
    for (uint64_t i = 0; i < Space; ++i)
    {
        cache->add_entry(i, i + 1, nullptr);
    }

    Timer t;
    t.begin();
    for (int i = 0; i < loop; ++i)
    {
        uint64_t k = rand() % Space;
        cache->find_entry(k);
    }
    t.end_print(loop);
}