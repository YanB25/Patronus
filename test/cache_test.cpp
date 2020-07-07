#include <iostream>
#include "IndexCache.h"


int main() {
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
}