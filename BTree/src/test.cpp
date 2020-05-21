#include "btree.h"

#include <cassert>

// MAIN
int main(int argc, char **argv) {

  btree *bt;
  bt = new btree();

  const uint64_t k_upper = 123456;
  for (uint64_t k = 1; k < k_upper; ++k) {
    bt->btree_insert(k, (char *)(k * 2));
  }

  for (uint64_t k = 1; k < k_upper; ++k) {
    auto v = bt->btree_search(k);
    assert((uint64_t)v == (k * 2));
  }
}
