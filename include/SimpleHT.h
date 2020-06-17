#if !defined(_SIMPLE_HT_H_)
#define _SIMPLE_HT_H_

#include "Common.h"
#include "GlobalAddress.h"

class SimpleHT {

  const static uint64_t kBuckSize = 640000;

public:
  SimpleHT() {
    table = new Item[kBuckSize];
    for (int i = 0; i < kBuckSize; ++i) {
      table[i].k = 0;
      table[i].v = GlobalAddress::Null();
    }
  }

  void set(Key k, GlobalAddress v) {
    auto &it = table[k % kBuckSize];
    it.k = k;
    it.v = v;
  }

  GlobalAddress get(Key k) {
    auto &it = table[k % kBuckSize];
    if (it.k == k) {
      return it.v;
    }
    return GlobalAddress::Null();
  }

private:
  struct Item {
    Key k;
    GlobalAddress v;
  };
  Item *table;
};

#endif // _SIMPLE_HT_H_
