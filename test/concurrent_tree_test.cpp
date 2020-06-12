#include "DSM.h"
#include "Tree.h"
#include <thread>

constexpr int kTestKeySpace = 10000000;
constexpr int kNodeNum = 2;
constexpr int kThreadNum = 1;

Tree *tree;
DSM *dsm;

void test(int id) {

  // if (dsm->getMyNodeID() == 0) {
  //   while (true);
  // }

  dsm->registerThread();

  int mod = dsm->getMyNodeID() * kThreadNum + id;
  int all_thread = kNodeNum * kThreadNum;
  for (uint64_t i = 1; i < kTestKeySpace; ++i) {
    if (i % all_thread == mod) {
      tree->insert(i, i * 2);
    }
    if (i % 100000 == 0) {
      printf("%d\n", i);
    }
  }
}

int main() {

  DSMConfig config;
  config.machineNR = kNodeNum;
  dsm = DSM::getInstance(config);

  dsm->registerThread();
  tree = new Tree(dsm);

  Value v;

  // test(0);

  std::thread th[kThreadNum];
  for (int i = 0; i < kThreadNum; ++i) {
    th[i] = std::thread(test, i);
  }

  for (int i = 0; i < kThreadNum; ++i) {
    th[i].join();
  }

  //   for (uint64_t i = 10240 - 1; i >= 1; --i) {
  //     tree->insert(i, i * 3);
  //   }

  //   for (uint64_t i = 1; i < 10240; ++i) {
  //     auto res = tree->search(i, v);
  //     assert(res && v == i * 3);
  //     // std::cout << "search result:  " << res << " v: " << v << std::endl;
  //   }

  //   for (uint64_t i = 10240 - 1; i >= 1; --i) {
  //     tree->del(i);
  //   }

  //   for (uint64_t i = 1; i < 10240; ++i) {
  //     auto res = tree->search(i, v);
  //     assert(!res);
  //     // std::cout << "search result:  " << res << " v: " << v << std::endl;
  //   }

  //   auto res = tree->search(23333, v);
  //   assert(!res);

  printf("Hello\n");

  while (true)
    ;
}