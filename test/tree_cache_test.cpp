#include "DSM.h"
#include "Tree.h"



extern uint64_t cache_miss[MAX_APP_THREAD][8];
extern uint64_t cache_hit[MAX_APP_THREAD][8];
extern bool enter_debug;

void cal_hit() {
  uint64_t all = 0;
  uint64_t hit = 0;
  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    all += (cache_hit[i][0] + cache_miss[i][0]);
    hit += cache_hit[i][0];

    cache_hit[i][0] = 0; 
    cache_miss[i][0] = 0;
  }
  printf("hit: %f\n", hit * 1.0 / all);


}

int main() {

  DSMConfig config;
  config.machineNR = 2;
  DSM *dsm = DSM::getInstance(config);

  dsm->registerThread();

  auto tree = new Tree(dsm);

  Value v;

  if (dsm->getMyNodeID() != 0) {
    while (true)
      ;
  }

  const uint64_t kSpace = 502400ull;
  for (uint64_t i = 1; i < kSpace; ++i) {
    tree->insert(i + 1, i * 2);

    // if (i % 100000 == 0) {
    //   printf("K\n");
    // }
  }

  printf("WarmUP\n");
  cal_hit();

  enter_debug = true;

  for (uint64_t i = 1; i < 10; ++i) {

    uint64_t k = 24544;
    tree->insert(k + 1, i * 2);

    printf("\n-------\n");

    if (i % 100000 == 0) {
      printf("K\n");
    }
  }
  cal_hit();

  printf("Hello\n");

  while (true)
    ;
}