#include "DSM.h"

int main() {

  DSMConfig config;
  config.machineNR = 2;
  DSM *dsm = DSM::getInstance(config);

  sleep(4);

  dsm->registerThread();

  for (int i = 0; i < 102; i++) {
     std::cout << dsm->alloc(233) << std::endl;
  }

  while (true)
    ;
}