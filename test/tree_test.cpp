#include "DSM.h"
#include "Tree.h"

int main() {

  DSMConfig config;
  config.machineNR = 2;
  DSM *dsm = DSM::getInstance(config);
 
  dsm->registerThread();

  auto tree = new Tree(dsm);

  
  Value v;
  tree->search(1, v);

  while (true)
    ;
}