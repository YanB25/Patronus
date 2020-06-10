#include "DSM.h"
#include "Tree.h"

int main() {

  DSMConfig config;
  config.machineNR = 2;
  DSM *dsm = DSM::getInstance(config);
 
  dsm->registerThread();

  auto tree = new Tree(dsm);

  
  Value v;
  auto res = tree->search(1, v);

  std::cout << "search result: " << res << std::endl;

  while (true)
    ;
}