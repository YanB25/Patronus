#include "DSM.h"
#include "Tree.h"

int main() {

  DSMConfig config;
  config.machineNR = 2;
  DSM *dsm = DSM::getInstance(config);
 
  dsm->registerThread();

  auto tree = new Tree(dsm);

  if (dsm->getMyNodeID() != 0) {
      while (true);
  }

  
  Value v;

  tree->insert(dsm->getMyNodeID() + 1, (dsm->getMyNodeID() + 1) * 2);

  sleep(3);


  v = 0;
  auto res = tree->search(1, v);
  std::cout << "search result:  " << res << " v: " << v << std::endl;

  res = tree->search(2, v);
  std::cout << "search result:  " << res << " v: " << v << std::endl;

  while (true)
    ;
}