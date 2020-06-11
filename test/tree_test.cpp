#include "DSM.h"
#include "Tree.h"

int main() {

  DSMConfig config;
  config.machineNR = 2;
  DSM *dsm = DSM::getInstance(config);
 
  dsm->registerThread();

  auto tree = new Tree(dsm);

  Value v;

  if (dsm->getMyNodeID() != 0) {
    while(true);
  }


  for (uint64_t i = 1; i < 32; ++i) {
    tree->insert(i, i * 2);
  }

  for (uint64_t i = 1; i < 32; ++i) {
    auto res = tree->search(i, v);
    std::cout << "search result:  " << res << " v: " << v << std::endl;
  }

  while (true)
    ;
}