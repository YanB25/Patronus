#include "DSM.h"

int main() {

  DSMConfig config;
  config.machineNR = 2;
  DSM *dsm = DSM::getInstance(config);

  sleep(1);

  dsm->registerThread();

  if (dsm->getMyNodeID() == 0) {
    RawMessage m;
    m.num = 111;

    dsm->rpc_call_dir(m, 1);

    auto r = dsm->rpc_wait();
    printf("recv %d\n", r->num);
  }

  while (true)
    ;
}