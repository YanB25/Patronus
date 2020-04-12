#include "DSM.h"

int main() {

  DSMConfig config;
  config.machineNR = 2;
  DSM *dsm = DSM::getInstance(config);

  sleep(1);

  dsm->registerThread();

  if (dsm->getMyNodeID() == 0) {
    // RawMessage m;
    // m.num = 111;

    // dsm->rpc_call_dir(m, 1);

    GlobalAddress gaddr;
    gaddr.nodeID = 1;
    gaddr.offset = 8;

    auto *buffer = dsm->get_rdma_buffer();
    char *s = "hello, world";
    int size = strlen(s);
    memcpy(buffer, s, size);

    dsm->write_sync(buffer, gaddr, size);

    printf("WRITE END %d\n", size);

    buffer += size;
    dsm->read_sync(buffer, gaddr, size);

    for (int i = 0; i < size; ++i) {
      printf("%c", buffer[i]);
    }
    printf("\n");

    *(uint64_t *)buffer = 123;
    dsm->write_sync(buffer, gaddr, sizeof(uint64_t));
    buffer += 12;

    buffer += 12;
    bool res = dsm->cas_mask_sync(gaddr, 1, 0, (uint64_t *)buffer, 1);

    assert(res);
    printf("%ld\n", *(uint64_t *)buffer);

    buffer += 12;
    dsm->read_sync(buffer, gaddr, sizeof(uint64_t));
    printf("%ld\n", *(uint64_t *)buffer);
  }

  printf("OK\n");

  while (true)
    ;
}