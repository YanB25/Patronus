#include "DSM.h"
#include "Timer.h"

// Two nodes
// one node issues cas operations

int main() {

  DSMConfig config;
  config.machineNR = 2;
  DSM *dsm = DSM::getInstance(config);

  sleep(1);

  dsm->registerThread();

  if (dsm->getMyNodeID() == 1) {
    while (true)
      ;
  }

  GlobalAddress gaddr;
  gaddr.nodeID = 1;
  gaddr.offset = 1024;

  auto *buffer = dsm->get_rdma_buffer();

  {
    uint64_t cur_val = 0;
    *(uint64_t *)buffer = cur_val;
    dsm->write_sync(buffer, gaddr, sizeof(uint64_t));

    uint64_t loop = 1000000;
    Timer timer;

    printf("\n--------  write ----------\n");
    timer.begin();
    for (size_t i = 0; i < loop; ++i) {
      dsm->write_sync(buffer, gaddr, sizeof(uint64_t));
    }
    timer.end_print(loop);

    printf("\n-------- read ----------\n");
    timer.begin();
    for (size_t i = 0; i < loop; ++i) {
      dsm->read_sync(buffer, gaddr, sizeof(uint64_t));
    }
    timer.end_print(loop);

    printf("\n-------- cas succ ----------\n");
    timer.begin();
    for (size_t i = 0; i < loop; ++i) {
      bool res = dsm->cas_sync(gaddr, cur_val, cur_val + 1, (uint64_t *)buffer);

      assert(res);
      cur_val++;
    }
    timer.end_print(loop);

    printf("\n-------- cas fail ----------\n");
    timer.begin();
    for (size_t i = 0; i < loop; ++i) {
      bool res = dsm->cas_sync(gaddr, 1, cur_val + 1, (uint64_t *)buffer);

      assert(!res);
    }
    timer.end_print(loop);

    printf("\n-------- cas mask succ ----------\n");
    timer.begin();
    for (size_t i = 0; i < loop; ++i) {
      bool res =
          dsm->cas_mask_sync(gaddr, cur_val, cur_val + 1, (uint64_t *)buffer);

      assert(res);
      cur_val++;
    }
    timer.end_print(loop);

    printf("\n-------- cas mask fail ----------\n");
    timer.begin();
    for (size_t i = 0; i < loop; ++i) {
      bool res = dsm->cas_mask_sync(gaddr, 1, cur_val + 1, (uint64_t *)buffer);

      assert(!res);
    }
    timer.end_print(loop);

    // function call time
    size_t call_loop = 100;
    printf("\n -------- cas function call ----------\n");
    timer.begin();
    for (size_t i = 0; i < call_loop; ++i) {
      dsm->cas(gaddr, 1, cur_val + 1, (uint64_t *)buffer);
    }
    timer.end_print(call_loop);

    dsm->poll_rdma_cq(call_loop);

    printf("\n -------- cas mask function call ----------\n");
    timer.begin();
    for (size_t i = 0; i < call_loop; ++i) {
      dsm->cas_mask(gaddr, 1, cur_val + 1, (uint64_t *)buffer);
    }
    timer.end_print(call_loop);

    dsm->poll_rdma_cq(call_loop);
  }

  { // on-chip memory
    uint64_t cur_val = 0;
    *(uint64_t *)buffer = cur_val;
    dsm->write_sync(buffer, gaddr, sizeof(uint64_t));

    uint64_t loop = 1000000;
    Timer timer;

    printf("\n-------- dm write ----------\n");
    timer.begin();
    for (size_t i = 0; i < loop; ++i) {
      dsm->write_dm_sync(buffer, gaddr, sizeof(uint64_t));
    }
    timer.end_print(loop);

    printf("\n-------- dm read ----------\n");
    timer.begin();
    for (size_t i = 0; i < loop; ++i) {
      dsm->read_dm_sync(buffer, gaddr, sizeof(uint64_t));
    }
    timer.end_print(loop);

    printf("\n-------- dm cas succ ----------\n");
    timer.begin();
    for (size_t i = 0; i < loop; ++i) {
      bool res =
          dsm->cas_dm_sync(gaddr, cur_val, cur_val + 1, (uint64_t *)buffer);

      assert(res);
      cur_val++;
    }
    timer.end_print(loop);

    printf("\n-------- dm cas fail ----------\n");
    timer.begin();
    for (size_t i = 0; i < loop; ++i) {
      bool res = dsm->cas_dm_sync(gaddr, 1, cur_val + 1, (uint64_t *)buffer);

      assert(!res);
    }
    timer.end_print(loop);

    printf("\n-------- dm cas mask succ ----------\n");
    timer.begin();
    for (size_t i = 0; i < loop; ++i) {
      bool res = dsm->cas_dm_mask_sync(gaddr, cur_val, cur_val + 1,
                                       (uint64_t *)buffer);

      assert(res);
      cur_val++;
    }
    timer.end_print(loop);

    printf("\n-------- dm cas mask fail ----------\n");
    timer.begin();
    for (size_t i = 0; i < loop; ++i) {
      bool res =
          dsm->cas_dm_mask_sync(gaddr, 1, cur_val + 1, (uint64_t *)buffer);

      assert(!res);
    }
    timer.end_print(loop);

    // function call time
    size_t call_loop = 100;
    printf("\n -------- cas dm function call ----------\n");
    timer.begin();
    for (size_t i = 0; i < call_loop; ++i) {
      dsm->cas_dm(gaddr, 1, cur_val + 1, (uint64_t *)buffer);
    }
    timer.end_print(call_loop);

    dsm->poll_rdma_cq(call_loop);

    printf("\n -------- cas dm mask function call ----------\n");
    timer.begin();
    for (size_t i = 0; i < call_loop; ++i) {
      dsm->cas_dm_mask(gaddr, 1, cur_val + 1, (uint64_t *)buffer);
    }
    timer.end_print(call_loop);

    dsm->poll_rdma_cq(call_loop);
  }

  printf("OK\n");

  while (true)
    ;
}