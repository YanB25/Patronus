#include "Tree.h"
#include "zipf.h"

#include <city.h>
#include <stdlib.h>
#include <thread>
#include <time.h>
#include <unistd.h>

// #define USE_CORO

static __inline__ unsigned long long rdtsc(void) {
  unsigned hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}

const int kMaxThread = 32;

int kReadRatio;
int kThreadCount;
int kNodeCount;
uint64_t kKeySpace = 40960;
// 100 * define::MB;

double zipfan = 0;

std::thread th[kMaxThread];
uint64_t tp[kMaxThread][8];

Tree *tree;
DSM *dsm;

class RequsetGenBench : public RequstGen {

public:
  RequsetGenBench(int coro_id, DSM *dsm, int id)
      : coro_id(coro_id), dsm(dsm), id(id) {
    seed = rdtsc();
    mehcached_zipf_init(&state, kKeySpace, zipfan,
                        rdtsc() & (0x0000ffffffffffffull) ^ id);
  }

  Request next() override {
    Request r;
    uint64_t dis = mehcached_zipf_next(&state);
    r.k = CityHash64((char *)&dis, sizeof(dis)) + 1;
    r.v = 23;
    r.is_search = rand_r(&seed) % 100 < kReadRatio;

    tp[id][0]++;

    return r;
  }

private:
  int coro_id;
  DSM *dsm;
  int id;

  unsigned int seed;
  struct zipf_gen_state state;
};

RequstGen *coro_func(int coro_id, DSM *dsm, int id) {
  return new RequsetGenBench(coro_id, dsm, id);
}

void thread_run(int id) {

  if (id != 0) {
    sleep(10);
  }

  bindCore(id);

  dsm->registerThread();

#ifdef USE_CORO
  tree->run_coroutine(coro_func, id, 4);

#else

  /// without coro
  unsigned int seed = rdtsc();
  struct zipf_gen_state state;
  mehcached_zipf_init(&state, kKeySpace, zipfan,
                      rdtsc() & (0x0000ffffffffffffull) ^ id);

  while (true) {

    uint64_t dis = mehcached_zipf_next(&state);
    uint64_t key = CityHash64((char *)&dis, sizeof(dis)) + 1;

    Value v;
    if (rand_r(&seed) % 100 < kReadRatio) { // GET
      tree->search(key, v);
    } else {
      v = 12;
      tree->insert(key, v);
    }

    tp[id][0]++;
  }

#endif
}

void warm_up() {

  // return;
  if (dsm->getMyNodeID() == 0) {
    for (uint64_t i = 1; i < 1024; ++i) {
      if (i % 5 == 0) {
        tree->insert(i, 12);
      }
    }
    // tree->print_and_check_tree();
  }
}

void parse_args(int argc, char *argv[]) {
  if (argc != 4) {
    printf("Usage: ./benchmark kNodeCount kReadRatio kThreadCount\n");
    exit(-1);
  }

  kNodeCount = atoi(argv[1]);
  kReadRatio = atoi(argv[2]);
  kThreadCount = atoi(argv[3]);

  printf("kNodeCount %d, kReadRatio %d, kThreadCount %d\n", kNodeCount,
         kReadRatio, kThreadCount);
}

int main(int argc, char *argv[]) {

  parse_args(argc, argv);

  DSMConfig config;
  config.machineNR = kNodeCount;
  dsm = DSM::getInstance(config);

  dsm->registerThread();
  tree = new Tree(dsm);

  warm_up();

  dsm->barrier("benchmark");

  for (int i = 0; i < kThreadCount; i++) {
    th[i] = std::thread(thread_run, i);
  }

  timespec s, e;
  uint64_t pre_tp = 0;
  while (true) {
    clock_gettime(CLOCK_REALTIME, &s);
    sleep(1);
    clock_gettime(CLOCK_REALTIME, &e);
    int microseconds = (e.tv_sec - s.tv_sec) * 1000000 +
                       (double)(e.tv_nsec - s.tv_nsec) / 1000;

    uint64_t all_tp = 0;
    for (int i = 0; i < kThreadCount; ++i) {
      all_tp += tp[i][0];
    }
    uint64_t cap = all_tp - pre_tp;
    pre_tp = all_tp;

    printf("throughput %.4f\n", cap * 1.0 / microseconds);
  }

  return 0;
}