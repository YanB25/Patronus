#include "Timer.h"
#include "Tree.h"
#include "zipf.h"

#include <city.h>
#include <stdlib.h>
#include <thread>
#include <time.h>
#include <unistd.h>
#include <vector>

inline size_t jenkins(const void *_ptr, size_t _len,
                      size_t _seed = 0xc70f6907UL) {
  // assert(_len < 100);
  size_t i = 0;
  size_t hash = 0;
  const char *key = static_cast<const char *>(_ptr);
  while (i != _len) {
    hash += key[i++];
    hash += hash << (10);
    hash ^= hash >> (6);
  }
  hash += hash << (3);
  hash ^= hash >> (11);
  hash += hash << (15);
  return hash;
}

extern volatile bool need_stop;

// #define USE_CORO

extern uint64_t cache_miss[MAX_APP_THREAD][8];
extern uint64_t cache_hit[MAX_APP_THREAD][8];
extern uint64_t lock_fail[MAX_APP_THREAD][8];
extern uint64_t pattern[MAX_APP_THREAD][8];
extern uint64_t hot_filter_count[MAX_APP_THREAD][8];

const int kMaxThread = 32;

int kReadRatio;
int kThreadCount;
int kNodeCount;
uint64_t kKeySpace = 20096000;
// 100 * define::MB;

double zipfan = 0.99;
int hottest = 1;

std::thread th[kMaxThread];
uint64_t tp[kMaxThread][8];

uint64_t latency[kMaxThread][10000];
uint64_t latency_th_all[10000];

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

    // if (rand_r(&seed) % 100 < 80) {
    //   dis = dis % uint64_t(kKeySpace / hottest);
    // }

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
    // sleep(5);
  }

  bindCore(id);

  dsm->registerThread();

#ifdef USE_CORO
  tree->run_coroutine(coro_func, id, 6);

#else

  /// without coro
  unsigned int seed = rdtsc();
  struct zipf_gen_state state;
  mehcached_zipf_init(&state, kKeySpace, zipfan,
                      rdtsc() & (0x0000ffffffffffffull) ^ id);

  Timer timer;
  while (true) {

    if (need_stop) {
      while (true)
        ;
    }

    uint64_t dis = mehcached_zipf_next(&state);

    // if (rand_r(&seed) % 100 < 80) {
    //   dis = dis % uint64_t(kKeySpace / hottest);
    // }

    // if (dis < 1) {
    //   continue;
    // }

    uint64_t key = CityHash64((char *)&dis, sizeof(dis)) + 1;

    // timer.begin();
    // tree->lock_bench(key);
    // auto us_10 = timer.end() / 100;
    // if (us_10 >= 10000) {
    //   us_10 = 9999;
    // }
    // latency[id][us_10]++;

    Value v;

    timer.begin();
    if (rand_r(&seed) % 100 < kReadRatio) { // GET
      tree->search(key, v);
    } else {
      v = 12;
      tree->insert(key, v);
    }
    auto us_10 = timer.end() / 100;
    if (us_10 >= 10000) {
      us_10 = 9999;
    }
    latency[id][us_10]++;

    tp[id][0]++;
  }

#endif
}

void warm_up() {

  // return;
  // if (dsm->getMyNodeID() == 0) {
  for (uint64_t i = 0; i < kKeySpace; ++i) {
    auto k = CityHash64((char *)&i, sizeof(i)) + 1;
    if (k % dsm->getClusterSize() == dsm->getMyNodeID()) {
      tree->insert(k, 12);
    }
    // }
    //
  }

  dsm->barrier("start-cache");
  tree->print_and_check_tree();

  dsm->barrier("end-cache");

  if (dsm->getMyNodeID() == 0) {
    for (uint64_t i = 0; i < 25; ++i) {
      auto k = CityHash64((char *)&i, sizeof(i)) + 1;
      std::cout << tree->query_cache(k) << "\t";
    }
    printf("\n");
  }

  dsm->barrier("end-print");
  // printf("End warmup\n");
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

void cal_latency() {
  uint64_t all_lat = 0;
  for (int i = 0; i < 10000; ++i) {
    latency_th_all[i] = 0;
    for (int k = 0; k < MAX_APP_THREAD; ++k) {
      latency_th_all[i] += latency[k][i];
    }
    all_lat += latency_th_all[i];
  }

  uint64_t th50 = all_lat / 2;
  uint64_t th90 = all_lat * 9 / 10;
  uint64_t th95 = all_lat * 95 / 100;
  uint64_t th99 = all_lat * 99 / 100;
  uint64_t th999 = all_lat * 999 / 1000;

  uint64_t cum = 0;
  for (int i = 0; i < 10000; ++i) {
    cum += latency_th_all[i];

    if (cum >= th50) {
      printf("p50 %f\t", i / 10.0);
      th50 = -1;
    }
    if (cum >= th90) {
      printf("p90 %f\t", i / 10.0);
      th90 = -1;
    }
    if (cum >= th95) {
      printf("p95 %f\t", i / 10.0);
      th95 = -1;
    }
    if (cum >= th99) {
      printf("p99 %f\t", i / 10.0);
      th99 = -1;
    }
    if (cum >= th999) {
      printf("p999 %f\t", i / 10.0);
      th999 = -1;
    }
  }
  printf("\n");
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

  if (dsm->getMyNodeID() == 0) {
    while (true)
      ;
  }

  for (int i = 0; i < kThreadCount; i++) {
    th[i] = std::thread(thread_run, i);
  }

  timespec s, e;
  uint64_t pre_tp = 0;
  uint64_t pre_ths[MAX_APP_THREAD];
  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    pre_ths[i] = 0;
  }

  int count = 0;
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

    printf("%d, throughput %.4f\n", cap * 1.0 / microseconds,
           dsm->getMyNodeID());
    for (int i = 0; i < kThreadCount; ++i) {
      auto val = tp[i][0];
      // printf("thread %d %ld\n", i, val - pre_ths[i]);
      pre_ths[i] = val;
    }

    uint64_t all = 0;
    uint64_t hit = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      all += (cache_hit[i][0] + cache_miss[i][0]);
      hit += cache_hit[i][0];
    }
    printf("cache hit rate: %lf\n", hit * 1.0 / all);

    uint64_t fail_locks_cnt = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      fail_locks_cnt += lock_fail[i][0];
      lock_fail[i][0] = 0;
    }
    if (fail_locks_cnt > 500000) {
      // need_stop = true;
    }

    printf("%d fail locks: %ld %s\n", dsm->getMyNodeID(), fail_locks_cnt,
           getIP());

    //  pattern
    uint64_t pp[8];
    memset(pp, 0, sizeof(pp));
    for (int i = 0; i < 8; ++i) {
      for (int t = 0; t < MAX_APP_THREAD; ++t) {
        pp[i] += pattern[t][i];
        pattern[t][i] = 0;
      }
    }
    printf("ACCESS PATTERN");
    for (int i = 0; i < 8; ++i) {
      printf("\t%ld", pp[i]);
    }
    printf("\n");

    uint64_t hot_count = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      hot_count += hot_filter_count[i][0];
      hot_filter_count[i][0] = 0;
    }
    printf("hot count %ld\n", hot_count);

    if (++count % 3 == 0 && dsm->getMyNodeID() == 1) {
      cal_latency();
    }
  }

  return 0;
}