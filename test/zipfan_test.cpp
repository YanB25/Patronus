#include "zipf.h"

constexpr uint64_t MB = 1024ull * 1024;
constexpr uint64_t kKeySpace = 100 * MB;
constexpr uint64_t kIterCnt = 10 * kKeySpace;
uint64_t counter[kKeySpace];

int main() {

  struct zipf_gen_state state;

  mehcached_zipf_init(&state, kKeySpace, 0.99, 0);

  for (uint64_t i = 0; i < kIterCnt; ++i) {
    counter[mehcached_zipf_next(&state)]++;
  }

  uint64_t sum = 0;
  for (uint64_t k = 0; k < kKeySpace; ++k) {
      sum += counter[k];
      double cum = 1.0 * sum / kIterCnt;

      if (k % (100 * 1024) == 0) {
          printf("%lf %lf\n", cum, 1.0 * k / kKeySpace);
      }
  }



  return 0;
}
