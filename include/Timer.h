#if !defined(_TIMER_H_)
#define _TIMER_H_

#include <cstdint>
#include <time.h>
#include <cstdio>

class Timer {
public:
  Timer() = default;

  void begin() {
      clock_gettime(CLOCK_REALTIME, &s);
  }

  void end(uint64_t loop = 1) {
      this->loop = loop;
      clock_gettime(CLOCK_REALTIME, &e);
  }

  void print() {
      uint64_t ns_all = (e.tv_sec - s.tv_sec) * 1000000000ull +
                    (e.tv_nsec - s.tv_nsec);
      uint64_t ns = ns_all / loop;

      if (ns < 1000) {
        printf("%lxns per loop\n", ns);
      } else {
        printf("%lfus per loop\n", ns * 1.0 / 1000);
      }
  }

  void end_print(uint64_t loop = 1) {
      end(loop);
      print();
  }


private:
  timespec s,e;
  uint64_t loop;
};

#endif // _TIMER_H_
