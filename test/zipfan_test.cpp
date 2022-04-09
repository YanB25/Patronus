#include "city.h"
#include "util/zipf.h"

constexpr uint64_t MB = 1024ull * 1024;
constexpr uint64_t kKeySpace = 1000 * MB;
constexpr uint64_t kIterCnt = kKeySpace;
uint64_t counter[kKeySpace];

void mehcached_test_zipf(double theta);

int main()
{
    mehcached_test_zipf(0.99);

    struct zipf_gen_state state;

    mehcached_zipf_init(&state, kKeySpace, 0.99, 0);

    for (uint64_t i = 0; i < kIterCnt; ++i)
    {
        counter[mehcached_zipf_next(&state)]++;
    }

    uint64_t sum = 0;
    for (uint64_t k = 0; k < kKeySpace; ++k)
    {
        sum += counter[k];
        double cum = 1.0 * sum / kIterCnt;

        if (k < 1000 || k % (100 * 1024) == 0)
        {
            printf("%ld\t%lf %lf\n", k, cum, 1.0 * k / kKeySpace);
        }
    }

    return 0;
}
