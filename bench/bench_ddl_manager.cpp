#include <glog/logging.h>

#include <chrono>

#include "Common.h"
#include "patronus/DDLManager.h"
#include "util/Rand.h"

using namespace define::literals;

DEFINE_string(exec_meta, "", "The meta data of this execution");

void test_handle_speed(size_t test_size, size_t interval_us)
{
    using namespace std::chrono_literals;
    patronus::DDLManager m;
    auto now = std::chrono::steady_clock::now();

    for (size_t i = 0; i < test_size; ++i)
    {
        m.push(now + 1s + std::chrono::microseconds(i * interval_us),
               [](CoroContext *) {});
    }

    size_t done = 0;

    while (true)
    {
        auto now = std::chrono::steady_clock::now();
        if (m.do_task(now, nullptr))
        {
            // okay, the first task arrived.
            done++;
            break;
        }
    }

    size_t none_nr = 0;
    size_t have_nr = 0;
    size_t max_task_per_poll = 0;
    while (done < test_size)
    {
        auto now = std::chrono::steady_clock::now();
        size_t size = m.do_task(now, nullptr);
        if (size > 0)
        {
            done += size;
            have_nr++;
            max_task_per_poll = std::max(max_task_per_poll, size);
        }
        else
        {
            none_nr++;
        }
    }
    LOG(INFO) << "[bench] ongoing requests: " << test_size
              << ", interval_us: " << interval_us << ", none_nr: " << none_nr
              << ", have_nr: " << have_nr
              << ", max_task_per_poll: " << max_task_per_poll
              << ", (have_nr / test_nr) " << 1.0 * (have_nr) / (test_size);
}

void burn(size_t max_size)
{
    patronus::DDLManager m;
    size_t op = 0;
    Timer t;
    t.begin();
    for (size_t i = 0; i < max_size / 2; ++i)
    {
        m.push(fast_pseudo_rand_int(), [](CoroContext *) {});
    }
    for (size_t i = 0; i < 10_M; ++i)
    {
        bool insert = fast_pseudo_bool_with_nth(2);
        if (unlikely(m.empty()))
        {
            insert = true;
        }
        if (unlikely(m.size() >= max_size))
        {
            insert = false;
        }
        if (insert)
        {
            m.push(fast_pseudo_rand_int(), [](CoroContext *) {});
            op++;
        }
        else
        {
            DCHECK_LE(m.do_task(fast_pseudo_rand_int(), nullptr, 1), 1);
        }
    }
    auto ns = t.end();
    double ops = 1.0 * op * 1e9 / ns;
    double ns_per_op = ns / op;
    LOG(INFO) << "[burn] op: " << op << ", ns: " << ns << ",ops: " << ops
              << " ns per op: " << ns_per_op;
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    bindCore(0);

    burn(1_M);

    for (size_t ongoing : {1_K, 1_M, 2_M, 8_M})
    {
        for (size_t us : {1, 2, 4, 8})
        {
            test_handle_speed(ongoing, us);
        }
    }

    LOG(INFO) << "finished. ctrl+C to quit.";
}