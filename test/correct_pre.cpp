#include <algorithm>
#include <map>
#include <random>

#include "Timer.h"
#include "patronus/Patronus.h"
#include "util/Pre.h"
#include "util/Rand.h"
#include "util/monitor.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    std::vector<size_t> vec;
    for (size_t i = 0; i < 20; ++i)
    {
        vec.push_back(fast_pseudo_rand_int(0, 100));
    }
    LOG(INFO) << "pre_vec: " << util::pre_vec(vec);
    for (size_t i = 0; i < 22; ++i)
    {
        LOG(INFO) << "pre_vec(" << i << "): " << util::pre_vec(vec, i);
    }

    std::map<std::string, std::string> m;
    for (size_t i = 0; i < 20; ++i)
    {
        std::string key(4, ' ');
        std::string value(4, ' ');
        fast_pseudo_fill_buf(&key[0], 4);
        fast_pseudo_fill_buf(&value[0], 4);
        m.emplace(key, value);
    }

    for (size_t i = 0; i < 22; ++i)
    {
        LOG(INFO) << "pre_map(" << i << "): " << util::pre_map(m, i);
    }

    LOG(INFO) << "finished. ctrl+C to quit.";
}