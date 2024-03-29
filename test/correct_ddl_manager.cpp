#include <glog/logging.h>

#include <chrono>

#include "Common.h"
#include "gflags/gflags.h"
#include "patronus/DDLManager.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

using namespace util::literals;

constexpr static size_t kTestTime = 1_M;

void burn()
{
    patronus::DDLManager m;

    for (size_t i = 0; i < kTestTime; ++i)
    {
        m.push(i, [](CoroContext *) {});
    }
    for (size_t i = 0; i < kTestTime; ++i)
    {
        CHECK_EQ(m.do_task(i, nullptr), 1);
    }
}
int main(int argc, char *argv[])
{
    using namespace std::chrono_literals;

    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    patronus::DDLManager m;
    auto now = std::chrono::steady_clock::now();

    m.push(now + 10ms, [](CoroContext *) { LOG(INFO) << "+10ms"; });
    m.push(now + 10ms, [](CoroContext *) { LOG(INFO) << "+10ms(2)"; });
    m.push(now + 20ms, [](CoroContext *) { LOG(INFO) << "+20ms"; });
    m.push(now + 50ms, [](CoroContext *) { LOG(INFO) << "+50ms"; });
    m.push(now + 100ms, [](CoroContext *) { LOG(INFO) << "+100ms"; });

    CHECK_EQ(m.do_task(now + 30ms, nullptr), 3) << "expect to finish 3 works";
    CHECK_EQ(m.do_task(now + 60ms, nullptr), 1) << "expect to finish 3 works";
    CHECK_EQ(m.do_task(now + 100ms, nullptr), 1) << "expect to finish 3 works";

    LOG(INFO) << "begin to burn";
    burn();
    LOG(INFO) << "finished. ctrl+C to quit.";
}