#include <glog/logging.h>

#include <chrono>

#include "Common.h"
#include "patronus/DDLManager.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

using namespace define::literals;

constexpr static size_t kTestTime = 1_M;

void burn()
{
    patronus::DDLManager m;

    for (size_t i = 0; i < kTestTime; ++i)
    {
        m.push(i, []() {});
    }
    for (size_t i = 0; i < kTestTime; ++i)
    {
        CHECK_EQ(m.do_task(i), 1);
    }
}
int main(int argc, char *argv[])
{
    using namespace std::chrono_literals;

    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    patronus::DDLManager m;
    auto now = std::chrono::steady_clock::now();

    m.push(now + 10ms, []() { LOG(INFO) << "+10ms"; });
    m.push(now + 10ms, []() { LOG(INFO) << "+10ms(2)"; });
    m.push(now + 20ms, []() { LOG(INFO) << "+20ms"; });
    m.push(now + 50ms, []() { LOG(INFO) << "+50ms"; });
    m.push(now + 100ms, []() { LOG(INFO) << "+100ms"; });

    CHECK_EQ(m.do_task(now + 30ms), 3) << "expect to finish 3 works";
    CHECK_EQ(m.do_task(now + 60ms), 1) << "expect to finish 3 works";
    CHECK_EQ(m.do_task(now + 100ms), 1) << "expect to finish 3 works";

    LOG(INFO) << "begin to burn";
    burn();
    LOG(INFO) << "finished. ctrl+C to quit.";
}