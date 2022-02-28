#include <algorithm>
#include <random>

#include "Timer.h"
#include "patronus/LeaseCache.h"
#include "patronus/Patronus.h"
#include "util/monitor.h"

using namespace patronus;

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    LeaseCache<3> cache;

    char buf[1024];

    memcpy(buf, "abcde", 5);
    cache.insert(0, 5, buf);
    memcpy(buf, "xyzkj", 5);
    cache.insert(15, 20, buf);
    memcpy(buf, "ABC", 3);
    cache.insert(7, 10, buf);

    char o_buf[1024];
    CHECK(cache.query(0, 5, o_buf));
    CHECK_EQ(memcmp(o_buf, "abcde", 5), 0);
    CHECK(cache.query(15, 20, o_buf));
    CHECK_EQ(memcmp(o_buf, "xyzkj", 5), 0);
    CHECK(cache.query(7, 10, o_buf));
    CHECK_EQ(memcmp(o_buf, "ABC", 3), 0);

    CHECK(cache.query(2, 3, o_buf));
    CHECK_EQ(memcmp(o_buf, "cde", 3), 0);
    CHECK(!cache.query(2, 4, o_buf));
    CHECK(cache.query(4, 1, o_buf));
    CHECK_EQ(memcmp(o_buf, "e", 1), 0);
    CHECK(cache.query(5, 0, o_buf));

    CHECK(cache.query(7, 10, o_buf));
    CHECK_EQ(memcmp(o_buf, "ABC", 3), 0);
    CHECK(cache.query(9, 1, o_buf));
    CHECK_EQ(memcmp(o_buf, "C", 1), 0);

    CHECK(cache.query(18, 2, o_buf));
    CHECK_EQ(memcmp(o_buf, "kj", 2), 0);
    CHECK(cache.query(17, 2, o_buf));
    CHECK_EQ(memcmp(o_buf, "zk", 2), 0);

    cache.insert(100, 105, "!@#$%");
    CHECK(!cache.query(0, 5, o_buf));
    CHECK(!cache.query(0, 1, o_buf));
    CHECK(!cache.query(1, 2, o_buf));
    CHECK(!cache.query(2, 2, o_buf));
    CHECK(!cache.query(4, 1, o_buf));

    CHECK(cache.query(101, 3, o_buf));
    CHECK_EQ(memcmp(o_buf, "@#$", 3), 0);

    LOG(INFO) << "PASSED.";
}