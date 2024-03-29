#include <algorithm>
#include <random>
#include <set>

#include "DSM.h"
#include "Timer.h"
#include "gflags/gflags.h"
#include "util/monitor.h"

DEFINE_string(exec_meta, "", "The meta data of this execution");

// Two nodes
// one node issues cas operations

struct Object
{
    uint64_t lower;
    uint64_t upper;
};

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    {
        LocalityObjectPool<Object> obj_pool(1024);
        for (size_t times = 0; times < 4; ++times)
        {
            std::set<Object *> obj_addrs;
            std::set<uint64_t> ids;
            for (size_t i = 0; i < 1024; ++i)
            {
                auto *addr = CHECK_NOTNULL(obj_pool.get());
                memset(addr, 0, sizeof(Object));
                CHECK_EQ(obj_addrs.count(addr), 0);
                obj_addrs.insert(addr);
                auto id = obj_pool.obj_to_id(addr);
                CHECK_EQ(ids.count(id), 0);
                ids.insert(id);
                CHECK_EQ(obj_pool.id_to_obj(id), addr)
                    << "check failed at times " << times << ", i = " << i
                    << ", id " << id << ", addr " << (void *) addr;
            }
            CHECK_EQ(obj_addrs.size(), 1024);
            CHECK_EQ(ids.size(), 1024);
            for (Object *po : obj_addrs)
            {
                obj_pool.put(po);
            }
        }
    }

    {
        LocalityBufferPool buff_pool(1024, sizeof(Object));
        for (size_t times = 0; times < 5; ++times)
        {
            std::set<void *> buf_addrs;
            std::set<uint64_t> ids;
            for (size_t i = 0; i < 1024; ++i)
            {
                auto *addr = CHECK_NOTNULL(buff_pool.get());
                memset(addr, 0, sizeof(Object));
                CHECK_EQ(buf_addrs.count(addr), 0)
                    << " times " << times << ", i " << i << ", addr "
                    << (void *) addr;
                buf_addrs.insert(addr);
                auto id = buff_pool.obj_to_id(addr);
                CHECK_EQ(ids.count(id), 0);
                ids.insert(id);
                CHECK_EQ(buff_pool.id_to_obj(id), addr)
                    << "check failed at times " << times << ", i = " << i
                    << ", id " << id << ", addr" << (void *) addr;
            }
            CHECK_EQ(buf_addrs.size(), 1024);
            CHECK_EQ(ids.size(), 1024);
            for (void *addr : buf_addrs)
            {
                buff_pool.put(addr);
            }
        }
    }
    LOG(INFO) << "PASS";
}