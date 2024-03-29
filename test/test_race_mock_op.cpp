#include <algorithm>
#include <random>
#include <set>

#include "Common.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "patronus/memory/direct_allocator.h"
#include "thirdparty/racehashing/hashtable.h"
#include "thirdparty/racehashing/hashtable_handle.h"
#include "thirdparty/racehashing/utils.h"
#include "util/Rand.h"

using namespace patronus::hash;
using namespace util::literals;

DEFINE_string(exec_meta, "", "The meta data of this execution");

void test_capacity(size_t initial_subtable)
{
    auto allocator = std::make_shared<patronus::mem::RawAllocator>();
    RaceHashingConfig conf;
    conf.initial_subtable = initial_subtable;
    conf.g_kvblock_pool_size = 512_MB;
    conf.g_kvblock_pool_addr = malloc(conf.g_kvblock_pool_size);

    using RaceHashingT = RaceHashing<1, 2, 2>;
    auto server_rdma_ctx = MockRdmaAdaptor::new_instance({});

    server_rdma_ctx->reg_default_allocator(
        patronus::mem::MallocAllocator::new_instance());
    patronus::mem::SlabAllocatorConfig slab_conf;
    slab_conf.block_class = {patronus::hash::config::kKVBlockAllocBatchSize};
    slab_conf.block_ratio = {1.0};
    auto slab_allocator = std::make_shared<patronus::mem::SlabAllocator>(
        conf.g_kvblock_pool_addr, conf.g_kvblock_pool_size, slab_conf);
    server_rdma_ctx->reg_allocator(patronus::hash::config::kAllocHintKVBlock,
                                   slab_allocator);

    RaceHashingT rh(server_rdma_ctx, allocator, conf);

    // RaceHashingHandleConfig handle_conf;
    auto handle_conf = RaceHashingConfigFactory::get_unprotected(
        "test mock", 64, 1 /* batch */, false /* mock kvblock match */);

    auto handle_rdma_ctx = MockRdmaAdaptor::new_instance(server_rdma_ctx);
    RaceHashingT::Handle rhh(
        0 /* node_id */, rh.meta_gaddr(), handle_conf, false, handle_rdma_ctx);
    rhh.init();

    std::string key;
    std::string value;
    std::map<std::string, std::string> inserted;
    size_t succ_nr = 0;
    size_t fail_nr = 0;
    char key_buf[128];
    char value_buf[128];

    util::TraceManager tm(0);
    LOG(INFO) << "Meta of hashtable: "
              << *(RaceHashingT::MetaT *) rh.meta_addr();

    for (size_t i = 0; i < 16; ++i)
    {
        fast_pseudo_fill_buf(key_buf, 8);
        fast_pseudo_fill_buf(value_buf, 8);
        key = std::string(key_buf, 8);
        value = std::string(value_buf, 8);
        LOG(INFO) << "Trying to push " << key << ", " << value;

        auto trace = tm.trace("test_capacity");
        trace.set("k", key);
        trace.set("v", value);
        trace.set("op", "put");

        auto rc = rhh.put(key, value, trace);
        if (rc == kOk)
        {
            inserted.emplace(key, value);
            succ_nr++;
            LOG(WARNING) << "[bench] pushed " << i
                         << "-th. succ_nr: " << succ_nr
                         << ", failed_nr: " << fail_nr << ", Table: " << rh;
        }
        else if (rc == kNoMem)
        {
            fail_nr++;
        }
        else
        {
            CHECK(false) << "Unknow return code: " << rc;
        }
    }
    handle_rdma_ctx->put_all_rdma_buffer();
    LOG(INFO) << "Inserted " << succ_nr << ", failed: " << fail_nr
              << ", ultilization: " << 1.0 * succ_nr / rh.max_capacity()
              << ", success rate: " << 1.0 * succ_nr / (succ_nr + fail_nr);

    LOG(INFO) << "Checking integrity";

    for (const auto &[key, expect_value] : inserted)
    {
        auto trace = tm.trace("test capacity: validate");
        trace.set("k", key);
        trace.set("v", expect_value);
        trace.set("op", "get");

        std::string get_val;
        CHECK_EQ(rhh.get(key, get_val, trace), kOk);
        CHECK_EQ(get_val, expect_value);
    }
    LOG(INFO) << rh;

    LOG(INFO) << "[bench] Integrity Passed. Delete all the kvs";

    for (const auto &[key, expect_value] : inserted)
    {
        auto trace = tm.trace("test capacity: tear down");
        trace.set("k", key);
        trace.set("v", expect_value);
        trace.set("op", "del");

        CHECK_EQ(rhh.del(key, trace), kOk);
    }

    free(conf.g_kvblock_pool_addr);
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    test_capacity(1);

    LOG(INFO) << "finished. ctrl+C to quit.";
}