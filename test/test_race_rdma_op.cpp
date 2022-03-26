#include <algorithm>
#include <random>
#include <set>

#include "Common.h"
#include "glog/logging.h"
#include "patronus/Patronus.h"
#include "patronus/RdmaAdaptor.h"
#include "patronus/memory/direct_allocator.h"
#include "thirdparty/racehashing/hashtable.h"
#include "thirdparty/racehashing/hashtable_handle.h"
#include "thirdparty/racehashing/utils.h"
#include "util/Rand.h"

using namespace patronus::hash;
using namespace define::literals;
using namespace patronus;

constexpr static size_t kKVBlockReserveSize = 512_MB;

DEFINE_string(exec_meta, "", "The meta data of this execution");

[[maybe_unused]] constexpr uint16_t kClientNodeId = 0;
[[maybe_unused]] constexpr uint16_t kServerNodeId = 1;
constexpr uint32_t kMachineNr = 2;

using RaceHashingT = RaceHashing<1, 2, 2>;
using RaceHandleT = typename RaceHashingT::Handle;

void client_test_capacity(Patronus::pointer p)
{
    auto meta_gaddr = p->get_object<GlobalAddress>("race:meta_gaddr", 1ms);

    LOG(INFO) << "[bench] got meta of hashtable: " << meta_gaddr;

    RaceHashingHandleConfig handle_conf;

    auto handle_rdma_ctx =
        patronus::RdmaAdaptor::new_instance(kServerNodeId, p);
    RaceHandleT rhh(kServerNodeId,
                    meta_gaddr,
                    handle_conf,
                    handle_rdma_ctx,
                    nullptr /* coro */);
    rhh.init();

    std::string key;
    std::string value;
    std::map<std::string, std::string> inserted;
    size_t succ_nr = 0;
    size_t fail_nr = 0;
    char key_buf[128];
    char value_buf[128];

    HashContext ctx(0);

    for (size_t i = 0; i < 16; ++i)
    {
        fast_pseudo_fill_buf(key_buf, 8);
        fast_pseudo_fill_buf(value_buf, 8);
        key = std::string(key_buf, 8);
        value = std::string(value_buf, 8);
        LOG(INFO) << "Trying to push " << key << ", " << value;

        ctx.key = key;
        ctx.value = value;
        ctx.op = "put";
        auto rc = rhh.put(key, value, &ctx);
        if (rc == kOk)
        {
            inserted.emplace(key, value);
            succ_nr++;
            LOG(WARNING) << "[bench] pushed " << i
                         << "-th. succ_nr: " << succ_nr
                         << ", failed_nr: " << fail_nr;
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
              << ", success rate: " << 1.0 * succ_nr / (succ_nr + fail_nr);

    LOG(INFO) << "Checking integrity";

    for (const auto &[key, expect_value] : inserted)
    {
        ctx.key = key;
        ctx.value = expect_value;
        ctx.op = "get";
        std::string get_val;
        CHECK_EQ(rhh.get(key, get_val, &ctx), kOk);
        CHECK_EQ(get_val, expect_value);
    }

    LOG(INFO) << "[bench] Integrity Passed. Delete all the kvs";

    for (const auto &[key, expect_value] : inserted)
    {
        ctx.key = key;
        ctx.value = expect_value;
        ctx.op = "del";
        CHECK_EQ(rhh.del(key, &ctx), kOk);
    }
}

void server(Patronus::pointer p, size_t initial_subtable)
{
    auto tid = p->get_thread_id();
    LOG(INFO) << "[bench] server starts to work. tid " << tid;

    auto allocator = std::make_shared<patronus::mem::RawAllocator>();
    RaceHashingConfig conf;
    conf.initial_subtable = initial_subtable;
    conf.g_kvblock_pool_size = kKVBlockReserveSize;
    conf.g_kvblock_pool_addr = malloc(conf.g_kvblock_pool_size);

    auto server_rdma_ctx = patronus::RdmaAdaptor::new_instance(0, p);

    RaceHashingT rh(server_rdma_ctx, allocator, conf);

    auto meta_gaddr = rh.meta_gaddr();
    p->put("race:meta_gaddr", meta_gaddr, 0ns);

    LOG(INFO) << "[debug] meta gaddr is " << meta_gaddr;

    p->server_serve(tid);

    free(conf.g_kvblock_pool_addr);
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    PatronusConfig config;
    config.machine_nr = kMachineNr;

    auto patronus = Patronus::ins(config);
    auto nid = patronus->get_node_id();
    if (nid == kClientNodeId)
    {
        patronus->registerClientThread();
        client_test_capacity(patronus);
    }
    else
    {
        patronus->registerServerThread();
        patronus->finished();
        server(patronus, 1 /* subtable */);
    }
    LOG(INFO) << "finished. ctrl+C to quit.";
}