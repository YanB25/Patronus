#include <algorithm>
#include <random>
#include <set>

#include "Common.h"
#include "gflags/gflags.h"
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

[[maybe_unused]] constexpr uint16_t kClientNodeId = 1;
[[maybe_unused]] constexpr uint16_t kServerNodeId = 0;
constexpr uint32_t kMachineNr = 2;
constexpr static size_t kCoroCnt = 1;

constexpr static size_t kWaitKey = 0;

using RaceHashingT = RaceHashing<1, 2, 2>;
using RaceHandleT = typename RaceHashingT::Handle;

RaceHandleT::pointer gen_rhh(Patronus::pointer p,
                             bool auto_expand,
                             CoroContext *ctx)
{
    auto tid = p->get_thread_id();
    auto dir_id = tid;

    auto meta_gaddr = p->get_object<GlobalAddress>("race:meta_gaddr", 1ms);
    LOG(INFO) << "[bench] got meta of hashtable: " << meta_gaddr;
    RaceHashingHandleConfig handle_conf;
    auto handle_rdma_ctx = patronus::RdmaAdaptor::new_instance(
        kServerNodeId, dir_id, p, false /* bypass_prot */, ctx);
    auto prhh = std::make_shared<RaceHandleT>(
        kServerNodeId, meta_gaddr, handle_conf, auto_expand, handle_rdma_ctx);
    prhh->init();
    return prhh;
}

void client_worker(Patronus::pointer p,
                   size_t coro_id,
                   CoroYield &yield,
                   CoroExecutionContext<kCoroCnt> &exe)
{
    CoroContext ctx(0, &yield, &exe.master(), coro_id);

    auto prhh = gen_rhh(p, false, &ctx);
    auto &rhh = *prhh;

    std::string key;
    std::string value;
    std::map<std::string, std::string> inserted;
    size_t succ_nr = 0;
    size_t fail_nr = 0;
    char key_buf[128];
    char value_buf[128];

    HashContext dctx(0);

    for (size_t i = 0; i < 16; ++i)
    {
        fast_pseudo_fill_buf(key_buf, 8);
        fast_pseudo_fill_buf(value_buf, 8);
        key = std::string(key_buf, 8);
        value = std::string(value_buf, 8);
        LOG(INFO) << "Trying to push " << key << ", " << value;

        dctx.key = key;
        dctx.value = value;
        dctx.op = "put";
        auto rc = rhh.put(key, value, &dctx);
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
    LOG(INFO) << "Inserted " << succ_nr << ", failed: " << fail_nr
              << ", success rate: " << 1.0 * succ_nr / (succ_nr + fail_nr);

    double utilization = 1.0 * succ_nr / rhh.max_capacity();
    LOG(INFO) << "[bench] inserted: " << succ_nr
              << ", capacity: " << rhh.max_capacity();
    CHECK_DOUBLE_EQ(utilization, 1);

    LOG(INFO) << "Checking integrity";

    for (const auto &[key, expect_value] : inserted)
    {
        dctx.key = key;
        dctx.value = expect_value;
        dctx.op = "get";
        std::string get_val;
        CHECK_EQ(rhh.get(key, get_val, &dctx), kOk);
        CHECK_EQ(get_val, expect_value);
    }

    LOG(INFO) << "[bench] Integrity Passed. Delete all the kvs";

    for (const auto &[key, expect_value] : inserted)
    {
        dctx.key = key;
        dctx.value = expect_value;
        dctx.op = "del";
        CHECK_EQ(rhh.del(key, &dctx), kOk);
    }
}

void client_master(Patronus::pointer p,
                   CoroYield &yield,
                   CoroExecutionContext<kCoroCnt> &exe)
{
    auto tid = p->get_thread_id();

    CoroContext mctx(tid, &yield, exe.workers());
    CHECK(mctx.is_master());

    for (size_t i = 0; i < kCoroCnt; ++i)
    {
        mctx.yield_to_worker(i);
    }

    LOG(INFO) << "Return back to master. start to recv messages";
    coro_t coro_buf[2 * kCoroCnt];
    while (!exe.is_finished_all())
    {
        auto nr = p->try_get_client_continue_coros(coro_buf, 2 * kCoroCnt);
        for (size_t i = 0; i < nr; ++i)
        {
            auto coro_id = coro_buf[i];
            DVLOG(1) << "[bench] yielding due to CQE: " << (int) coro_id;
            mctx.yield_to_worker(coro_id);
        }
    }

    p->finished(kWaitKey);
    LOG(WARNING) << "[bench] all worker finish their work. exiting...";
}

void client_test_capacity(Patronus::pointer p)
{
    CoroExecutionContext<kCoroCnt> coro_exe_ctx;
    auto tid = p->get_thread_id();
    LOG(INFO) << "I am client. tid " << tid;

    auto meta_gaddr = p->get_object<GlobalAddress>("race:meta_gaddr", 1ms);
    LOG(INFO) << "[bench] got meta of hashtable: " << meta_gaddr;

    for (size_t i = 0; i < kCoroCnt; ++i)
    {
        coro_exe_ctx.worker(i) =
            CoroCall([p, coro_id = i, &coro_exe_ctx](CoroYield &yield) {
                client_worker(p, coro_id, yield, coro_exe_ctx);
            });
    }
    auto &master = coro_exe_ctx.master();
    master = CoroCall([p, &coro_exe_ctx](CoroYield &yield) {
        client_master(p, yield, coro_exe_ctx);
    });

    master();
}

void server(Patronus::pointer p, size_t initial_subtable)
{
    auto tid = p->get_thread_id();
    LOG(INFO) << "[bench] server starts to work. tid " << tid;

    auto rh_buffer = p->get_user_reserved_buffer();
    CHECK_GE(rh_buffer.size, kKVBlockReserveSize);

    auto rh_allocator_buffer = Buffer(rh_buffer.buffer + kKVBlockReserveSize,
                                      rh_buffer.size - kKVBlockReserveSize);
    char *kvblock_pool_addr = rh_buffer.buffer;
    size_t kvblock_pool_size = kKVBlockReserveSize;

    // for server to handle kv block allocation requests
    RaceHashingConfig conf;
    conf.initial_subtable = initial_subtable;
    conf.g_kvblock_pool_size = kKVBlockReserveSize;
    conf.g_kvblock_pool_addr = kvblock_pool_addr;
    mem::SlabAllocatorConfig kvblock_slab_config;
    kvblock_slab_config.block_class = {hash::config::kKVBlockAllocBatchSize};
    kvblock_slab_config.block_ratio = {1.0};
    auto kvblock_allocator = mem::SlabAllocator::new_instance(
        kvblock_pool_addr, kvblock_pool_size, kvblock_slab_config);
    p->reg_allocator(hash::config::kAllocHintKVBlock, kvblock_allocator);

    auto server_rdma_ctx = patronus::RdmaAdaptor::new_instance(p);

    // for server to init the begining directory, subtables
    mem::SlabAllocatorConfig rh_slab_conf;
    rh_slab_conf.block_class = {2_MB};
    rh_slab_conf.block_ratio = {1.0};
    auto rh_slab_allocator = mem::SlabAllocator::new_instance(
        rh_allocator_buffer.buffer, rh_allocator_buffer.size, rh_slab_conf);

    RaceHashingT rh(server_rdma_ctx, rh_slab_allocator, conf);

    auto meta_gaddr = rh.meta_gaddr();
    p->put("race:meta_gaddr", meta_gaddr, 0ns);

    LOG(INFO) << "[bench] meta gaddr is " << meta_gaddr;

    p->server_serve(kWaitKey);

    p->patronus_free(
        conf.g_kvblock_pool_addr, conf.g_kvblock_pool_size, 0 /* hint */);
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    PatronusConfig config;
    config.machine_nr = kMachineNr;
    config.block_class = {2_MB, 4_KB};
    config.block_ratio = {0.5, 0.5};

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
        patronus->finished(kWaitKey);
        server(patronus, 1 /* subtable */);
    }
    LOG(INFO) << "finished. ctrl+C to quit.";
}