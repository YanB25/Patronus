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

constexpr static size_t kWaitKey = 0;

constexpr static size_t kCoroCnt = 1;

constexpr static uint64_t kAllocHintA = 1;
constexpr static size_t kAllocSize = 4_KB;

DEFINE_string(exec_meta, "", "The meta data of this execution");

constexpr static uint64_t kMagic = 0xaabbccdd14237624;
struct Test
{
    uint64_t magic;
    uint64_t inited;
    GlobalAddress test_gaddr;
};
std::ostream &operator<<(std::ostream &os, const Test &t)
{
    os << "{Test magic: " << (void *) t.magic << ", inited: " << t.inited
       << ", test_gaddr: " << t.test_gaddr << "}";
    return os;
}

void client_worker(Patronus::pointer p,
                   size_t coro_id,
                   CoroYield &yield,
                   CoroExecutionContext<kCoroCnt> &exe_ctx)
{
    CoroContext ctx(0, &yield, &exe_ctx.master(), coro_id);
    p->keeper_barrier("ready", 100ms);

    auto gaddr = p->get_object<GlobalAddress>("p:gaddr", 1ms);
    auto dir_id = 0;
    auto server_nid = ::config::get_server_nids().front();

    LOG(INFO) << "Client get gaddr: " << gaddr;

    auto rdma_adpt =
        patronus::RdmaAdaptor::new_instance(server_nid,
                                            dir_id,
                                            p,
                                            false /* bypass prot */,
                                            false /* two sided */,
                                            &ctx);

    // checking acquire_perm
    {
        auto ac_flag = (flag_t) AcquireRequestFlag::kNoGc;
        auto handle =
            rdma_adpt->acquire_perm(gaddr, 0 /* hint */, 64, 0ns, ac_flag);
        auto rdma_buf = rdma_adpt->get_rdma_buffer(128);
        DCHECK_GE(rdma_buf.size, 128);
        // expect okay
        {
            auto rc = rdma_adpt->rdma_write(gaddr, rdma_buf.buffer, 64, handle);
            CHECK_EQ(rc, kOk);
            rc = rdma_adpt->commit();
            CHECK_EQ(rc, kOk);
        }
        // expect okay
        // write with offset 32 and length 32 (till the end)
        {
            auto rc = rdma_adpt->rdma_write(
                gaddr + 32, rdma_buf.buffer + 32, 32, handle);
            CHECK_EQ(rc, kOk);
            rc = rdma_adpt->commit();
            CHECK_EQ(rc, kOk);
        }

        // this will generate protection error
        // write one more byte than protected
        {
            auto rc1 =
                rdma_adpt->rdma_write(gaddr, rdma_buf.buffer, 65, handle);
            auto rc2 = rdma_adpt->commit();
            if (rc1 == kOk && rc2 == kOk)
            {
                CHECK(false) << "Failed to generate protection error.";
            }
            else
            {
                CHECK(rc1 == kRdmaProtectionErr || rc2 == kRdmaProtectionErr)
                    << "** Expect kRdmaProtectionError. got " << rc1 << " and "
                    << rc2;
            }
        }
        // this will generate protection error
        // write 64 byte but with one byte offset
        {
            auto rc1 =
                rdma_adpt->rdma_write(gaddr + 1, rdma_buf.buffer, 64, handle);
            auto rc2 = rdma_adpt->commit();
            if (rc1 == kOk && rc2 == kOk)
            {
                CHECK(false) << "Failed to generate protection error.";
            }
            else
            {
                CHECK(rc1 == kRdmaProtectionErr || rc2 == kRdmaProtectionErr)
                    << "** Expect kRdmaProtectionError. got " << rc1 << " and "
                    << rc2;
            }
        }

        auto rel_flag = 0;
        rdma_adpt->relinquish_perm(handle, 0, rel_flag);

        rdma_adpt->put_all_rdma_buffer();
    }

    // checking acquire_perm with allocation semantics
    {
        auto ac_flag = (flag_t) AcquireRequestFlag::kNoGc |
                       (flag_t) AcquireRequestFlag::kWithAllocation |
                       (flag_t) AcquireRequestFlag::kNoBindPR;
        auto handle =
            rdma_adpt->acquire_perm(nullgaddr, 0 /* hint */, 64, 0ns, ac_flag);

        auto rdma_buf = rdma_adpt->get_rdma_buffer(128);
        DCHECK_GE(rdma_buf.size, 128);
        // expect okay
        {
            auto rc = rdma_adpt->rdma_write(
                handle.gaddr(), rdma_buf.buffer, 64, handle);
            CHECK_EQ(rc, kOk);
            rc = rdma_adpt->commit();
            CHECK_EQ(rc, kOk);
        }
        // expact okay again
        {
            // write with offset 32 and length 32 (till the end)
            auto rc = rdma_adpt->rdma_write(
                handle.gaddr() + 32, rdma_buf.buffer + 32, 32, handle);
            CHECK_EQ(rc, kOk);
            rc = rdma_adpt->commit();
            CHECK_EQ(rc, kOk);
        }

        // expect protection error
        // write one more byte
        {
            auto rc1 = rdma_adpt->rdma_write(
                handle.gaddr(), rdma_buf.buffer, 65, handle);
            auto rc2 = rdma_adpt->commit();
            if (rc1 == kOk && rc2 == kOk)
            {
                CHECK(false) << "** Expect to get rdma protection error";
            }
            else
            {
                CHECK(rc1 == kRdmaProtectionErr || rc2 == kRdmaProtectionErr)
                    << "** Expect to get rdma protection error. rc1: " << rc1
                    << ", rc2: " << rc2;
            }
        }
        // expect protection error
        // write one 64 byte but with additional offset 1
        {
            auto rc1 = rdma_adpt->rdma_write(
                handle.gaddr() + 1, rdma_buf.buffer, 64, handle);
            auto rc2 = rdma_adpt->commit();
            if (rc1 == kOk && rc2 == kOk)
            {
                CHECK(false) << "** Expect to get rdma protection error";
            }
            else
            {
                CHECK(rc1 == kRdmaProtectionErr || rc2 == kRdmaProtectionErr)
                    << "** Expect to get rdma protection error. rc1: " << rc1
                    << ", rc2: " << rc2;
            }
        }

        // auto rel_flag = (flag_t) LeaseModifyFlag::kWaitUntilSuccess;
        auto rel_flag = (flag_t) 0;
        rdma_adpt->relinquish_perm(handle, 0 /* hint */, rel_flag);

        rdma_adpt->put_all_rdma_buffer();
    }

    // checking remote_alloc & remote_dealloc (separate allocation & binding)
    {
        std::vector<GlobalAddress> gaddrs;
        gaddrs.reserve(10240);  // don't know
        while (true)
        {
            auto gaddr = rdma_adpt->remote_alloc(kAllocSize, kAllocHintA);
            if (gaddr.is_null())
            {
                LOG(INFO) << "[bench] remote_alloc got nullptr: run out of "
                             "memory. allocated "
                          << gaddrs.size() << " nr, i.e. "
                          << gaddrs.size() * kAllocSize << " B.";
                break;
            }
            gaddrs.push_back(gaddr);
        }

        size_t check_size = 0;
        for (const auto &gaddr : gaddrs)
        {
            // should be writable
            auto ac_flag = (flag_t) AcquireRequestFlag::kNoGc;
            auto handle = rdma_adpt->acquire_perm(
                gaddr, 0 /* hint */, kAllocSize, 0ns, ac_flag);
            auto rdma_w_buf = rdma_adpt->get_rdma_buffer(kAllocSize);
            CHECK_GE(rdma_w_buf.size, kAllocSize);
            fast_pseudo_fill_buf(rdma_w_buf.buffer, kAllocSize);
            auto rc = rdma_adpt->rdma_write(
                gaddr, rdma_w_buf.buffer, kAllocSize, handle);
            CHECK_EQ(rc, kOk);
            rc = rdma_adpt->commit();
            CHECK_EQ(rc, kOk);

            // read and see can we read it back
            auto rdma_r_buf = rdma_adpt->get_rdma_buffer(kAllocSize);
            CHECK_GE(rdma_r_buf.size, kAllocSize);
            rc = rdma_adpt->rdma_read(
                rdma_r_buf.buffer, gaddr, kAllocSize, handle);
            CHECK_EQ(rc, kOk);
            rc = rdma_adpt->commit();
            CHECK_EQ(rc, kOk);
            CHECK_EQ(memcmp(rdma_w_buf.buffer, rdma_r_buf.buffer, kAllocSize),
                     0);

            auto rel_flag = 0;
            rdma_adpt->relinquish_perm(handle, 0 /* hint */, rel_flag);
            rdma_adpt->put_all_rdma_buffer();

            if (check_size++ >= 1_K)
            {
                // don't check too much
                // don't have this time.
                break;
            }
        }

        for (const auto &gaddr : gaddrs)
        {
            rdma_adpt->remote_free(gaddr, kAllocSize, kAllocHintA);
        }
    }

    // validating if these two address is consistent
    // a) server: patronus_->patronus_alloc() => patronus_->to_exposed_gaddr()
    // b) client: rdma_adpt_->read(gaddr);
    {
        auto test_gaddr = p->get_object<GlobalAddress>("p:test_gaddr", 1ms);
        auto handle =
            rdma_adpt->acquire_perm(test_gaddr,
                                    0,
                                    sizeof(Test),
                                    0ns,
                                    (flag_t) AcquireRequestFlag::kNoGc |
                                        (flag_t) AcquireRequestFlag::kNoBindPR);
        auto test_buf = rdma_adpt->get_rdma_buffer(sizeof(Test));
        rdma_adpt->rdma_read(test_buf.buffer, test_gaddr, sizeof(Test), handle)
            .expect(RC::kOk);
        rdma_adpt->commit().expect(RC::kOk);
        const auto &got_test = *(Test *) test_buf.buffer;
        CHECK_EQ(got_test.inited, 1) << got_test;
        CHECK_EQ(got_test.magic, kMagic) << got_test;
        CHECK_EQ(got_test.test_gaddr, test_gaddr) << got_test;

        rdma_adpt->relinquish_perm(handle, 0, 0);
    }

    exe_ctx.worker_finished(coro_id);
    ctx.yield_to_master();
}

void client_master(Patronus::pointer p,
                   CoroYield &yield,
                   CoroExecutionContext<kCoroCnt> &exe_ctx)
{
    auto tid = p->get_thread_id();

    CoroContext mctx(tid, &yield, exe_ctx.workers());
    CHECK(mctx.is_master());

    for (size_t i = 0; i < kCoroCnt; ++i)
    {
        mctx.yield_to_worker(i);
    }

    LOG(INFO) << "Return back to master. start to recv messages";
    coro_t coro_buf[2 * kCoroCnt];
    while (!exe_ctx.is_finished_all())
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

void client(Patronus::pointer p)
{
    CoroExecutionContext<kCoroCnt> coro_exe_ctx;

    auto tid = p->get_thread_id();
    LOG(INFO) << "I am client. tid " << tid;
    for (size_t i = 0; i < kCoroCnt; ++i)
    {
        coro_exe_ctx.worker(i) =
            CoroCall([p, i, &coro_exe_ctx](CoroYield &yield) {
                client_worker(p, i, yield, coro_exe_ctx);
            });
    }
    auto &master = coro_exe_ctx.master();
    master = CoroCall([p, &coro_exe_ctx](CoroYield &yield) {
        client_master(p, yield, coro_exe_ctx);
    });

    master();
}
void server(Patronus::pointer p)
{
    auto tid = p->get_thread_id();
    LOG(INFO) << "[bench] server starts to work. tid " << tid;

    void *addr;
    void *test_addr;
    {
        addr = p->patronus_alloc(4_KB, 0 /* hint */);
        auto gaddr = p->to_exposed_gaddr(addr);
        p->put("p:gaddr", gaddr, 0ns);
        LOG(INFO) << "Allocated " << (void *) addr << ". gaddr: " << gaddr;
    }

    auto default_allocator = p->get_allocator(0);
    {
        // test_addr = p->patronus_alloc(sizeof(Test), 0 /* hint */);
        test_addr = default_allocator->alloc(sizeof(Test), nullptr);
        auto test_gaddr = p->to_exposed_gaddr(test_addr);
        auto &test_obj = *(Test *) test_addr;

        auto *magic = (std::atomic<uint64_t> *) &test_obj.magic;
        magic->store(kMagic);
        auto *init = (std::atomic<uint64_t> *) &test_obj.inited;
        init->store(1);
        auto *test_obj_gaddr = (std::atomic<uint64_t> *) &test_obj.test_gaddr;
        test_obj_gaddr->store(test_gaddr.val);

        p->put("p:test_gaddr", test_gaddr, 0ns);
        LOG(INFO) << "Allocated for test: " << (void *) test_addr
                  << ". gaddr: " << test_gaddr;
    }

    // register allocator for kAllocHintA
    auto user_reserved_buf = p->get_user_reserved_buffer();
    mem::SlabAllocatorConfig slab_conf;
    slab_conf.block_class = {kAllocSize};
    slab_conf.block_ratio = {1};
    auto slab_allocator = mem::SlabAllocator::new_instance(
        user_reserved_buf.buffer, user_reserved_buf.size, slab_conf);
    p->reg_allocator(kAllocHintA, slab_allocator);

    p->keeper_barrier("ready", 100ms);

    p->server_serve(kWaitKey);

    p->patronus_free(addr, 4_KB, 0 /* hint */);
    default_allocator->free(test_addr, nullptr);
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    PatronusConfig config;
    config.machine_nr = ::config::kMachineNr;
    config.alloc_buffer_size = 16_MB;
    config.block_class = {4_KB};
    config.block_ratio = {1.0};

    auto patronus = Patronus::ins(config);
    auto nid = patronus->get_node_id();
    if (::config::is_client(nid))
    {
        patronus->registerClientThread();
        patronus->keeper_barrier("begin", 100ms);
        client(patronus);
        patronus->finished(kWaitKey);
    }
    else
    {
        patronus->registerServerThread();
        patronus->finished(kWaitKey);
        patronus->keeper_barrier("begin", 100ms);
        server(patronus);
    }
    LOG(INFO) << "finished. ctrl+C to quit.";
}