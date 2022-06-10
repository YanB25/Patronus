#include <algorithm>
#include <random>
#include <set>

#include "Common.h"
#include "boost/thread/barrier.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "patronus/Patronus.h"
#include "patronus/RdmaAdaptor.h"
#include "patronus/memory/direct_allocator.h"
#include "patronus/memory/mr_allocator.h"
#include "thirdparty/linked-list/conf.h"
#include "thirdparty/linked-list/list.h"
#include "thirdparty/linked-list/list_handle.h"
#include "util/BenchRand.h"
#include "util/DataFrameF.h"
#include "util/Pre.h"
#include "util/Rand.h"
#include "util/TimeConv.h"

using namespace patronus::list;
using namespace define::literals;
using namespace patronus;
using namespace hmdf;

constexpr static size_t kServerThreadNr = NR_DIRECTORY;
constexpr static size_t kClientThreadNr = 1;
constexpr static size_t kMaxCoroNr = 1;

DEFINE_string(exec_meta, "", "The meta data of this execution");

struct ListItem
{
    uint64_t nid;
    uint64_t tid;
    uint64_t coro_id;
    uint64_t magic_number;
};

inline std::ostream &operator<<(std::ostream &os, const ListItem &item)
{
    os << "{ListItem nid: " << item.nid << ", tid: " << item.tid
       << ", coro_id: " << item.coro_id
       << ", magic_number: " << (void *) item.magic_number << "}";
    return os;
}

struct BenchConfig
{
    std::string name;
    size_t thread_nr{1};
    size_t coro_nr{1};
    size_t test_nr{0};
    bool should_report{false};

    bool should_report_lat() const
    {
        return should_report && thread_nr == 32 && coro_nr == 1;
    }

    void validate() const
    {
    }
    size_t cluster_client_nr() const
    {
        return local_client_nr() * ::config::get_client_nids().size();
    }
    size_t local_client_nr() const
    {
        return thread_nr * coro_nr;
    }
    std::string conf_name() const
    {
        return name;
    }

    BenchConfig clone() const
    {
        BenchConfig ret;
        ret.name = name;
        ret.thread_nr = thread_nr;
        ret.coro_nr = coro_nr;
        ret.test_nr = test_nr;
        ret.should_report = should_report;
        return ret;
    }
    static BenchConfig get_conf(const std::string &name,
                                size_t test_nr,
                                size_t thread_nr,
                                size_t coro_nr)
    {
        BenchConfig conf;
        conf.name = name;
        conf.test_nr = test_nr;
        conf.thread_nr = thread_nr;
        conf.coro_nr = coro_nr;
        conf.test_nr = test_nr;
        conf.should_report = true;

        conf.validate();
        return conf;
    }
};
std::ostream &operator<<(std::ostream &os, const BenchConfig &conf)
{
    os << "{conf: name: " << conf.name << ", thread_nr: " << conf.thread_nr
       << ", coro_nr: " << conf.coro_nr
       << "(local_client_nr: " << conf.local_client_nr()
       << ", cluster: " << conf.cluster_client_nr()
       << "), test_nr: " << conf.test_nr
       << ", should_report: " << conf.should_report << "}";
    return os;
}

template <typename T>
typename ListHandle<T>::pointer gen_handle(Patronus::pointer p,
                                           size_t dir_id,
                                           const HandleConfig &conf,
                                           GlobalAddress meta_gaddr,
                                           CoroContext *ctx)
{
    auto server_nid = ::config::get_server_nids().front();

    DVLOG(1) << "Getting from race:meta_gaddr got " << meta_gaddr;

    auto rdma_adpt = patronus::RdmaAdaptor::new_instance(
        server_nid, dir_id, p, conf.bypass_prot, ctx);

    auto handle =
        ListHandle<T>::new_instance(server_nid, meta_gaddr, rdma_adpt, conf);

    // debug
    handle->read_meta();
    LOG(INFO) << "[debug] !! meta is " << handle->cached_meta();
    return handle;
}

struct AdditionalCoroCtx
{
    ssize_t thread_remain_task{0};
    size_t put_nr{0};
    size_t put_succ_nr{0};
    size_t get_nr{0};
    size_t get_succ_nr{0};
    size_t rdma_protection_nr{0};
};

template <typename T>
typename List<T>::pointer gen_list(Patronus::pointer p,
                                   const ListConfig &config)
{
    auto allocator = p->get_allocator(0 /* default hint */);

    auto server_rdma_adpt = patronus::RdmaAdaptor::new_instance(p);
    auto list = List<T>::new_instance(server_rdma_adpt, allocator, config);

    auto meta_gaddr = list->meta_gaddr();
    p->put("race:meta_gaddr", meta_gaddr, 0ns);
    LOG(INFO) << "Puting to race:meta_gaddr with " << meta_gaddr;

    LOG(INFO) << "[debug] !! meta is " << list->meta();
    return list;
}

void validate_helper(const std::list<ListItem> &reference,
                     const std::list<ListItem> &test)
{
    CHECK_EQ(reference.size(), test.size())
        << "[debug] size mismatch. " << util::pre_iter(reference, 10)
        << " v.s. " << util::pre_iter(test, 10);

    auto ref_it = reference.cbegin();
    auto test_it = test.cbegin();
    size_t ith = 0;
    while (ref_it != reference.cend())
    {
        if (test_it == test.cend())
        {
            LOG(FATAL) << "Size mismatch. test reach tail at " << ith << "-th";
        }
        auto reference_magic = ref_it->magic_number;
        auto test_magic = test_it->magic_number;
        CHECK_EQ(reference_magic, test_magic);
        ref_it++;
        test_it++;
        ith++;
    }
    if (test_it != test.cend())
    {
        LOG(FATAL) << "Size mismatch. reference reaches tail at " << ith
                   << "-th, but test is not.";
    }
}

void test_basic_client_worker(
    Patronus::pointer p,
    size_t coro_id,
    CoroYield &yield,
    const BenchConfig &bench_conf,
    const HandleConfig &handle_conf,
    GlobalAddress meta_gaddr,
    CoroExecutionContextWith<kMaxCoroNr, AdditionalCoroCtx> &ex)
{
    std::ignore = bench_conf;

    auto nid = p->get_node_id();
    auto tid = p->get_thread_id();
    auto dir_id = tid % kServerThreadNr;
    CoroContext ctx(tid, &yield, &ex.master(), coro_id);

    auto handle =
        gen_handle<ListItem>(p, dir_id, handle_conf, meta_gaddr, &ctx);

    size_t put_succ_nr = 0;
    size_t put_nomem_nr = 0;
    size_t get_succ_nr = 0;
    size_t get_nomem_nr = 0;
    size_t rdma_protection_nr = 0;
    size_t executed_nr = 0;

    std::list<ListItem> book_list;

    CHECK_EQ(handle->lk_pop_front(nullptr), kNotFound)
        << "** list empty, should be unable to pop";

    while (ex.get_private_data().thread_remain_task > 0)
    {
        if (true_with_prob(0.2))
        {
            // pop
            ListItem item;
            LOG(INFO) << "[debug] POP ";
            auto rc = handle->lk_pop_front(&item);
            if (book_list.empty())
            {
                CHECK_EQ(rc, kNotFound)
                    << "** list empty, should be unable to pop";
            }
            else
            {
                CHECK_EQ(rc, kOk);
                auto front_item = book_list.front();
                book_list.pop_front();
                CHECK_EQ(front_item.magic_number, item.magic_number);
            }
        }
        else
        {
            // push
            ListItem item;
            item.nid = nid;
            item.tid = tid;
            item.coro_id = coro_id;
            item.magic_number = fast_pseudo_rand_int();
            book_list.push_back(item);
            LOG(INFO) << "[debug] PUSH " << item;
            auto rc = handle->lk_push_back(item);
            CHECK_EQ(rc, kOk);
        }
        auto lists = handle->debug_iterator();
        validate_helper(book_list, lists);

        ex.get_private_data().thread_remain_task--;
    }

    auto &comm = ex.get_private_data();
    comm.put_succ_nr += put_succ_nr;
    comm.put_nr += put_succ_nr + get_succ_nr;
    comm.get_succ_nr += get_succ_nr;
    comm.get_nr += get_succ_nr + get_nomem_nr;
    comm.rdma_protection_nr += rdma_protection_nr;

    LOG(INFO) << "[bench] put: succ: " << put_succ_nr
              << ", nomem: " << put_nomem_nr << ", get: succ: " << get_succ_nr
              << ", not found: " << get_nomem_nr
              << ", rdma_protection_nr: " << rdma_protection_nr
              << ". executed: " << executed_nr << ". ctx: " << ctx;

    handle.reset();

    ex.worker_finished(coro_id);
    ctx.yield_to_master();
}

void test_basic_client_master(
    Patronus::pointer p,
    CoroYield &yield,
    size_t test_nr,
    std::atomic<ssize_t> &atm_task_nr,
    size_t coro_nr,
    CoroExecutionContextWith<kMaxCoroNr, AdditionalCoroCtx> &ex)
{
    auto tid = p->get_thread_id();

    CoroContext mctx(tid, &yield, ex.workers());
    CHECK(mctx.is_master());

    ssize_t task_per_sync = test_nr / 100;
    LOG_IF(WARNING, task_per_sync <= (ssize_t) coro_nr);
    task_per_sync = std::max(task_per_sync, ssize_t(coro_nr));
    ssize_t remain =
        atm_task_nr.fetch_sub(task_per_sync, std::memory_order_relaxed) -
        task_per_sync;
    ex.get_private_data().thread_remain_task = task_per_sync;

    for (size_t i = coro_nr; i < kMaxCoroNr; ++i)
    {
        ex.worker_finished(i);
    }

    for (size_t i = 0; i < coro_nr; ++i)
    {
        mctx.yield_to_worker(i);
    }

    coro_t coro_buf[2 * kMaxCoroNr];
    while (true)
    {
        if ((ssize_t) ex.get_private_data().thread_remain_task <=
            2 * ssize_t(coro_nr))
        {
            // refill the thread_remain_task
            auto cur_task_nr = std::min(remain, task_per_sync);
            if (cur_task_nr >= 0)
            {
                remain = atm_task_nr.fetch_sub(cur_task_nr,
                                               std::memory_order_relaxed) -
                         cur_task_nr;
                if (remain >= 0)
                {
                    ex.get_private_data().thread_remain_task += cur_task_nr;
                }
            }
        }
        auto nr = p->try_get_client_continue_coros(coro_buf, 2 * kMaxCoroNr);
        for (size_t i = 0; i < nr; ++i)
        {
            auto coro_id = coro_buf[i];
            // DVLOG(1) << "[bench] yielding due to CQE: " << (int) coro_id;
            mctx.yield_to_worker(coro_id);
        }

        if (remain <= 0)
        {
            if (ex.is_finished_all())
            {
                CHECK_LE(remain, 0);
                break;
            }
        }
    }
}

std::atomic<ssize_t> g_total_test_nr;
GlobalAddress g_meta_gaddr;
void benchmark_client(Patronus::pointer p,
                      boost::barrier &bar,
                      bool is_master,
                      const BenchConfig &bench_conf,
                      const HandleConfig &handle_conf,
                      uint64_t key)
{
    auto selected_client = ::config::get_client_nids().front();
    auto coro_nr = bench_conf.coro_nr;
    auto thread_nr = bench_conf.thread_nr;
    bool first_enter = true;
    bool server_should_leave = true;
    CHECK_LE(coro_nr, kMaxCoroNr);
    size_t actual_test_nr = bench_conf.test_nr;

    auto tid = p->get_thread_id();
    auto nid = p->get_node_id();
    if (is_master)
    {
        // init here by master
        g_total_test_nr = actual_test_nr;
        if (first_enter)
        {
            p->keeper_barrier("server_ready-" + std::to_string(key), 100ms);
            // fetch meta_gaddr here by master thread
            // because it may be slow
            g_meta_gaddr = p->get_object<GlobalAddress>("race:meta_gaddr", 1ms);
        }
    }
    bar.wait();

    // auto min = util::time::to_ns(0ns);
    // auto max = util::time::to_ns(1ms);
    // auto rng = util::time::to_ns(1us);
    // OnePassBucketMonitor lat_m(min, max, rng);

    ChronoTimer timer;
    CoroExecutionContextWith<kMaxCoroNr, AdditionalCoroCtx> ex;
    bool should_enter = tid < thread_nr && nid == selected_client;

    if (should_enter)
    {
        ex.get_private_data().thread_remain_task = 0;
        for (size_t i = coro_nr; i < kMaxCoroNr; ++i)
        {
            // no that coro, so directly finished.
            ex.worker_finished(i);
        }
        for (size_t i = 0; i < coro_nr; ++i)
        {
            ex.worker(
                i) = CoroCall([p,
                               coro_id = i,
                               &bench_conf,
                               &handle_conf,
                               &ex,
                               meta_gaddr = g_meta_gaddr](CoroYield &yield) {
                test_basic_client_worker(
                    p, coro_id, yield, bench_conf, handle_conf, meta_gaddr, ex);
            });
        }
        auto &master = ex.master();
        master = CoroCall([p, &ex, actual_test_nr = actual_test_nr, coro_nr](
                              CoroYield &yield) {
            test_basic_client_master(
                p, yield, actual_test_nr, g_total_test_nr, coro_nr, ex);
        });

        master();
    }

    bar.wait();
    auto ns = timer.pin();

    double ops = 1e9 * actual_test_nr / ns;
    double avg_ns = 1.0 * ns / actual_test_nr;
    LOG_IF(INFO, is_master)
        << "[bench] total op: " << actual_test_nr << ", ns: " << ns
        << ", ops: " << ops << ", avg " << avg_ns << " ns";

    if (is_master && server_should_leave)
    {
        LOG(INFO) << "p->finished(" << key << ")";
        p->finished(key);
    }
}

void benchmark_server(Patronus::pointer p,
                      boost::barrier &bar,
                      bool is_master,
                      const std::vector<BenchConfig> &confs,
                      const ListConfig &list_config,
                      uint64_t key)
{
    auto thread_nr = confs[0].thread_nr;
    for (const auto &conf : confs)
    {
        thread_nr = std::max(thread_nr, conf.thread_nr);
    }

    typename List<ListItem>::pointer list;

    if (is_master)
    {
        p->finished(key);
        list = gen_list<ListItem>(p, list_config);
    }
    // wait for everybody to finish preparing
    bar.wait();
    if (is_master)
    {
        p->keeper_barrier("server_ready-" + std::to_string(key), 100ms);
    }

    p->server_serve(key);
    bar.wait();
}

void benchmark(Patronus::pointer p, boost::barrier &bar, bool is_client)
{
    uint64_t key = 0;
    bool is_master = p->get_thread_id() == 0;
    bar.wait();

    std::vector<HandleConfig> handle_configs;
    LOG(WARNING) << "TODO: set up handle config well";
    handle_configs.push_back(HandleConfig{});

    for (const auto &handle_conf : handle_configs)
    {
        LOG_IF(INFO, is_master)
            << "[bench] benching multiple threads for " << handle_conf;
        key++;
        auto conf = BenchConfig::get_conf(
            "default", 1000, 1 /* thread_nr */, 1 /* coro_nr */);
        if (is_client)
        {
            conf.validate();
            LOG_IF(INFO, is_master) << "[sub-conf] running conf: " << conf;
            benchmark_client(p, bar, is_master, conf, handle_conf, key);
        }
        else
        {
            // TODO: let list config be real.
            benchmark_server(p, bar, is_master, {conf}, ListConfig{}, key);
        }
    }
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    PatronusConfig pconfig;
    pconfig.machine_nr = ::config::kMachineNr;
    pconfig.block_class = {32_MB, 2_MB, 4_KB};
    pconfig.block_ratio = {0.1, 0.5, 0.4};
    pconfig.reserved_buffer_size = 2_GB;
    pconfig.lease_buffer_size = (kDSMCacheSize - 2_GB) / 2;
    pconfig.alloc_buffer_size = (kDSMCacheSize - 2_GB) / 2;

    auto patronus = Patronus::ins(pconfig);

    std::vector<std::thread> threads;
    // boost::barrier bar(kThreadNr);
    auto nid = patronus->get_node_id();

    bool is_client = ::config::is_client(nid);

    if (is_client)
    {
        patronus->registerClientThread();
    }
    else
    {
        patronus->registerServerThread();
    }

    LOG(INFO) << "[bench] " << pre_patronus_explain(*patronus);

    if (is_client)
    {
        boost::barrier bar(kClientThreadNr);
        for (size_t i = 0; i < kClientThreadNr - 1; ++i)
        {
            threads.emplace_back([patronus, &bar]() {
                patronus->registerClientThread();
                bar.wait();
                benchmark(patronus, bar, true);
            });
        }
        bar.wait();
        patronus->keeper_barrier("begin", 10ms);
        benchmark(patronus, bar, true);

        for (auto &t : threads)
        {
            t.join();
        }
    }
    else
    {
        boost::barrier bar(kServerThreadNr);
        for (size_t i = 0; i < kServerThreadNr - 1; ++i)
        {
            threads.emplace_back([patronus, &bar]() {
                patronus->registerServerThread();
                bar.wait();
                benchmark(patronus, bar, false);
            });
        }
        bar.wait();
        patronus->keeper_barrier("begin", 100ms);
        benchmark(patronus, bar, false);

        for (auto &t : threads)
        {
            t.join();
        }
    }

    patronus->keeper_barrier("finished", 100ms);

    LOG(INFO) << "finished. ctrl+C to quit.";
}