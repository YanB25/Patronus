#pragma once
#include "./manager.h"
#include "CoroContext.h"

namespace bench
{
template <typename Context, typename Config, typename CoroComm>
class CoroManager
{
public:
    constexpr static size_t V = config::verbose::kCoroLauncher;

    using ManagerT = Manager<Context, Config>;
    using InitF = typename ManagerT::InitF;
    CoroManager(size_t thread_nr, size_t coro_nr, const Context &context = {})
        : m_(thread_nr, context), coro_nr_(coro_nr)
    {
        CHECK_LE(coro_nr, define::kMaxCoroNr);
        CHECK_LE(thread_nr, kMaxAppThread);
    }
    void register_init(const InitF &init_f)
    {
        m_.register_init(init_f);
    }
    using NodeBarrier = typename ManagerT::NodeBarrier;
    void register_node_barrier(const NodeBarrier &node_barrier_f)
    {
        m_.register_node_barrier(node_barrier_f);
    }
    using ClusterBarrier = typename ManagerT::ClusterBarrier;
    void register_cluster_barrier(const ClusterBarrier &cluster_barrier_f)
    {
        m_.register_cluster_barrier(cluster_barrier_f);
    }

    using MasterF = std::function<void(
        CoroYield &, CoroCall *, Context &, CoroComm &, const Config &)>;
    void register_master_coro(const MasterF &master_f)
    {
        master_f_ = master_f;
    }

    using WorkerF = std::function<void(size_t,
                                       CoroYield &,
                                       CoroCall *,
                                       Context &,
                                       CoroComm &,
                                       const Config &,
                                       bool)>;
    void register_worker_coro(const WorkerF &worker_f)
    {
        worker_f_ = worker_f;
    }

    using PostSubBenchF = typename ManagerT::PostSubBenchF;
    void register_post_sub_bench(PostSubBenchF &post_sub_bench_f)
    {
        m_.register_post_sub_bench(post_sub_bench_f);
    }

    void bench(const std::vector<Config> &configs)
    {
        m_.register_bench(
            [this](Context &context, const Config &config, bool is_master) {
                do_bench_thread(context, config, is_master);
            });
        m_.bench(configs);
    }

    void do_bench_thread(Context &context, const Config &config, bool is_master)
    {
        VLOG(V) << "[coro_manager] entering do_bench_thread: start of coro "
                   "lifecycle";
        CoroComm coro_context;
        CoroCall workers[define::kMaxCoroNr];
        CoroCall master;
        for (size_t i = 0; i < coro_nr_; ++i)
        {
            workers[i] = CoroCall(
                [i, &master, &context, &coro_context, &config, is_master, this](
                    CoroYield &yield) {
                    bench_worker_coro(i,
                                      yield,
                                      &master,
                                      context,
                                      coro_context,
                                      config,
                                      is_master);
                });
        }
        master = CoroCall([&workers, &context, &coro_context, &config, this](
                              CoroYield &yield) {
            bench_master_coro(yield, workers, context, coro_context, config);
        });

        master();
        VLOG(V) << "[coro_manager] leaving do_bench_thread: end of coro "
                   "lifecycle";
    }

    void bench_master_coro(CoroYield &yield,
                           CoroCall *workers,
                           Context &context,
                           CoroComm &coro_context,
                           const Config &config)
    {
        VLOG(V) << "[coro_manager] entering master coro";
        master_f_(yield, workers, context, coro_context, config);
        VLOG(V) << "[coro_manager] leaving master coro";
    }

    void bench_worker_coro(size_t coro_id,
                           CoroYield &yield,
                           CoroCall *master,
                           Context &context,
                           CoroComm &coro_context,
                           const Config &config,
                           bool is_master)
    {
        VLOG(V) << "[coro_manager] entering worker coro(" << coro_id << ")";
        worker_f_(
            coro_id, yield, master, context, coro_context, config, is_master);
        VLOG(V) << "[coro_manager] leaving worker coro(" << coro_id << ")";
    }

private:
    ManagerT m_;
    size_t coro_nr_;
    MasterF master_f_;
    WorkerF worker_f_;
};
}  // namespace bench