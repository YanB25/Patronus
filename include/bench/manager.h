#pragma once
#include <functional>
#include <thread>

#include "Common.h"
#include "util/Pre.h"

namespace bench
{
/**
 * workflow
 *
 * init() - called once, at very begin
 *
 * start_bench() - called once, at each config
 * bench() - called each thread, a each config
 * end_bench() - called once, at each config
 *
 */
template <typename Context, typename Config>
class Manager
{
public:
    constexpr static size_t V = config::verbose::kCoroLauncher;
    using InitF = std::function<void(Context &)>;
    Manager(size_t thread_nr) : thread_nr_(thread_nr)
    {
    }
    void register_init(const InitF &init_f)
    {
        has_init_ = true;
        init_f_ = init_f;
    }

    using BenchF = std::function<void(Context &, const Config &, bool)>;
    void register_bench(const BenchF &bench_f)
    {
        has_bench_ = true;
        bench_f_ = bench_f;
    }

    using HookF = std::function<void(Context &, const Config &)>;
    void register_start_bench(const HookF &hook_f)
    {
        start_bench_f_ = hook_f;
    }
    void register_end_bench(const HookF &hook_f)
    {
        end_bench_f_ = hook_f;
    }

    using NodeBarrier = std::function<void()>;
    using ClusterBarrier = std::function<void(const std::string &)>;

    void register_node_barrier(const NodeBarrier &node_barrier_f)
    {
        has_node_barrier_ = true;
        node_barrier_f_ = node_barrier_f;
    }
    void register_cluster_barrier(const ClusterBarrier &cluster_barrier_f)
    {
        has_cluster_barrier_ = true;
        cluster_barrier_f_ = cluster_barrier_f;
    }

    void bench(const std::vector<Config> &configs)
    {
        std::vector<std::thread> threads;
        for (size_t i = 1; i < thread_nr_; ++i)
        {
            threads.emplace_back(
                [&configs, this]() { bench_thread(configs, false); });
        }
        bench_thread(configs, true);

        for (auto &t : threads)
        {
            t.join();
        }
    }
    template <typename T, typename U>
    friend std::ostream &operator<<(std::ostream &, Manager<T, U>);

private:
    size_t thread_nr_;
    Context context_;
    bool has_init_{false};
    InitF init_f_;
    bool has_bench_{false};
    BenchF bench_f_;
    bool has_node_barrier_{false};
    NodeBarrier node_barrier_f_;
    bool has_cluster_barrier_{false};
    ClusterBarrier cluster_barrier_f_;

    std::optional<HookF> start_bench_f_;
    std::optional<HookF> end_bench_f_;

    void do_bench_thread(const Config &config, Context &context, bool is_master)
    {
        VLOG(V) << "[manager] Entering bench_f...";
        bench_f_(context, config, is_master);
        VLOG(V) << "[manager] Leaving bench_f...";
    }

    void bench_thread(const std::vector<Config> &configs, bool is_master)
    {
        static size_t __times = 0;
        VLOG(V) << "[manager] initing";
        init_f_(context_);

        node_barrier_f_();

        if (is_master)
        {
            cluster_barrier_f_("manager:enter");
        }

        for (const auto &config : configs)
        {
            if (is_master)
            {
                auto name =
                    std::string("manager:run-") + std::to_string(__times);
                __times++;
                cluster_barrier_f_(name);
            }

            node_barrier_f_();

            if (is_master && start_bench_f_.has_value())
            {
                start_bench_f_.value()(context_, config);
            }

            VLOG(V) << "[manager] entering benchmark";
            do_bench_thread(config, context_, is_master);
            VLOG(V) << "[manager] leaving benchmark, waiting for node barrier";
            node_barrier_f_();
            VLOG(V) << "[manager] node barrier leaved.";

            if (is_master && end_bench_f_.has_value())
            {
                end_bench_f_.value()(context_, config);
            }

            node_barrier_f_();
        }

        if (is_master)
        {
            cluster_barrier_f_("manager:leave");
        }
    }
};

template <typename Context, typename Config>
inline std::ostream &operator<<(std::ostream &os,
                                const Manager<Context, Config> &m)
{
    os << "{manager has_init: " << m.has_init_
       << ", has_bench: " << m.has_bench_
       << ", has_node_barrier: " << m.has_node_barrier_
       << ", has_cluster_barrier: " << m.has_cluster_barrier_ << "}";
    return os;
}
}  // namespace bench