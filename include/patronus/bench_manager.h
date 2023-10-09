#pragma once
#include <functional>

#include "./Patronus.h"
#include "CoroContext.h"
#include "bench/coro_manager.h"

namespace patronus::bench
{
template <typename Context, typename Config, typename CoroComm>
class PatronusManager
{
public:
    constexpr static size_t V = ::config::verbose::kCoroLauncher;
    using CoroManagerT = ::bench::CoroManager<Context, Config, CoroComm>;
    PatronusManager(Patronus::pointer patronus,
                    size_t thread_nr,
                    size_t coro_nr)
        : patronus_(patronus),
          client_m_(thread_nr, coro_nr),
          server_m_(thread_nr),
          thread_nr_(thread_nr),
          coro_nr_(coro_nr),
          bar_(thread_nr_)
    {
        auto nid = patronus_->get_node_id();
        if (::config::is_client(nid))
        {
            is_client_ = true;
        }
        else if (::config::is_server(nid))
        {
            is_client_ = false;
        }
        else
        {
            LOG(FATAL) << "** Unknown node_id: " << nid;
        }

        if (is_client_)
        {
            client_m_.register_node_barrier([this]() { bar_.wait(); });
            client_m_.register_cluster_barrier(
                [p = patronus_](const std::string &key) {
                    p->keeper_barrier(key, 100ms);
                });

            client_m_.register_init(
                [this](Context &context) { client_init_hook(context); });

            client_m_.register_master_coro(
                [p = patronus_, this](CoroYield &yield,
                                      CoroCall *workers,
                                      Context &context,
                                      CoroComm &coro_context,
                                      const Config &config) {
                    patronus_client_master_coro(
                        yield, workers, context, coro_context, config);
                });
            client_m_.register_start_bench(
                [this](Context &context, const Config &config) {
                    client_start_bench_hook(context, config);
                });
            client_m_.register_end_bench(
                [this](Context &context, const Config &config) {
                    client_end_bench_hook(context, config);
                });
        }
        else
        {
            server_m_.register_node_barrier([this]() { bar_.wait(); });
            server_m_.register_cluster_barrier(
                [p = patronus_](const std::string &key) {
                    p->keeper_barrier(key, 100ms);
                });

            server_m_.register_init(
                [this](Context &context) { server_init_hook(context); });
            server_m_.register_start_bench(
                [this](Context &context, const Config &config) {
                    server_start_bench_hook(context, config);
                });
            register_server_task(
                [this](Patronus::pointer p, Context &, const Config &) {
                    p->server_serve(wait_key_);
                });
            server_m_.register_end_bench(
                [this](Context &context, const Config &config) {
                    server_end_bench_hook(context, config);
                });
        }
    }

    using InitF = typename CoroManagerT::InitF;
    void register_init(const InitF init_f)
    {
        init_f_ = init_f;
    }

    using ServerTaskF =
        std::function<void(Patronus::pointer, Context &, const Config &)>;
    void register_server_task(ServerTaskF task_f)
    {
        server_m_.register_bench([p = patronus_, task_f](Context &context,
                                                         const Config &config,
                                                         bool is_master) {
            std::ignore = is_master;
            VLOG(V) << "[patronus_manager] entering task_f";
            task_f(p, context, config);
            VLOG(V) << "[patronus_manager] leaving task_f";
        });
    }

    using HookF = typename CoroManagerT::HookF;
    void register_start_bench(const HookF &hook_f)
    {
        start_bench_f_ = hook_f;
    }
    void register_end_bench(const HookF &hook_f)
    {
        end_bench_f_ = hook_f;
    }

    using TaskF = std::function<void(Patronus::pointer,
                                     Context &,
                                     CoroComm &,
                                     const Config &,
                                     CoroContext &ctx,
                                     bool)>;
    void register_task(TaskF task_f)
    {
        client_m_.register_worker_coro(
            [p = patronus_, task_f, this](size_t coro_id,
                                          CoroYield &yield,
                                          CoroCall *master,
                                          Context &context,
                                          CoroComm &coro_comm,
                                          const Config &config,
                                          bool is_master) {
                VLOG(V) << "[patronus_manager] entering worker coro.";
                auto tid = p->get_thread_id();
                auto &finish_all_task = finish_all_tasks_[tid];
                CHECK(!finish_all_task[coro_id]);
                CoroContext ctx(tid, &yield, master, coro_id);

                VLOG(V) << "[patronus_manager] entering task_f " << ctx;
                task_f(p, context, coro_comm, config, ctx, is_master);
                finish_all_task[coro_id] = true;
                VLOG(V) << "[patronus_manager] coro finishing all the tasks. "
                           "leaving... "
                        << ctx << ", " << util::pre_vec(finish_all_task);
                ctx.yield_to_master();
                LOG(FATAL) << "** not reachable.";
            });
    }

    void bench(const std::vector<Config> &configs)
    {
        if (is_client_)
        {
            client_m_.bench(configs);
        }
        else
        {
            server_m_.bench(configs);
        }
    }

private:
    Patronus::pointer patronus_;
    // for client
    ::bench::CoroManager<Context, Config, CoroComm> client_m_;
    // for server
    ::bench::Manager<Context, Config> server_m_;
    size_t thread_nr_;
    size_t coro_nr_;
    boost::barrier bar_;
    bool is_client_{false};

    std::optional<HookF> start_bench_f_;
    std::optional<HookF> end_bench_f_;
    std::optional<InitF> init_f_;

    Perthread<std::vector<bool>> finish_all_tasks_;

    uint64_t wait_key_{0};

    void server_init_hook(Context &context)
    {
        patronus_->registerServerThread();
        if (init_f_.has_value())
        {
            init_f_.value()(context);
        }
    }
    void client_init_hook(Context &context)
    {
        patronus_->registerClientThread();
        if (init_f_.has_value())
        {
            init_f_.value()(context);
        }
    }

    void server_start_bench_hook(Context &context, const Config &config)
    {
        if (start_bench_f_.has_value())
        {
            start_bench_f_.value()(context, config);
        }

        patronus_->finished(wait_key_);
    }
    void server_end_bench_hook(Context &context, const Config &config)
    {
        if (end_bench_f_.has_value())
        {
            end_bench_f_.value()(context, config);
        }
        wait_key_++;
    }

    void client_start_bench_hook(Context &context, const Config &config)
    {
        if (start_bench_f_.has_value())
        {
            start_bench_f_.value()(context, config);
        }
    }
    void client_end_bench_hook(Context &context, const Config &config)
    {
        if (end_bench_f_.has_value())
        {
            end_bench_f_.value()(context, config);
        }

        patronus_->finished(wait_key_);
        wait_key_++;
    }

    void patronus_client_master_coro(CoroYield &yield,
                                     CoroCall *workers,
                                     Context &context,
                                     CoroComm &coro_context,
                                     const Config &config)
    {
        VLOG(V) << "[patronus_manager] entering patronus_client_master_coro";
        std::ignore = context;
        std::ignore = coro_context;
        std::ignore = config;

        auto tid = patronus_->get_thread_id();

        CoroContext mctx(tid, &yield, workers);
        CHECK(mctx.is_master());

        auto &finish_all_task = finish_all_tasks_[tid];
        finish_all_task.clear();
        finish_all_task.resize(coro_nr_, false);

        for (size_t i = 0; i < coro_nr_; ++i)
        {
            VLOG(V) << "[patronus_manager] before yielding to worker " << i
                    << " for init. " << mctx;
            mctx.yield_to_worker(i);
        }
        coro_t coro_buf[2 * define::kMaxCoroNr];
        while (!std::all_of(std::begin(finish_all_task),
                            std::end(finish_all_task),
                            [](bool i) { return i; }))
        {
            auto nr = patronus_->try_get_client_continue_coros(
                coro_buf, 2 * define::kMaxCoroNr);
            for (size_t i = 0; i < nr; ++i)
            {
                auto coro_id = coro_buf[i];
                VLOG(V) << "[patronus_manager] before yielding to worker " << i
                        << " for CQE. " << mctx;
                mctx.yield_to_worker(coro_id);
            }
        }

        VLOG(V) << "[patronus_manager] master coro detects all work finished. "
                   "leaving... "
                << mctx;
    }
};
}  // namespace patronus::bench