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
            client_m_.register_init(
                [p = patronus_]() { p->registerClientThread(); });
            client_m_.register_node_barrier([this]() { bar_.wait(); });
            client_m_.register_cluster_barrier(
                [p = patronus_](const std::string &key) {
                    p->keeper_barrier(key, 100ms);
                });

            client_m_.register_master_coro(
                [p = patronus_, this](CoroYield &yield,
                                      CoroCall *workers,
                                      Context &context,
                                      CoroComm &coro_context,
                                      const Config &config) {
                    patronus_client_master_coro(
                        yield, workers, context, coro_context, config);
                });
        }
        else
        {
            server_m_.register_init(
                [p = patronus_]() { p->registerServerThread(); });
            server_m_.register_node_barrier([this]() { bar_.wait(); });
            server_m_.register_cluster_barrier(
                [p = patronus_](const std::string &key) {
                    p->keeper_barrier(key, 100ms);
                });
        }
    }

    using ServerTaskF =
        std::function<void(Patronus::pointer, Context &, const Config &)>;
    void register_server_task(ServerTaskF task_f)
    {
        server_m_.register_bench([p = patronus_, task_f](Context &context,
                                                         const Config &config,
                                                         bool is_master) {
            std::ignore = is_master;
            task_f(p, context, config);
        });
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
                auto tid = p->get_thread_id();
                CHECK(!finish_all_task_[coro_id]);
                CoroContext ctx(tid, &yield, master, coro_id);

                task_f(p, context, coro_comm, config, ctx, is_master);
                finish_all_task_[coro_id] = true;
                ctx.yield_to_master();
                LOG(FATAL) << "** not reachable.";
            });
    }

    void patronus_client_master_coro(CoroYield &yield,
                                     CoroCall *workers,
                                     Context &context,
                                     CoroComm &coro_context,
                                     const Config &config)
    {
        std::ignore = context;
        std::ignore = coro_context;
        std::ignore = config;

        auto tid = patronus_->get_thread_id();

        CoroContext mctx(tid, &yield, workers);
        CHECK(mctx.is_master());

        finish_all_task_.clear();
        finish_all_task_.resize(coro_nr_, false);

        for (size_t i = 0; i < coro_nr_; ++i)
        {
            mctx.yield_to_worker(i);
        }
        LOG(INFO) << "Return back to master. start to recv messages";
        coro_t coro_buf[2 * define::kMaxCoroNr];
        while (!std::all_of(std::begin(finish_all_task_),
                            std::end(finish_all_task_),
                            [](bool i) { return i; }))
        {
            auto nr = patronus_->try_get_client_continue_coros(
                coro_buf, 2 * define::kMaxCoroNr);
            for (size_t i = 0; i < nr; ++i)
            {
                auto coro_id = coro_buf[i];
                DVLOG(1) << "[bench] yielding due to CQE";
                mctx.yield_to_worker(coro_id);
            }
        }

        LOG(WARNING) << "[bench] all worker finish their work. at tid " << tid
                     << " thread exiting...";
    }
    using PostSubBenchF = typename CoroManagerT::PostSubBenchF;
    void register_post_sub_bench(PostSubBenchF &post_sub_bench_f)
    {
        if (is_client_)
        {
            client_m_.register_post_sub_bench(post_sub_bench_f);
        }
        else
        {
            server_m_.register_post_sub_bench(post_sub_bench_f);
        }
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

    std::vector<bool> finish_all_task_;
};
}  // namespace patronus::bench