#pragma once
#ifndef THIRD_PARTY_SERVERLESS_SERVERLESS_H_
#define THIRD_PARTY_SERVERLESS_SERVERLESS_H_

#include <cinttypes>
#include <functional>
#include <optional>

#include "./parameters.h"
#include "Common.h"
#include "CoroContext.h"
#include "GlobalAddress.h"
#include "patronus/All.h"
#include "util/Pre.h"
#include "util/RetCode.h"

namespace serverless
{
class CoroLauncher
{
    constexpr static size_t kMaxLambdaNr = define::kMaxCoroNr;

public:
    using lambda_t = uint64_t;
    using Lambda = std::function<RetCode(
        Parameters &parameters, CoroContext *, util::TraceView)>;
    using ParameterMap = std::map<std::string, Parameter>;
    CoroLauncher(patronus::Patronus::pointer p,
                 size_t server_nid,
                 size_t dir_id,
                 const Config &config,
                 ssize_t work_nr,
                 std::atomic<ssize_t> &remain_work_nr,
                 double trace_rate = 0)
        : patronus_(p),
          server_nid_(server_nid),
          dir_id_(dir_id),
          config_(config),
          tid_(p->get_thread_id()),
          work_nr_(work_nr),
          remain_work_nr_(remain_work_nr),
          trace_rate_(trace_rate)
    {
        coro_ex_.get_private_data().thread_remain_task = work_nr;
    }

    lambda_t add_lambda(Lambda lambda,
                        std::optional<ParameterMap> init_para,
                        std::optional<lambda_t> recv_para_from,
                        const std::vector<lambda_t> &depend_on,
                        std::optional<lambda_t> reloop_to_lambda)
    {
        auto ret = lambda_nr_++;
        CHECK_LE(lambda_nr_, kMaxLambdaNr);
        lambdas_[ret] = lambda;
        if (init_para.has_value())
        {
            // it is the root
            is_root_[ret] = true;
            roots_.push_back(ret);
            depend_nr_[ret] = 0;  // root does not depend on anyone
            inits_[ret] = init_para.value();

            parameters_[ret] = Parameters::new_instance(
                patronus_, server_nid_, dir_id_, config_, trace_rate_);
            parameters_[ret]->install_params(inits_[ret]);

            readys_[ret] = true;
            running_[ret] = false;
            waiting_nr_[ret] = 0;
            CHECK(depend_on.empty())
                << "** providing init_para implies a root lambda, which can "
                   "not have dependent lambdas";
            CHECK(!reloop_to_lambda.has_value())
                << "** providing init_para implies a root lambda, and only "
                   "tail lambda can provide reloop to lambda";
            CHECK(!recv_para_from.has_value());
        }
        else
        {
            // must not be root
            is_root_[ret] = false;
            readys_[ret] = false;
            running_[ret] = false;

            depend_nr_[ret] = depend_on.size();
            waiting_nr_[ret] = depend_nr_[ret];
            CHECK(!init_para.has_value());
            CHECK(!depend_on.empty());
            CHECK(recv_para_from.has_value())
                << "** For any lambda not a root, need to recv para from one "
                   "upstream lambda";
        }
        if (recv_para_from.has_value())
        {
            // the parameter object is shared across the chain
            CHECK_NE(parameters_[recv_para_from.value()], nullptr);
            parameters_[ret] = parameters_[recv_para_from.value()];

            recv_input_from_[ret] = recv_para_from.value();
        }
        CHECK_NE(parameters_[ret], nullptr)
            << "** Does not receive any parameter object";
        for (auto dl : depend_on)
        {
            CHECK_LT(dl, lambda_nr_) << "** invalid dependent lambda";
            let_go_[dl].push_back(ret);
        }

        reloop_to_lambda_[ret] = reloop_to_lambda;
        return ret;
    }

    void launch()
    {
        for (size_t i = 0; i < lambda_nr_; ++i)
        {
            auto &worker = coro_ex_.worker(i);
            worker = CoroCall(
                [this, i](CoroYield &yield) { coro_worker(yield, i); });
        }
        auto &master = coro_ex_.master();
        master = CoroCall([this](CoroYield &yield) { coro_master(yield); });

        master();
    }
    friend std::ostream &operator<<(std::ostream &os, const CoroLauncher &l);

private:
    void coro_master(CoroYield &yield)
    {
        CoroContext mctx(tid_, &yield, coro_ex_.workers());

        size_t coro_nr = lambda_nr_;
        ssize_t task_per_sync = work_nr_ / 100;
        task_per_sync = std::max(task_per_sync, ssize_t(coro_nr));
        ssize_t remain = remain_work_nr_.fetch_sub(task_per_sync,
                                                   std::memory_order_relaxed) -
                         task_per_sync;
        coro_ex_.get_private_data().thread_remain_task = task_per_sync;

        for (size_t i = coro_nr; i < kMaxLambdaNr; ++i)
        {
            coro_ex_.worker_finished(i);
        }

        coro_t coro_buf[2 * kMaxLambdaNr];
        while (true)
        {
            if ((ssize_t) coro_ex_.get_private_data().thread_remain_task <=
                2 * ssize_t(coro_nr))
            {
                // refill
                auto cur_task_nr = std::min(remain, task_per_sync);
                if (cur_task_nr > 0)
                {
                    remain = remain_work_nr_.fetch_sub(
                                 cur_task_nr, std::memory_order_relaxed) -
                             cur_task_nr;
                    if (remain >= 0)
                    {
                        // LOG(INFO)
                        //     << "[refill] cur_task_nr: " << cur_task_nr
                        //     << ", remain: " << remain
                        //     << ", old thread_remain_task: "
                        //     << coro_ex_.get_private_data().thread_remain_task
                        //     << ", new: "
                        //     << coro_ex_.get_private_data().thread_remain_task
                        //     +
                        //            cur_task_nr;
                        coro_ex_.get_private_data().thread_remain_task +=
                            cur_task_nr;
                    }
                }
            }

            auto nr = patronus_->try_get_client_continue_coros(
                coro_buf, 2 * kMaxLambdaNr);
            for (size_t i = 0; i < nr; ++i)
            {
                auto coro_id = coro_buf[i];
                mctx.yield_to_worker(coro_id);
            }

            // for other possible runnables
            for (size_t i = 0; i < coro_nr; ++i)
            {
                if (readys_[i] && !running_[i] && waiting_nr_[i] == 0)
                {
                    mctx.yield_to_worker(i);
                }
            }

            if (remain <= 0)
            {
                if (is_finished())
                {
                    break;
                }
            }
        }
    }

    bool is_finished() const
    {
        return is_finished_;
    }

    void coro_worker(CoroYield &yield, size_t coro_id)
    {
        CoroContext ctx(tid_, &yield, &coro_ex_.master(), coro_id);

        while (coro_ex_.get_private_data().thread_remain_task > 0)
        {
            auto parameters = DCHECK_NOTNULL(parameters_[coro_id]);

            auto &lambda = lambdas_[coro_id];

            DCHECK(readys_[coro_id]);
            DCHECK_EQ(waiting_nr_[coro_id], 0) << "** this lambda not ready.";
            DCHECK(!running_[coro_id]);
            running_[coro_id] = true;
            DVLOG(4) << "[launcher] Entering lambda(" << coro_id
                     << ") with param: " << *parameters << ", ctx: " << ctx;
            auto trace = parameters->trace();
            auto rc = lambda(*parameters, &ctx, trace.child("lambda"));
            CHECK_EQ(rc, RC::kOk) << "** Not prepared to handle any error";
            DVLOG(4) << "[launcher] Leaving lambda(" << coro_id
                     << ") with updated params: " << *parameters
                     << ", ctx: " << ctx;

            running_[coro_id] = false;
            readys_[coro_id] = false;

            // do clean up immediately after this lambda
            // BEFORE letting go its depending children
            if (reloop_to_lambda_[coro_id].has_value())
            {
                auto root = reloop_to_lambda_[coro_id].value();
                parameters_[coro_id]->clear(&ctx, trace.child("clear"));
                parameters_[coro_id]->install_params(inits_[root]);
            }
            else
            {
                // it is not the tail lambda
                parameters_[coro_id]->next_lambda(&ctx);
                trace.pin("next lambda");
            }

            // letting go depending children
            if (reloop_to_lambda_[coro_id].has_value())
            {
                auto root = reloop_to_lambda_[coro_id].value();
                // This lambda is the last in the chain
                coro_ex_.get_private_data().thread_remain_task--;
                reset(root);
                readys_[root] = true;
                DCHECK_EQ(waiting_nr_[root], 0);

                // clear and reinstall parameters for a restart
                DVLOG(4) << "[launcher] Leaving lambda(" << coro_id
                         << ") reloop to root " << root << ", ctx: " << ctx;
            }

            for (auto l : let_go_[coro_id])
            {
                DCHECK_GT(waiting_nr_[l], 0);
                waiting_nr_[l]--;
                DCHECK_GE(waiting_nr_[l], 0);
                if (waiting_nr_[l] == 0)
                {
                    readys_[l] = true;
                }
                DVLOG(4) << "[launcher] Leaving lambda(" << coro_id
                         << ") solve dependency => " << l << " to "
                         << waiting_nr_[l] << ", ready: " << readys_[l]
                         << ". ctx: " << ctx;
            }

            ctx.yield_to_master();
        }

        readys_[coro_id] = false;
        running_[coro_id] = false;
        coro_ex_.worker_finished(coro_id);

        bool can_exit = true;
        for (size_t i = 0; i < lambda_nr_; ++i)
        {
            if (readys_[i] || running_[i])
            {
                can_exit = false;
                break;
            }
        }
        if (can_exit)
        {
            is_finished_ = true;
        }

        ctx.yield_to_master();
        LOG(FATAL) << "** coro " << coro_id
                   << " expect unreachable. ctx: " << ctx;
    }

    void reset(lambda_t root)
    {
        readys_[root] = false;
        waiting_nr_[root] = depend_nr_[root];

        DCHECK(!running_[root]);
        for (auto child : let_go_[root])
        {
            reset(child);
        }
    }
    patronus::Patronus::pointer patronus_;
    size_t server_nid_{0};
    size_t dir_id_{0};
    Config config_;
    size_t tid_{0};
    ssize_t work_nr_;
    std::atomic<ssize_t> &remain_work_nr_;
    double trace_rate_{0};

    // below fields are book-keeping information
    std::array<bool, kMaxLambdaNr> is_root_{};
    std::vector<lambda_t> roots_;
    std::array<std::optional<lambda_t>, kMaxLambdaNr> reloop_to_lambda_{};
    std::array<std::vector<lambda_t>, kMaxLambdaNr> let_go_{};
    std::array<size_t, kMaxLambdaNr> depend_nr_{};
    std::array<ParameterMap, kMaxLambdaNr> inits_{};
    std::array<std::optional<lambda_t>, kMaxLambdaNr> recv_input_from_{};

    // below fields are runtime concerned for lambda
    std::array<Parameters::pointer, kMaxLambdaNr> parameters_{};

    // below fields are runtime concerned for internal implementations
    lambda_t lambda_nr_{0};
    std::array<Lambda, kMaxLambdaNr> lambdas_;
    std::array<bool, kMaxLambdaNr> readys_{};
    std::array<bool, kMaxLambdaNr> running_{};
    std::array<ssize_t, kMaxLambdaNr> waiting_nr_{};

    bool is_finished_{false};

    struct Prv
    {
        ssize_t thread_remain_task;
    };
    CoroExecutionContextWith<kMaxLambdaNr, Prv> coro_ex_;
};

}  // namespace serverless

#endif