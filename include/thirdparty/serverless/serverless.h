#pragma once
#ifndef THIRD_PARTY_SERVERLESS_SERVERLESS_H_
#define THIRD_PARTY_SERVERLESS_SERVERLESS_H_

#include <cinttypes>
#include <functional>
#include <optional>

#include "Common.h"
#include "CoroContext.h"
#include "GlobalAddress.h"
#include "patronus/All.h"
#include "util/Pre.h"
#include "util/RetCode.h"

namespace serverless
{
struct Parameter
{
    GlobalAddress gaddr;
    size_t size;
    void *prv;
};
inline std::ostream &operator<<(std::ostream &os, const Parameter &p)
{
    os << "{gaddr: " << p.gaddr << ", size: " << p.size << ", prv: " << p.prv
       << "}";
    return os;
}
class CoroLauncher
{
    constexpr static size_t kMaxLambdaNr = define::kMaxCoroNr;

public:
    using lambda_t = uint64_t;
    using Parameters = std::map<std::string, Parameter>;
    using Lambda = std::function<RetCode(
        const Parameters &input, Parameters &output, CoroContext *)>;
    CoroLauncher(patronus::Patronus::pointer p,
                 ssize_t work_nr,
                 std::atomic<ssize_t> &remain_work_nr)
        : patronus_(p),
          tid_(p->get_thread_id()),
          work_nr_(work_nr),
          remain_work_nr_(remain_work_nr)
    {
        coro_ex_.get_private_data().thread_remain_task = work_nr;
    }

    lambda_t add_lambda(Lambda lambda,
                        std::optional<Parameters> init_para,
                        std::optional<lambda_t> dependent_lambda,
                        std::optional<lambda_t> root_lambda)
    {
        auto ret = lambda_nr_++;
        CHECK_LE(lambda_nr_, kMaxLambdaNr);
        lambdas_[ret] = lambda;
        if (init_para.has_value())
        {
            // it is the root
            inits_[ret] = init_para.value();
            is_root_[ret] = true;
            roots_.push_back(ret);
            readys_[ret] = true;
            CHECK(!dependent_lambda.has_value())
                << "** providing init_para implies a root lambda, which can "
                   "not have dependent lambdas";
            CHECK(!root_lambda.has_value())
                << "** providing init_para implies a root lambda, and only "
                   "tail lambda can provide its root_lambda";
        }
        if (dependent_lambda.has_value())
        {
            // it is the middle (, or possibly tail)
            CHECK_LT(dependent_lambda.value(), lambda_nr_)
                << "** invalid dependent lambda";
            is_root_[ret] = false;
            readys_[ret] = false;
            let_go_[dependent_lambda.value()].push_back(ret);
            depend_on_[ret] = dependent_lambda.value();
            CHECK(!init_para.has_value());
        }

        root_lambda_[ret] = root_lambda;
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
                if (readys_[i] && !running_[i])
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
            Parameters empty_input;
            const Parameters *input_para{};
            if (depend_on_[coro_id].has_value())
            {
                auto d = depend_on_[coro_id].value();
                input_para = &outputs_[d];
            }
            else
            {
                input_para = &inits_[coro_id];
            }

            auto &lambda = lambdas_[coro_id];

            DCHECK(readys_[coro_id]);
            DCHECK(!running_[coro_id]);
            running_[coro_id] = true;
            outputs_[coro_id].clear();
            DVLOG(4) << "[launcher] Entering lambda(" << coro_id
                     << ") with input: " << util::pre_map(*input_para)
                     << ", ctx: " << ctx;
            auto rc = lambda(*input_para, outputs_[coro_id], &ctx);
            CHECK_EQ(rc, RC::kOk) << "** Not prepared to handle any error";
            DVLOG(4) << "[launcher] Leaving lambda(" << coro_id
                     << ") with output: " << util::pre_map(outputs_[coro_id])
                     << ", ctx: " << ctx;
            for (auto l : let_go_[coro_id])
            {
                DCHECK(!readys_[l]);
                readys_[l] = true;
                DCHECK(depend_on_[l].has_value());
                DCHECK_EQ(depend_on_[l].value(), coro_id);
                DVLOG(4) << "[launcher] Leaving lambda(" << coro_id
                         << ") solve dependency => " << l << ", ctx: " << ctx;
            }
            readys_[coro_id] = false;
            running_[coro_id] = false;

            if (root_lambda_[coro_id].has_value())
            {
                // This lambda is the last in the chain
                coro_ex_.get_private_data().thread_remain_task--;
                auto root = root_lambda_[coro_id].value();
                reset(root);
                readys_[root] = true;
                DVLOG(4) << "[launcher] Leaving lambda(" << coro_id
                         << ") reloop to root " << root << ", ctx: " << ctx;
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
        inputs_[root].clear();
        outputs_[root].clear();
        DCHECK(!running_[root]);
        for (auto child : let_go_[root])
        {
            reset(child);
        }
    }
    patronus::Patronus::pointer patronus_;
    size_t tid_{0};
    ssize_t work_nr_;
    std::atomic<ssize_t> &remain_work_nr_;

    lambda_t lambda_nr_{0};
    std::array<std::optional<lambda_t>, kMaxLambdaNr> depend_on_{};
    std::array<std::vector<lambda_t>, kMaxLambdaNr> let_go_{};
    std::array<std::optional<lambda_t>, kMaxLambdaNr> root_lambda_{};
    std::array<Lambda, kMaxLambdaNr> lambdas_;
    std::array<bool, kMaxLambdaNr> readys_{};
    std::array<bool, kMaxLambdaNr> running_{};
    std::array<bool, kMaxLambdaNr> is_root_{};
    std::vector<lambda_t> roots_;

    std::array<Parameters, kMaxLambdaNr> inputs_{};
    std::array<Parameters, kMaxLambdaNr> outputs_{};
    std::array<Parameters, kMaxLambdaNr> inits_{};

    bool is_finished_{false};

    struct Prv
    {
        size_t thread_remain_task;
    };
    CoroExecutionContextWith<kMaxLambdaNr, Prv> coro_ex_;
};

inline std::ostream &operator<<(std::ostream &os,
                                const CoroLauncher::Parameters &ps)
{
    os << util::pre_map(ps);
    return os;
}

}  // namespace serverless

#endif