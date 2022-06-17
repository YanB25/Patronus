#pragma once
#ifndef PATRONUS_CONCURRENT_QUEUE_CONFIG_H_
#define PATRONUS_CONCURRENT_QUEUE_CONFIG_H_
#include "thirdparty/linked-list/conf.h"

namespace patronus::cqueue
{
struct QueueHandleConfig
{
    std::string name;
    bool bypass_prot{false};
    size_t entry_per_block{};
    size_t retry_nr{std::numeric_limits<size_t>::max()};
    list::ListImplConfig list_impl_config;
    double task_scale{1};

    std::string conf_name() const
    {
        return name;
    }

    static QueueHandleConfig get_basic(const std::string &name,
                                       bool bypass_prot,
                                       size_t entry_per_block)
    {
        QueueHandleConfig conf;
        conf.name = name;
        conf.bypass_prot = bypass_prot;
        conf.entry_per_block = entry_per_block;
        return conf;
    }

    static QueueHandleConfig get_mw(const std::string &name,
                                    size_t entry_per_block)
    {
        QueueHandleConfig conf =
            get_basic(name, false /* bypass prot */, entry_per_block);
        MemHandleDecision default_dec;
        default_dec.use_mw().wo_expire();
        conf.list_impl_config.rdma.default_ = default_dec;
        conf.list_impl_config.rdma.meta_ = default_dec;
        conf.list_impl_config.rdma.alloc_.with_alloc(0 /* hint */)
            .use_mw()
            .wo_expire();
        return conf;
    }
    static QueueHandleConfig get_unprotected(const std::string &name,
                                             size_t entry_per_block)
    {
        QueueHandleConfig conf =
            get_basic(name, true /* bypass prot */, entry_per_block);
        conf.list_impl_config.rdma.default_.no_rpc();
        conf.list_impl_config.rdma.meta_ = conf.list_impl_config.rdma.default_;
        conf.list_impl_config.rdma.alloc_.only_alloc(0);
        return conf;
    }
    static QueueHandleConfig get_mr(const std::string &name,
                                    size_t entry_per_block)
    {
        QueueHandleConfig conf =
            get_basic(name, false /* bypass prot */, entry_per_block);
        conf.list_impl_config.rdma.default_.use_mr().wo_expire();
        conf.list_impl_config.rdma.meta_ = conf.list_impl_config.rdma.default_;
        conf.list_impl_config.rdma.alloc_.with_alloc(0).use_mr().wo_expire();
        conf.task_scale = 0.1;
        return conf;
    }
};
inline std::ostream &operator<<(std::ostream &os,
                                const QueueHandleConfig &config)
{
    os << "{HandleConfig name: " << config.name
       << ", bypass_prot: " << config.bypass_prot
       << ", entry_per_block: " << config.entry_per_block
       << ", retry_nr: " << config.retry_nr
       << ", list_impl_config: " << config.list_impl_config << "}";
    return os;
}

}  // namespace patronus::cqueue
#endif