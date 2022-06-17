#pragma once
#ifndef PATRONUS_LINKED_LIST_CONFIG_H_
#define PATRONUS_LINKED_LIST_CONFIG_H_

namespace patronus::list
{
struct ListImplConfig
{
    // bool bypass_prot{false};
    struct
    {
        MemHandleDecision default_;
        MemHandleDecision meta_;
        MemHandleDecision alloc_;
    } rdma;
};

inline std::ostream &operator<<(std::ostream &os, const ListImplConfig &c)
{
    os << "{ListImplConfig default: " << c.rdma.default_
       << ", meta: " << c.rdma.meta_ << ", alloc: " << c.rdma.alloc_ << "}";
    return os;
}

struct ListHandleConfig
{
    bool lock_free{true};
    bool bypass_prot{false};
    size_t retry_nr{std::numeric_limits<size_t>::max()};

    ListImplConfig list_impl_config;
    ListHandleConfig(bool lock_free, bool bypass_prot)
        : lock_free(lock_free), bypass_prot(bypass_prot)
    {
        list_impl_config.rdma.default_.use_mw().wo_expire();
        list_impl_config.rdma.meta_ = list_impl_config.rdma.default_;
        list_impl_config.rdma.alloc_.use_mw().with_alloc(0).wo_expire();
    }
};
inline std::ostream &operator<<(std::ostream &os, const ListHandleConfig &c)
{
    os << "{ListHandleConfig lock_free: " << c.lock_free
       << ", retry_nr: " << c.retry_nr << ", bypass_prot: " << c.bypass_prot
       << ", list_impl_config: " << c.list_impl_config << "}";
    return os;
}

}  // namespace patronus::list
#endif