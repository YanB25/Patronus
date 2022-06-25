#pragma once
#ifndef PATRONUS_RACEHASHING_HASHTABLE_RH_CONF_H_
#define PATRONUS_RACEHASHING_HASHTABLE_RH_CONF_H_

#include <chrono>
#include <cstdint>

#include "conf.h"
#include "patronus/Type.h"
#include "util/TimeConv.h"

namespace patronus::hash
{
using namespace std::chrono_literals;
using AcquireRequestFlag = patronus::AcquireRequestFlag;

struct RaceHashingHandleConfig
{
    // about how to manage reading kv_block
    MemHandleDecision read_kvblock;
    MemHandleDecision alloc_kvblock;
    struct
    {
        MemHandleDecision d;
        bool eager_bind_subtable{false};
    } meta;

    struct
    {
        bool use_patronus_lock{false};
        flag_t patronus_lock_flag{(flag_t) AcquireRequestFlag::kReserved};
        std::chrono::nanoseconds lock_time_ns{0ns};
        flag_t patronus_unlock_flag{(flag_t) 0};
        mutable ssize_t mock_crash_nr{0};
    } expand;

    std::optional<MemHandleDecision> kvblock_region;

    // the hints
    uint64_t subtable_hint{hash::config::kAllocHintDirSubtable};
    // for some slow benchmark, we want test_nr *= test_nr_scale_factor.
    double test_nr_scale_factor{1.0};
    std::string name;
    size_t kvblock_expect_size{64};
    bool bypass_prot{false};

    // If this flag is on, we always assume FP matches => actually matches.
    // The actual content of kv blocks is ignored.
    // This flag is helpful for cases where we
    bool force_kvblock_to_match{false};
};

inline std::ostream &operator<<(std::ostream &os,
                                const RaceHashingHandleConfig &conf)
{
    os << "{RaceHashingHandleConfig: " << conf.name
       << ", read_kvblock: " << conf.read_kvblock
       << ", alloc_kvblock: " << conf.alloc_kvblock
       << ", meta: {eager_bind_subtable: " << conf.meta.eager_bind_subtable
       << ", d: " << conf.meta.d << "}, kvblock_region: ";
    if (conf.kvblock_region.has_value())
    {
        os << conf.kvblock_region.value() << ". ";
    }
    else
    {
        os << " none. ";
    }

    {
        const auto &c = conf.expand;
        auto ns = util::time::to_ns(c.lock_time_ns);
        os << "expand: {use_patronus_lock: " << c.use_patronus_lock
           << ", mock_crash_nr: " << c.mock_crash_nr << ", patronus_lock_flag: "
           << AcquireRequestFlagOut(c.patronus_lock_flag)
           << ", patronus_unlock_flag: "
           << AcquireRequestFlagOut(c.patronus_unlock_flag) << ", time: " << ns
           << "}, ";
    }
    os << "subtable_hint: " << conf.subtable_hint
       << ", kvblock_expect_size: " << conf.kvblock_expect_size
       << ", bypass_prot: " << conf.bypass_prot << "}";
    return os;
}

class RaceHashingConfigFactory
{
public:
    static RaceHashingHandleConfig get_basic(const std::string &name,
                                             size_t kvblock_expect_size,
                                             bool force_kvblock_to_match)
    {
        RaceHashingHandleConfig handle_conf;
        handle_conf.name = name;
        handle_conf.subtable_hint = hash::config::kAllocHintDirSubtable;
        handle_conf.kvblock_expect_size = kvblock_expect_size;
        handle_conf.force_kvblock_to_match = force_kvblock_to_match;
        return handle_conf;
    }
    static RaceHashingHandleConfig get_unprotected(const std::string name,
                                                   size_t kvblock_expect_size,
                                                   size_t batch_size,
                                                   bool force_kvblock_to_match)
    {
        CHECK_EQ(batch_size, 1) << "TODO: ";
        auto c = get_basic(name, kvblock_expect_size, force_kvblock_to_match);
        c.bypass_prot = true;
        c.read_kvblock.no_rpc();
        c.alloc_kvblock.only_alloc(hash::config::kAllocHintKVBlock);
        // unprotected does not need to relinquish anything
        c.alloc_kvblock.relinquish_flag |= (flag_t) LeaseModifyFlag::kNoRpc;
        c.meta.d.no_rpc();
        return c;
    }
    static RaceHashingHandleConfig get_mw_protected(const std::string &name,
                                                    size_t kvblock_expect_size,
                                                    size_t batch_size,
                                                    bool force_kvblock_to_match)
    {
        CHECK_EQ(batch_size, 1) << "TODO: ";
        auto c = get_basic(name, kvblock_expect_size, force_kvblock_to_match);
        c.read_kvblock.use_mw().wo_expire().no_bind_pr();
        c.alloc_kvblock.with_alloc(hash::config::kAllocHintKVBlock)
            .use_mw()
            .wo_expire();
        c.meta.d.use_mw().wo_expire().no_bind_pr();
        return c;
    }
    static RaceHashingHandleConfig get_mw_protected_debug(
        const std::string &name,
        size_t kvblock_expect_size,
        size_t batch_size,
        bool force_kvblock_to_match)
    {
        CHECK_EQ(batch_size, 1) << "TODO: ";
        auto c = get_basic(name, kvblock_expect_size, force_kvblock_to_match);
        c.read_kvblock.use_mw().wo_expire().no_bind_pr();
        c.alloc_kvblock.with_alloc(hash::config::kAllocHintKVBlock)
            .use_mw()
            .wo_expire();
        c.meta.d.use_mw().wo_expire().no_bind_pr();
        c.kvblock_region =
            MemHandleDecision().use_mw().wo_expire().no_bind_pr();
        return c;
    }
    static RaceHashingHandleConfig get_mw_protected_expand_fault_tolerance(
        const std::string &name,
        size_t kvblock_size,
        ssize_t mock_crash_nr,
        std::chrono::nanoseconds lock_time_ns)
    {
        auto c = get_mw_protected(
            name, kvblock_size, 1, false /* force kvblock to match */);
        c.expand.use_patronus_lock = true;
        c.expand.patronus_lock_flag =
            (flag_t) AcquireRequestFlag::kWithConflictDetect;
        c.expand.lock_time_ns = lock_time_ns;
        c.expand.mock_crash_nr = mock_crash_nr;
        return c;
    }
    // can not support batch allocation
    // because what we get
    static RaceHashingHandleConfig get_mw_protected_with_timeout(
        const std::string &name,
        size_t kvblock_expect_size,
        bool force_kvblock_to_match)
    {
        auto c = get_basic(name, kvblock_expect_size, force_kvblock_to_match);
        c.read_kvblock.use_mw().with_expire(1ms).no_bind_pr();
        c.alloc_kvblock.with_alloc(hash::config::kAllocHintKVBlock)
            .use_mw()
            .with_expire(1ms);
        c.meta.d.use_mw().wo_expire().no_bind_pr();
        return c;
    }
    static RaceHashingHandleConfig get_mr_protected(const std::string &name,
                                                    size_t kvblock_expect_size,
                                                    size_t batch_size,
                                                    bool force_kvblock_to_match)
    {
        auto c = get_mw_protected(
            name, kvblock_expect_size, batch_size, force_kvblock_to_match);
        c.read_kvblock.use_mr();
        c.alloc_kvblock.use_mr();
        c.meta.d.use_mr();

        c.test_nr_scale_factor = 1.0 / 10;
        return c;
    }
};

}  // namespace patronus::hash

#endif