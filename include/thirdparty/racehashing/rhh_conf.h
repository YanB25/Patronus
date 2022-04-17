#pragma once
#ifndef PATRONUS_RACEHASHING_HASHTABLE_RH_CONF_H_
#define PATRONUS_RACEHASHING_HASHTABLE_RH_CONF_H_

#include <chrono>
#include <cstdint>

#include "conf.h"
#include "patronus/Type.h"

namespace patronus::hash
{
using namespace std::chrono_literals;
using AcquireRequestFlag = patronus::AcquireRequestFlag;

struct RaceHashingHandleConfig
{
    // about how to manage reading kv_block
    struct
    {
        struct
        {
            bool do_nothing{false};
            flag_t flag{
                (flag_t) AcquireRequestFlag::kNoGc |
                (flag_t) AcquireRequestFlag::kNoBindPR};  // AcquireRequestFlag
            uint64_t alloc_hint{0};
            std::chrono::nanoseconds lease_time{0ns};

        } begin;
        struct
        {
            bool do_nothing{false};
            uint64_t alloc_hint{0};
            flag_t flag{(flag_t) 0};
        } end;

    } read_kvblock;

    struct
    {
        struct
        {
            bool use_alloc_api{false};
            flag_t flag{(flag_t) AcquireRequestFlag::kNoGc |
                        (flag_t) AcquireRequestFlag::kWithAllocation |
                        (flag_t) AcquireRequestFlag::kNoBindPR};
            uint64_t alloc_hint{hash::config::kAllocHintKVBlock};
            std::chrono::nanoseconds lease_time{0ns};
        } begin;
        struct
        {
            // insert success
            // recover the MW resource
            bool do_nothing{false};
            bool use_alloc_api{false};
            uint64_t alloc_hint{hash::config::kAllocHintKVBlock};
            flag_t flag{0};
        } end;
        struct
        {
            // insert failure
            // recover ALL the resources
            bool do_nothing{false};
            bool use_dealloc_api{false};
            uint64_t alloc_hint{hash::config::kAllocHintKVBlock};
            flag_t flag{(flag_t) LeaseModifyFlag::kWithDeallocation};
        } free;
        bool enable_batch_alloc{false};
        size_t batch_alloc_size{0};
    } insert_kvblock;

    struct
    {
        // used in overwrite & delete, currently do nothing
        bool do_nothing{true};
        uint64_t alloc_hint{hash::config::kAllocHintKVBlock};
    } free_kvblock;

    struct
    {
        bool do_nothing{false};
        bool skip_bind_g_kvblock{false};
        bool eager_bind_subtable{false};
        uint64_t alloc_hint{hash::config::kAllocHintDefault};
        std::chrono::nanoseconds lease_time{0ns};
        flag_t flag{(flag_t) AcquireRequestFlag::kNoGc};
        std::optional<RemoteMemHandle> dcache_handle{std::nullopt};
    } init;
    struct
    {
        uint64_t alloc_hint{0};
        flag_t flag{(flag_t) LeaseModifyFlag::kWaitUntilSuccess};
    } dctor;

    // the hints
    uint64_t subtable_hint{hash::config::kAllocHintDirSubtable};
    // for some slow benchmark, we want test_nr *= test_nr_scale_factor.
    double test_nr_scale_factor{1.0};
    std::string name;
    size_t kvblock_expect_size{64};
};

inline std::ostream &operator<<(std::ostream &os,
                                const RaceHashingHandleConfig &conf)
{
    os << "{RaceHashingHandleConfig: " << conf.name;
    {
        os << ". read_kvblock:{ ";
        {
            const auto &c = conf.read_kvblock.begin;
            if (c.do_nothing)
            {
                os << "do nothing";
            }
            else
            {
                auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              c.lease_time)
                              .count();
                os << "acquire hint: " << c.alloc_hint << ", time: " << ns
                   << " ns, flag: " << AcquireRequestFlagOut(c.flag) << ". ";
            }
        }
        {
            const auto &c = conf.read_kvblock.end;
            os << "rel: ";
            if (c.do_nothing)
            {
                os << "do nothing";
            }
            else
            {
                os << "rel hint: " << c.alloc_hint
                   << ", flag: " << LeaseModifyFlagOut(c.flag);
            }
        }
        os << "}. ";
    }
    {
        os << " insert_kvblock { ";
        {
            const auto &c = conf.insert_kvblock.begin;
            auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                          c.lease_time)
                          .count();
            os << "ins: alloc_api: " << c.use_alloc_api
               << ", hint: " << c.alloc_hint << ", ns: " << ns
               << ", flag: " << AcquireRequestFlagOut(c.flag);
        }
        {
            const auto &c = conf.insert_kvblock.end;
            os << ". rel: alloc_api: " << c.use_alloc_api
               << ", hint: " << c.alloc_hint
               << ", flag: " << LeaseModifyFlagOut(c.flag);
        }
        {
            const auto &c = conf.insert_kvblock.free;
            os << ". free: ";
            if (c.do_nothing)
            {
                os << "do nothing";
            }
            else
            {
                os << "alloc_api: " << c.use_dealloc_api
                   << ", hint: " << c.alloc_hint
                   << ", flag: " << LeaseModifyFlagOut(c.flag);
            }
        }
        os << "}";
    }
    {
        os << " init {";
        {
            const auto &c = conf.init;
            if (c.do_nothing)
            {
                os << "do_nothing";
            }
            else
            {
                auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              c.lease_time)
                              .count();
                os << "skip_kvblock: " << c.skip_bind_g_kvblock
                   << ", eager_subtable: " << c.eager_bind_subtable
                   << ", hint: " << c.alloc_hint << ", time: " << ns
                   << ", flag: " << AcquireRequestFlagOut(c.flag)
                   << ", has_dcache_handle: " << c.dcache_handle.has_value();
            }
        }
        os << "}";
    }
    {
        os << " dctor {";
        {
            const auto &c = conf.dctor;
            os << "hint: " << c.alloc_hint
               << ", flag: " << LeaseModifyFlagOut(c.flag);
        }
        os << "}";
    }
    return os;
}

class RaceHashingConfigFactory
{
public:
    static RaceHashingHandleConfig get_basic(const std::string &name,
                                             size_t kvblock_expect_size)
    {
        RaceHashingHandleConfig handle_conf;
        handle_conf.name = name;
        handle_conf.subtable_hint = hash::config::kAllocHintDirSubtable;
        handle_conf.kvblock_expect_size = kvblock_expect_size;
        return handle_conf;
    }
    static RaceHashingHandleConfig get_unprotected(const std::string name,
                                                   size_t kvblock_expect_size,
                                                   size_t batch_size)
    {
        auto c = get_basic(name, kvblock_expect_size);
        c.read_kvblock.begin.do_nothing = true;
        c.read_kvblock.begin.flag = (flag_t) AcquireRequestFlag::kReserved;
        c.read_kvblock.end.do_nothing = true;
        c.read_kvblock.end.flag = (flag_t) LeaseModifyFlag::kReserved;
        c.insert_kvblock.begin.use_alloc_api = true;
        c.insert_kvblock.begin.alloc_hint = hash::config::kAllocHintKVBlock;
        c.insert_kvblock.begin.flag = (flag_t) AcquireRequestFlag::kReserved;
        c.insert_kvblock.end.use_alloc_api =
            c.insert_kvblock.begin.use_alloc_api;
        c.insert_kvblock.end.alloc_hint = c.insert_kvblock.begin.alloc_hint;
        c.insert_kvblock.end.flag = (flag_t) LeaseModifyFlag::kReserved;
        c.insert_kvblock.batch_alloc_size = batch_size;
        c.insert_kvblock.enable_batch_alloc = batch_size > 1;
        c.insert_kvblock.free.do_nothing = false;
        c.insert_kvblock.free.use_dealloc_api =
            c.insert_kvblock.begin.use_alloc_api;
        c.insert_kvblock.free.alloc_hint = c.insert_kvblock.begin.alloc_hint;
        c.insert_kvblock.free.flag = (flag_t) LeaseModifyFlag::kReserved;
        c.free_kvblock.do_nothing = true;
        return c;
    }
    static RaceHashingHandleConfig get_unprotected_boostrap(
        const std::string name)
    {
        auto c = get_unprotected(name, 0 /* kv block size */, 1);
        c.init.do_nothing = true;
        c.init.skip_bind_g_kvblock = true;
        // no subtable, because no protection
        c.init.eager_bind_subtable = false;
        c.init.flag = (flag_t) AcquireRequestFlag::kReserved;
        c.dctor.flag = (flag_t) LeaseModifyFlag::kNoRelinquishUnbind;
        // can not init mem_handle here
        // because we don't know the dir_id
        return c;
    }
    static RaceHashingHandleConfig get_mw_protected(const std::string &name,
                                                    size_t kvblock_expect_size,
                                                    size_t batch_size)
    {
        auto c = get_basic(name, kvblock_expect_size);
        c.read_kvblock.begin.do_nothing = false;
        c.read_kvblock.begin.flag = (flag_t) AcquireRequestFlag::kNoGc |
                                    (flag_t) AcquireRequestFlag::kNoBindPR;
        c.read_kvblock.begin.lease_time = 0ns;
        c.read_kvblock.end.do_nothing = false;
        c.read_kvblock.end.flag = (flag_t) 0;
        c.insert_kvblock.begin.use_alloc_api = false;
        c.insert_kvblock.begin.flag =
            (flag_t) AcquireRequestFlag::kNoGc |
            (flag_t) AcquireRequestFlag::kWithAllocation |
            (flag_t) AcquireRequestFlag::kNoBindPR;
        c.insert_kvblock.begin.alloc_hint = hash::config::kAllocHintKVBlock;
        c.insert_kvblock.end.use_alloc_api =
            c.insert_kvblock.begin.use_alloc_api;
        c.insert_kvblock.end.alloc_hint = c.insert_kvblock.begin.alloc_hint;
        c.insert_kvblock.end.flag = 0;
        c.insert_kvblock.batch_alloc_size = batch_size;
        c.insert_kvblock.enable_batch_alloc = batch_size > 1;
        c.insert_kvblock.free.do_nothing = false;
        c.insert_kvblock.free.use_dealloc_api =
            c.insert_kvblock.begin.use_alloc_api;
        c.insert_kvblock.free.alloc_hint = c.insert_kvblock.begin.alloc_hint;
        c.insert_kvblock.free.flag =
            (flag_t) LeaseModifyFlag::kWithDeallocation;
        c.free_kvblock.do_nothing = true;
        return c;
    }
    static RaceHashingHandleConfig get_mw_protected_bootstrap(
        const std::string &name, bool eager_bind_subtable)
    {
        auto c =
            get_mw_protected(name, 64 /* kv block size */, 1 /* batch size */);
        c.init.do_nothing = false;
        // binding kvblock is a trick, it should not be included into boostrap
        // time
        c.init.skip_bind_g_kvblock = true;
        c.init.eager_bind_subtable = eager_bind_subtable;
        c.init.flag = (flag_t) AcquireRequestFlag::kNoGc;
        c.dctor.flag = (flag_t) LeaseModifyFlag::kNoRelinquishUnbind;
        return c;
    }
    // can not support batch allocation
    // because what we get
    static RaceHashingHandleConfig get_mw_protected_with_timeout(
        const std::string &name, size_t kvblock_expect_size)
    {
        auto c = get_basic(name, kvblock_expect_size);
        c.read_kvblock.begin.do_nothing = false;
        c.read_kvblock.begin.flag = 0;
        c.read_kvblock.begin.lease_time = 1ms;  // definitely enough
        c.read_kvblock.end.do_nothing = true;
        c.read_kvblock.end.flag = (flag_t) LeaseModifyFlag::kReserved;
        c.insert_kvblock.begin.use_alloc_api = false;
        // with PR, because lease is enabled.
        c.insert_kvblock.begin.flag =
            (flag_t) AcquireRequestFlag::kWithAllocation;
        c.insert_kvblock.begin.lease_time = 1ms;  // definitely enough
        c.insert_kvblock.begin.alloc_hint = hash::config::kAllocHintKVBlock;
        c.insert_kvblock.end.use_alloc_api =
            c.insert_kvblock.begin.use_alloc_api;
        c.insert_kvblock.end.do_nothing = true;
        c.insert_kvblock.end.alloc_hint = c.insert_kvblock.begin.alloc_hint;
        c.insert_kvblock.end.flag = (flag_t) LeaseModifyFlag::kReserved;
        c.insert_kvblock.enable_batch_alloc = false;
        c.insert_kvblock.free.do_nothing = false;
        c.insert_kvblock.free.use_dealloc_api =
            c.insert_kvblock.begin.use_alloc_api;
        c.insert_kvblock.free.alloc_hint = c.insert_kvblock.begin.alloc_hint;
        c.insert_kvblock.free.flag =
            (flag_t) LeaseModifyFlag::kWithDeallocation;
        c.free_kvblock.do_nothing = true;
        return c;
    }
    static RaceHashingHandleConfig get_mr_protected(const std::string &name,
                                                    size_t kvblock_expect_size,
                                                    size_t batch_size)
    {
        auto c = get_mw_protected(name, kvblock_expect_size, batch_size);
        c.read_kvblock.begin.flag |= (flag_t) AcquireRequestFlag::kUseMR;
        c.read_kvblock.end.flag |= (flag_t) LeaseModifyFlag::kUseMR;
        // NOTE: we already set flag kUSeMR ON.
        // No need to use KVBlockOverMR hint.
        // Otherwise, the MR will be bind twice, and MR in the allocator is
        // leaked.
        c.insert_kvblock.begin.alloc_hint = hash::config::kAllocHintKVBlock;
        DCHECK(!c.insert_kvblock.begin.use_alloc_api);
        c.insert_kvblock.end.alloc_hint = c.insert_kvblock.begin.alloc_hint;
        c.insert_kvblock.begin.flag |= (flag_t) AcquireRequestFlag::kUseMR;
        c.insert_kvblock.end.flag |= (flag_t) LeaseModifyFlag::kUseMR;
        c.insert_kvblock.free.flag |= (flag_t) LeaseModifyFlag::kUseMR;
        c.dctor.flag |= (flag_t) LeaseModifyFlag::kUseMR;

        c.test_nr_scale_factor = 1.0 / 10;
        return c;
    }
    static RaceHashingHandleConfig get_mr_protected_boostrap(
        const std::string &name, bool eager_bind_subtable)
    {
        auto c = get_mw_protected_bootstrap(name, eager_bind_subtable);
        c.init.flag |= (flag_t) AcquireRequestFlag::kUseMR;
        c.dctor.flag = (flag_t) LeaseModifyFlag::kNoRelinquishUnbind;

        c.test_nr_scale_factor = 1.0 / 10;
        return c;
    }
};

}  // namespace patronus::hash

#endif