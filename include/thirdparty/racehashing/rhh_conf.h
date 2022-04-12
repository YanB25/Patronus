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
            uint8_t flag{
                (uint8_t) AcquireRequestFlag::kNoGc |
                (uint8_t) AcquireRequestFlag::kNoBindPR};  // AcquireRequestFlag
            uint64_t alloc_hint{0};
            std::chrono::nanoseconds lease_time{0ns};

        } begin;
        struct
        {
            bool do_nothing{false};
            uint64_t alloc_hint{0};
            uint8_t flag{(uint8_t) 0};
        } end;

    } read_kvblock;

    struct
    {
        struct
        {
            bool use_alloc_api{false};
            uint8_t flag{(uint8_t) AcquireRequestFlag::kNoGc |
                         (uint8_t) AcquireRequestFlag::kWithAllocation |
                         (uint8_t) AcquireRequestFlag::kNoBindPR};
            uint64_t alloc_hint{hash::config::kAllocHintKVBlock};
            std::chrono::nanoseconds lease_time{0ns};
        } begin;
        struct
        {
            bool do_nothing{false};
            bool use_alloc_api{false};
            uint64_t alloc_hint{hash::config::kAllocHintKVBlock};
            uint8_t flag{0};
        } end;
        bool enable_batch_alloc{false};
        size_t batch_alloc_size{0};
    } insert_kvblock;

    struct
    {
    } free_kvblock;

    // the hints
    uint64_t subtable_hint{hash::config::kAllocHintDirSubtable};
    // for some slow benchmark, we want test_nr *= test_nr_scale_factor.
    double test_nr_scale_factor{1.0};
    std::string name;
};

inline std::ostream &operator<<(std::ostream &os,
                                const RaceHashingHandleConfig &conf)
{
    os << "{RaceHashingHandleConfig: ";
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
        os << "}";
    }
    return os;
}

class RaceHashingConfigFactory
{
public:
    static RaceHashingHandleConfig get_basic(const std::string &name)
    {
        RaceHashingHandleConfig handle_conf;
        handle_conf.name = name;
        handle_conf.subtable_hint = hash::config::kAllocHintDirSubtable;
        return handle_conf;
    }
    static RaceHashingHandleConfig get_unprotected(const std::string name,
                                                   size_t batch_size)
    {
        auto c = get_basic(name);
        c.read_kvblock.begin.do_nothing = true;
        c.read_kvblock.begin.flag = (uint8_t) AcquireRequestFlag::kReserved;
        c.read_kvblock.end.do_nothing = true;
        c.read_kvblock.end.flag = (uint8_t) LeaseModifyFlag::kReserved;
        c.insert_kvblock.begin.use_alloc_api = true;
        c.insert_kvblock.begin.alloc_hint = hash::config::kAllocHintKVBlock;
        c.insert_kvblock.begin.flag = (uint8_t) AcquireRequestFlag::kReserved;
        c.insert_kvblock.end.use_alloc_api =
            c.insert_kvblock.begin.use_alloc_api;
        c.insert_kvblock.end.alloc_hint = c.insert_kvblock.begin.alloc_hint;
        c.insert_kvblock.end.flag = (uint8_t) LeaseModifyFlag::kReserved;
        c.insert_kvblock.batch_alloc_size = batch_size;
        c.insert_kvblock.enable_batch_alloc = batch_size > 1;
        return c;
    }
    static RaceHashingHandleConfig get_mw_protected(const std::string &name,
                                                    size_t batch_size)
    {
        auto c = get_basic(name);
        c.read_kvblock.begin.do_nothing = false;
        c.read_kvblock.begin.flag = (uint8_t) AcquireRequestFlag::kNoGc |
                                    (uint8_t) AcquireRequestFlag::kNoBindPR;
        c.read_kvblock.begin.lease_time = 0ns;
        c.read_kvblock.end.do_nothing = false;
        c.read_kvblock.end.flag = (uint8_t) 0;
        c.insert_kvblock.begin.use_alloc_api = false;
        c.insert_kvblock.begin.flag =
            (uint8_t) AcquireRequestFlag::kNoGc |
            (uint8_t) AcquireRequestFlag::kWithAllocation |
            (uint8_t) AcquireRequestFlag::kNoBindPR;
        c.insert_kvblock.begin.alloc_hint = hash::config::kAllocHintKVBlock;
        c.insert_kvblock.end.use_alloc_api =
            c.insert_kvblock.begin.use_alloc_api;
        c.insert_kvblock.end.alloc_hint = c.insert_kvblock.begin.alloc_hint;
        c.insert_kvblock.end.flag = 0;
        c.insert_kvblock.batch_alloc_size = batch_size;
        c.insert_kvblock.enable_batch_alloc = batch_size > 1;
        return c;
    }
    // can not support batch allocation
    // because what we get
    static RaceHashingHandleConfig get_mw_protected_with_timeout(
        const std::string &name)
    {
        auto c = get_basic(name);
        c.read_kvblock.begin.do_nothing = false;
        c.read_kvblock.begin.flag = 0;
        c.read_kvblock.begin.lease_time = 1ms;  // definitely enough
        c.read_kvblock.end.do_nothing = true;
        c.read_kvblock.end.flag = (uint8_t) LeaseModifyFlag::kReserved;
        c.insert_kvblock.begin.use_alloc_api = false;
        c.insert_kvblock.begin.flag =
            (uint8_t) AcquireRequestFlag::kWithAllocation;
        c.insert_kvblock.begin.lease_time = 1ms;  // definitely enough
        c.insert_kvblock.begin.alloc_hint = hash::config::kAllocHintKVBlock;
        c.insert_kvblock.end.use_alloc_api =
            c.insert_kvblock.begin.use_alloc_api;
        c.insert_kvblock.end.do_nothing = true;
        c.insert_kvblock.end.alloc_hint = c.insert_kvblock.begin.alloc_hint;
        c.insert_kvblock.end.flag = (uint8_t) LeaseModifyFlag::kReserved;
        c.insert_kvblock.enable_batch_alloc = false;
        return c;
    }
    static RaceHashingHandleConfig get_mr_protected(const std::string &name,
                                                    size_t batch_size)
    {
        auto c = get_mw_protected(name, batch_size);
        c.read_kvblock.begin.flag |= (uint8_t) AcquireRequestFlag::kUseMR;
        c.read_kvblock.end.flag |= (uint8_t) LeaseModifyFlag::kUseMR;
        // NOTE: we already set flag kUSeMR ON.
        // No need to use KVBlockOverMR hint.
        // Otherwise, the MR will be bind twice, and MR in the allocator is
        // leaked.
        c.insert_kvblock.begin.alloc_hint = hash::config::kAllocHintKVBlock;
        DCHECK(!c.insert_kvblock.begin.use_alloc_api);
        c.insert_kvblock.end.alloc_hint = c.insert_kvblock.begin.alloc_hint;
        c.insert_kvblock.begin.flag |= (uint8_t) AcquireRequestFlag::kUseMR;
        c.insert_kvblock.end.flag |= (uint8_t) LeaseModifyFlag::kUseMR;
        c.test_nr_scale_factor = 1.0 / 10;
        return c;
    }
};

}  // namespace patronus::hash

#endif