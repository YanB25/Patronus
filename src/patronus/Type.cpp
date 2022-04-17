#include "patronus/Type.h"

#include <iostream>

namespace patronus
{
std::ostream &operator<<(std::ostream &os, const RequestType &t)
{
    switch (t)
    {
    case RequestType::kAcquireRLease:
    {
        os << "AcR";
        break;
    }
    case RequestType::kAcquireWLease:
    {
        os << "AcW";
        break;
    }
    case RequestType::kUpgrade:
    {
        os << "U";
        break;
    }
    case RequestType::kRelinquish:
    {
        os << "Rel";
        break;
    }
    case RequestType::kExtend:
    {
        os << "Ex";
        break;
    }
    case RequestType::kAdmin:
    {
        os << "Adm";
        break;
    }
    case RequestType::kTimeSync:
    {
        os << "TimeSync";
        break;
    }
    default:
    {
        os << "Unknown(" << (int) t << ")";
        break;
    }
    }
    return os;
}

std::ostream &operator<<(std::ostream &os, AcquireRequestStatus status)
{
    switch (status)
    {
    case AcquireRequestStatus::kSuccess:
    {
        os << "success";
        break;
    }
    case AcquireRequestStatus::kMagicMwErr:
    {
        os << "magic-mw-err";
        break;
    }
    case AcquireRequestStatus::kLockedErr:
    {
        os << "locked";
        break;
    }
    case AcquireRequestStatus::kBindErr:
    {
        os << "bind-err";
        break;
    }
    case AcquireRequestStatus::kRegMrErr:
    {
        os << "reg-mr-err";
    }
    case AcquireRequestStatus::kAddressOutOfRangeErr:
    {
        os << "addr-out-of-range-err";
        break;
    }
    case AcquireRequestStatus::kNoMem:
    {
        os << "no-mem";
        break;
    }
    case AcquireRequestStatus::kNoMw:
    {
        os << "no-mw";
        break;
    }
    case AcquireRequestStatus::kReserved:
    {
        os << "reserved";
        break;
    }
    case AcquireRequestStatus::kReservedNoReturn:
    {
        os << "reserved-no-return";
        break;
    }
    default:
    {
        CHECK(false);
    }
    }
    return os;
}

std::ostream &operator<<(std::ostream &os, const AdminFlag &f)
{
    switch (f)
    {
    case AdminFlag::kAdminReqExit:
    {
        os << "Exit";
        break;
    }
    case AdminFlag::kAdminReqRecovery:
    {
        os << "Recov";
        break;
    }
    case AdminFlag::kAdminBarrier:
    {
        os << "Barrier";
        break;
    }
    default:
    {
        os << "Unknown(" << (int) f << ")";
        break;
    }
    }
    return os;
}

std::ostream &operator<<(std::ostream &os, const ClientID &cid)
{
    os << "{CID node: " << cid.node_id << ", tid: " << cid.thread_id
       << ", mid: " << (int) cid.mid << ", coro: " << (int) cid.coro_id
       << ", rpc_ctx_id: " << cid.rpc_ctx_id << "}";
    return os;
}

void debug_validate_acquire_request_flag(flag_t flag)
{
    if constexpr (debug())
    {
        bool no_gc = flag & (flag_t) AcquireRequestFlag::kNoGc;
        bool with_conflict_detect =
            flag & (flag_t) AcquireRequestFlag::kWithConflictDetect;
        [[maybe_unused]] bool no_bind_pr =
            flag & (flag_t) AcquireRequestFlag::kNoBindPR;
        [[maybe_unused]] bool no_bind_any =
            flag & (flag_t) AcquireRequestFlag::kNoBindAny;
        bool with_alloc = flag & (flag_t) AcquireRequestFlag::kWithAllocation;
        bool only_alloc = flag & (flag_t) AcquireRequestFlag::kOnlyAllocation;
        bool reserved = flag & (flag_t) AcquireRequestFlag::kReserved;
        bool use_mr = flag & (flag_t) AcquireRequestFlag::kUseMR;
        bool do_nothing = flag & (flag_t) AcquireRequestFlag::kDoNothing;
        DCHECK(!reserved);
        if (with_alloc)
        {
            DCHECK(!only_alloc) << "only_alloc conflict with with_alloc";
            DCHECK(!with_conflict_detect)
                << "Allocation semantics will not detect conflict";
            DCHECK(!no_bind_any)
                << "If does not bind_any, it should be only_alloc";
        }
        if (only_alloc)
        {
            DCHECK(!with_alloc) << "with_alloc conflict with only_alloc";
            DCHECK(no_gc) << "Set no-gc for allocation semantics";
            DCHECK(!with_conflict_detect)
                << "Allocation semantics will not detect conflict";
        }
        if (use_mr)
        {
            DCHECK(no_gc)
                << "Not sure: If using MR, not guarantee to work with lease. "
                   "And the performance will be terrible in my expection";
        }
        if (do_nothing)
        {
            CHECK(!with_conflict_detect)
                << "with_conflict_detect conflict with do_nothing";
            CHECK(!with_alloc) << "with_alloc conflict with do_nothing";
            CHECK(!only_alloc) << "only_alloc conflict with do_nothing";
            CHECK(!use_mr) << "use_mr conflict with do_nothing";
        }
    }
}

std::ostream &operator<<(std::ostream &os, AcquireRequestFlagOut flag)
{
    os << "{AcquireRequestFlag ";
    bool reserved = flag.flag & (flag_t) AcquireRequestFlag::kReserved;
    if (reserved)
    {
        os << "RESERVED, ";
    }
    bool no_gc = flag.flag & (flag_t) AcquireRequestFlag::kNoGc;
    if (no_gc)
    {
        os << "no-gc, ";
    }
    bool with_conflict_detect =
        flag.flag & (flag_t) AcquireRequestFlag::kWithConflictDetect;
    if (with_conflict_detect)
    {
        os << "with-lock, ";
    }
    bool debug_no_bind_pr = flag.flag & (flag_t) AcquireRequestFlag::kNoBindPR;
    if (debug_no_bind_pr)
    {
        os << "no-pr, ";
    }
    bool debug_no_bind_any =
        flag.flag & (flag_t) AcquireRequestFlag::kNoBindAny;
    if (debug_no_bind_any)
    {
        os << "no-any, ";
    }
    bool with_alloc = flag.flag & (flag_t) AcquireRequestFlag::kWithAllocation;
    if (with_alloc)
    {
        os << "with-alloc, ";
    }
    bool only_alloc = flag.flag & (flag_t) AcquireRequestFlag::kOnlyAllocation;
    if (only_alloc)
    {
        os << "only-alloc, ";
    }
    bool use_mr = flag.flag & (flag_t) AcquireRequestFlag::kUseMR;
    if (use_mr)
    {
        os << "use-MR, ";
    }
    bool do_nothing = flag.flag & (flag_t) AcquireRequestFlag::kDoNothing;
    if (do_nothing)
    {
        os << "do-nothing, ";
    }
    os << "}";
    return os;
}

std::ostream &operator<<(std::ostream &os, RWFlagOut flag)
{
    os << "{RWFlag ";
    bool no_local_expire_check =
        flag.flag & (flag_t) RWFlag::kNoLocalExpireCheck;
    if (no_local_expire_check)
    {
        os << "no-check, ";
    }
    bool with_auto_extend = flag.flag & (flag_t) RWFlag::kWithAutoExtend;
    if (with_auto_extend)
    {
        os << "with-extend, ";
    }
    bool with_cache = flag.flag & (flag_t) RWFlag::kWithCache;
    if (with_cache)
    {
        os << "with-cache, ";
    }
    bool use_universal_rkey = flag.flag & (flag_t) RWFlag::kUseUniversalRkey;
    if (use_universal_rkey)
    {
        os << "use-universal-rkey, ";
    }
    bool reserve = flag.flag & (flag_t) RWFlag::kReserved;
    DCHECK(!reserve);
    os << "}";
    return os;
}
void debug_validate_lease_modify_flag(flag_t flag)
{
    if constexpr (debug())
    {
        [[maybe_unused]] bool no_relinquish_unbind =
            flag & (flag_t) LeaseModifyFlag::kNoRelinquishUnbind;
        [[maybe_unused]] bool force_unbind =
            flag & (flag_t) LeaseModifyFlag::kForceUnbind;
        bool with_dealloc = flag & (flag_t) LeaseModifyFlag::kWithDeallocation;
        bool only_dealloc = flag & (flag_t) LeaseModifyFlag::kOnlyDeallocation;
        bool reserved = flag & (flag_t) LeaseModifyFlag::kReserved;
        bool wait_success = flag & (flag_t) LeaseModifyFlag::kWaitUntilSuccess;
        bool do_nothing = flag & (flag_t) LeaseModifyFlag::kDoNothing;
        if (only_dealloc)
        {
            DCHECK(!with_dealloc) << "with_dealloc conflict with only_dealloc";
        }
        if (with_dealloc)
        {
            DCHECK(!only_dealloc) << "only_dealloc conflict with with_dealloc";
        }
        if (wait_success)
        {
            DCHECK(!no_relinquish_unbind)
                << "wait_success conflict with no_relinquish_unbind: If you do "
                   "not want to unbind, make no sense to wait for nothing.";
        }
        if (do_nothing)
        {
            CHECK(!with_dealloc) << "with_dealloc conflict with do_nothing";
            CHECK(!only_dealloc) << "only_dealloc conflict with do_nothing";
            CHECK(!wait_success) << "wait_success conflict with do_nothing";
        }
        DCHECK(!reserved);
    }
}

std::ostream &operator<<(std::ostream &os, LeaseModifyFlagOut flag)
{
    os << "{LeaseModifyFlag ";
    bool reserved = flag.flag & (flag_t) LeaseModifyFlag::kReserved;
    if (reserved)
    {
        os << "RESERVED, ";
    }
    bool no_relinquish_unbind =
        flag.flag & (flag_t) LeaseModifyFlag::kNoRelinquishUnbind;
    if (no_relinquish_unbind)
    {
        os << "no-rel-unbind, ";
    }
    bool force_unbind = flag.flag & (flag_t) LeaseModifyFlag::kForceUnbind;
    if (force_unbind)
    {
        os << "force-unbind, ";
    }
    bool with_dealloc = flag.flag & (flag_t) LeaseModifyFlag::kWithDeallocation;
    if (with_dealloc)
    {
        os << "with-dealloc, ";
    }
    bool only_dealloc = flag.flag & (flag_t) LeaseModifyFlag::kOnlyDeallocation;
    if (only_dealloc)
    {
        os << "only-dealloc, ";
    }
    bool wait_till_succ =
        flag.flag & (flag_t) LeaseModifyFlag::kWaitUntilSuccess;
    if (wait_till_succ)
    {
        os << "wait-success, ";
    }
    bool use_mr = flag.flag & (flag_t) LeaseModifyFlag::kUseMR;
    if (use_mr)
    {
        os << "use-MR, ";
    }
    bool do_nothing = flag.flag & (flag_t) LeaseModifyFlag::kDoNothing;
    if (do_nothing)
    {
        os << "do-nothing, ";
    }

    os << "}";
    return os;
}

std::ostream &operator<<(std::ostream &os, const AcquireRequest &req)
{
    os << "{AcquireRequest type: " << req.type << ", cid: " << req.cid
       << ", key: " << req.key << ", size: " << req.size
       << ", require_ns: " << req.required_ns << ", dir_id: " << req.dir_id
       << "}";
    return os;
}

std::ostream &operator<<(std::ostream &os, const AcquireResponse &resp)
{
    os << "{AcquireResponse type: " << resp.type << ", cid: " << resp.cid
       << ", lease_id: " << resp.lease_id << ", rkey_0: " << resp.rkey_0
       << ", rkey_header: " << resp.rkey_header << ", buffer_base "
       << resp.buffer_base << ", header_base: " << resp.header_base
       << ", begin_term: " << resp.begin_term
       << ", ns_per_unit: " << resp.ns_per_unit << ", status: " << resp.status
       << " }";
    return os;
}

std::ostream &operator<<(std::ostream &os, const AdminRequest &req)
{
    os << "{AdminRequest type: " << req.type << ", cid: " << req.cid
       << "flags: " << (int) req.flag << " }";
    return os;
}

std::ostream &operator<<(std::ostream &os, const LeaseModifyRequest &req)
{
    os << "{LeaseModifyRequest type: " << req.type << ", cid: " << req.cid
       << ", lease_id: " << req.lease_id << ", ns: " << req.ns
       << ", hint: " << req.hint << ", flag: " << LeaseModifyFlagOut(req.flag)
       << " }";
    return os;
}
std::ostream &operator<<(std::ostream &os, const LeaseModifyResponse &resp)
{
    os << "{LeaseModifyResponse type: " << resp.type << ", cid : " << resp.cid
       << ", lease_id: " << resp.lease_id << ", success: " << resp.success
       << " }";
    return os;
}

}  // namespace patronus