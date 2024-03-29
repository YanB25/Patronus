#include "patronus/Type.h"

#include <iostream>

namespace patronus
{
std::ostream &operator<<(std::ostream &os, const RpcType &t)
{
    switch (t)
    {
    case RpcType::kAcquireRLeaseReq:
    {
        os << "AcR-req";
        break;
    }
    case RpcType::kAcquireWLeaseReq:
    {
        os << "AcW-req";
        break;
    }
    case RpcType::kAcquireLeaseResp:
    {
        os << "AcW-resp";
        break;
    }
    case RpcType::kRelinquishReq:
    {
        os << "Rel-req";
        break;
    }
    case RpcType::kRelinquishResp:
    {
        os << "Rel-resp";
        break;
    }
    case RpcType::kAdmin:
    {
        os << "Adm";
        break;
    }
    case RpcType::kAdminReq:
    {
        os << "Adm-req";
        break;
    }
    case RpcType::kAdminResp:
    {
        os << "Adm-resp";
        break;
    }
    case RpcType::kTimeSync:
    {
        os << "TimeSync";
        break;
    }
    case RpcType::kExtendReq:
    {
        os << "Ext-req";
        break;
    }
    case RpcType::kExtendResp:
    {
        os << "Ext-resp";
        break;
    }
    case RpcType::kMemoryReq:
    {
        os << "Mem-req";
        break;
    }
    case RpcType::kMemoryResp:
    {
        os << "Mem-resp";
        break;
    }
    case RpcType::kMemcpyReq:
    {
        os << "Memcpy-req";
        break;
    }
    case RpcType::kMemcpyResp:
    {
        os << "Memcpy-resp";
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
    case AdminFlag::kAdminQPtoRO:
    {
        os << "QP2RO";
        break;
    }
    case AdminFlag::kAdminQPtoRW:
    {
        os << "QP2RW";
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
       << ", coro: " << (int) cid.coro_id << ", rpc_ctx_id: " << cid.rpc_ctx_id
       << "}";
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
        bool no_rpc = flag & (flag_t) AcquireRequestFlag::kNoRpc;
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
            DCHECK(no_bind_pr) << "Set no-bind-pr too.";
            DCHECK(!with_conflict_detect)
                << "Allocation semantics will not detect conflict";
            DCHECK(!use_mr) << "Conflict with use_mr";
            DCHECK(!no_rpc) << "Conflict with no_rpc";
        }
        if (use_mr)
        {
            DCHECK(no_gc)
                << "Not sure: If using MR, not guarantee to work with lease. "
                   "And the performance will be terrible in my expection";
        }
        if (no_rpc)
        {
            CHECK(!with_conflict_detect)
                << "with_conflict_detect conflict with do_nothing. "
                << ", ";
            CHECK(!with_alloc)
                << "with_alloc conflict with do_nothing. " << no_rpc;
            CHECK(!only_alloc)
                << "only_alloc conflict with do_nothing. " << no_rpc;
            CHECK(!use_mr) << "use_mr conflict with do_nothing. " << no_rpc;
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
    bool no_rpc = flag.flag & (flag_t) AcquireRequestFlag::kNoRpc;
    if (no_rpc)
    {
        os << "no-rpc, ";
    }
    bool debug_srv_do_nothing =
        flag.flag & (flag_t) AcquireRequestFlag::kDebugServerDoNothing;
    if (debug_srv_do_nothing)
    {
        os << "dbg-svr-do-nothing, ";
    }
    bool debug_1 = flag.flag & (flag_t) AcquireRequestFlag::kDebugFlag_1;
    if (debug_1)
    {
        os << "dbg-flg-1, ";
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
    bool enable_trace = flag.flag & (flag_t) RWFlag::kEnableTrace;
    if (enable_trace)
    {
        os << "enable-trace, ";
    }
    bool use_two_sided = flag.flag & (flag_t) RWFlag::kUseTwoSided;
    if (use_two_sided)
    {
        os << "use-two-sided, ";
    }
    bool use_two_sided_auto_pack =
        flag.flag & (flag_t) RWFlag::kUseTwoSidedAutoPacking;
    if (use_two_sided_auto_pack)
    {
        os << "use-two-sided-auto-pack, ";
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
        [[maybe_unused]] bool no_unbind_any =
            flag & (flag_t) LeaseModifyFlag::kNoRelinquishUnbindAny;
        [[maybe_unused]] bool no_unbind_pr =
            flag & (flag_t) LeaseModifyFlag::kNoRelinquishUnbindPr;
        [[maybe_unused]] bool force_unbind =
            flag & (flag_t) LeaseModifyFlag::kForceUnbind;
        bool with_dealloc = flag & (flag_t) LeaseModifyFlag::kWithDeallocation;
        bool only_dealloc = flag & (flag_t) LeaseModifyFlag::kOnlyDeallocation;
        bool reserved = flag & (flag_t) LeaseModifyFlag::kReserved;
        bool wait_success = flag & (flag_t) LeaseModifyFlag::kWaitUntilSuccess;
        bool no_rpc = flag & (flag_t) LeaseModifyFlag::kNoRpc;
        bool srv_do_nothing = flag & (flag_t) LeaseModifyFlag::kServerDoNothing;
        if (only_dealloc)
        {
            DCHECK(!with_dealloc) << "with_dealloc conflict with only_dealloc";
            CHECK(!srv_do_nothing);
        }
        if (with_dealloc)
        {
            DCHECK(!only_dealloc) << "only_dealloc conflict with with_dealloc";
            CHECK(!srv_do_nothing);
        }
        if (wait_success)
        {
            DCHECK(!no_unbind_any)
                << "wait_success conflict with no_relinquish_unbind: If you do "
                   "not want to unbind, make no sense to wait for nothing.";
            CHECK(!srv_do_nothing);
        }
        if (no_rpc)
        {
            DCHECK(!no_unbind_any);
            DCHECK(!no_unbind_pr);
            DCHECK(!force_unbind);
            DCHECK(!with_dealloc);
            DCHECK(!only_dealloc);
            DCHECK(!wait_success);
        }
        if (srv_do_nothing)
        {
            CHECK(!force_unbind);
            CHECK(!with_dealloc);
            CHECK(!only_dealloc);
        }
        DCHECK(!(no_unbind_any && no_unbind_pr))
            << "Only one of no_unbind_any and no_unbind_pr should be set";
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
    bool no_unbind_any =
        flag.flag & (flag_t) LeaseModifyFlag::kNoRelinquishUnbindAny;
    if (no_unbind_any)
    {
        os << "no-unbind-any, ";
    }
    bool no_unbind_pr =
        flag.flag & (flag_t) LeaseModifyFlag::kNoRelinquishUnbindPr;
    if (no_unbind_pr)
    {
        os << "no-unbind-pr, ";
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
    bool no_rpc = flag.flag & (flag_t) LeaseModifyFlag::kNoRpc;
    if (no_rpc)
    {
        os << "no-rpc, ";
    }

    os << "}";
    return os;
}

std::ostream &operator<<(std::ostream &os, const AcquireRequest &req)
{
    os << "{AcquireRequest type: " << req.type << ", cid: " << req.cid
       << ", key: " << req.key << ", size: " << req.size
       << ", require_ns: " << req.required_ns << ", dir_id: " << req.dir_id
       << ", flag: " << AcquireRequestFlagOut(req.flag) << "}";
    return os;
}

std::ostream &operator<<(std::ostream &os, const AcquireResponse &resp)
{
    os << "{AcquireResponse type: " << resp.type << ", cid: " << resp.cid
       << ", lease_id: " << resp.lease_id << ", rkey_0: " << resp.rkey_0
       << ", rkey_header: " << resp.rkey_header << ", buffer_base "
       << (void *) resp.buffer_base
       << ", header_base: " << (void *) resp.header_base
       << ", begin_term: " << resp.begin_term
       << ", ns_per_unit: " << resp.ns_per_unit << ", status: " << resp.status
       << ", aba_id: " << resp.aba_id << " }";
    return os;
}

std::ostream &operator<<(std::ostream &os, const AdminRequest &req)
{
    os << "{AdminRequest type: " << req.type << ", cid: " << req.cid
       << "flags: " << req.flag << ", dir_id: " << req.dir_id
       << ", data: " << req.data << ", need_resp: " << req.need_response
       << " }";
    return os;
}

std::ostream &operator<<(std::ostream &os, const AdminResponse &resp)
{
    os << "{AdminResponse type: " << resp.type << ", cid: " << resp.cid
       << ", flag: " << (AdminFlag) resp.flag << ", success: " << resp.success
       << "}";
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
       << ", success: " << resp.success << " }";
    return os;
}

std::ostream &operator<<(std::ostream &os, MemoryRequestFlag flag)
{
    os << "{MemoryRequestFlag ";
    switch (flag)
    {
    case MemoryRequestFlag::kRead:
        os << "Read";
        break;
    case MemoryRequestFlag::kWrite:
        os << "Write";
        break;
    case MemoryRequestFlag::kCAS:
        os << "CAS";
        break;
    case MemoryRequestFlag::kFAA:
        os << "FAA";
        break;
    default:
        LOG(FATAL) << "** Unknown flag " << (int) flag;
    }
    return os;
}

std::ostream &operator<<(std::ostream &os, const MemoryRequest &req)
{
    os << "{MemoryRequest type: " << req.type << ", cid: " << req.cid
       << ", flag: " << (MemoryRequestFlag) req.flag
       << ", remote_addr: " << (void *) req.remote_addr
       << ", size: " << (size_t) req.size << "}";
    return os;
}
std::ostream &operator<<(std::ostream &os, const MemoryResponse &resp)
{
    os << "{MemoryResponse type: " << resp.type << ", cid: " << resp.cid
       << ", flag: " << (MemoryRequestFlag) resp.flag
       << ", success: " << resp.success << ", size: " << resp.size << "}";
    return os;
}

std::ostream &operator<<(std::ostream &os, const MemcpyRequest &req)
{
    // os << "{MemcpyRequest type: " << req.type << ", cid: " << req.cid
    //    << ", source: " << (void *) req.source
    //    << ", target: " << (void *) req.target << ", size: " << req.size <<
    //    "}";
    os << "{MemcpyRequest type: " << req.type << ", cid: " << req.cid
       << ", times: " << req.times << ", size: " << req.size << "}";
    return os;
}

std::ostream &operator<<(std::ostream &os, const MemcpyResponse &resp)
{
    os << "{MemcpyResponse type: " << resp.type << ", cid: " << resp.cid << "}";
    return os;
}

}  // namespace patronus