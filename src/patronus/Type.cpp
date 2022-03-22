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

std::ostream &operator<<(std::ostream &os, AcquireRequestFlagOut flag)
{
    os << "{AcquireRequestFlag ";
    bool no_gc = flag.flag & (uint8_t) AcquireRequestFlag::kNoGc;
    if (no_gc)
    {
        os << "no-gc, ";
    }
    bool with_conflict_detect =
        flag.flag & (uint8_t) AcquireRequestFlag::kWithConflictDetect;
    if (with_conflict_detect)
    {
        os << "with-lock, ";
    }
    bool debug_no_bind_pr =
        flag.flag & (uint8_t) AcquireRequestFlag::kDebugNoBindPR;
    if (debug_no_bind_pr)
    {
        os << "no-pr, ";
    }
    bool debug_no_bind_any =
        flag.flag & (uint8_t) AcquireRequestFlag::kDebugNoBindAny;
    if (debug_no_bind_any)
    {
        os << "no-any, ";
    }
    bool type_alloc = flag.flag & (uint8_t) AcquireRequestFlag::kTypeAllocation;
    if (type_alloc)
    {
        os << "alloc, ";
        DCHECK(no_gc) << "Set no-gc for allocation semantics.";
        DCHECK(!with_conflict_detect)
            << "Allocation semantics will not detect conflict";
        DCHECK(!debug_no_bind_any) << "Allocation semantics will not bind any";
        DCHECK(!debug_no_bind_pr) << "Allocation semantics will not bind pr";
    }
    bool reserved = flag.flag & (uint8_t) AcquireRequestFlag::kReserved;
    DCHECK(!reserved);
    os << "}";
    return os;
}

std::ostream &operator<<(std::ostream &os, RWFlagOut flag)
{
    os << "{RWFlag ";
    bool no_local_expire_check =
        flag.flag & (uint8_t) RWFlag::kNoLocalExpireCheck;
    if (no_local_expire_check)
    {
        os << "no-check, ";
    }
    bool with_auto_extend = flag.flag & (uint8_t) RWFlag::kWithAutoExtend;
    if (with_auto_extend)
    {
        os << "with-extend, ";
    }
    bool with_cache = flag.flag & (uint8_t) RWFlag::kWithCache;
    if (with_cache)
    {
        os << "with-cache, ";
    }
    bool reserve = flag.flag & (uint8_t) RWFlag::kReserved;
    DCHECK(!reserve);
    os << "}";
    return os;
}

std::ostream &operator<<(std::ostream &os, LeaseModifyFlagOut flag)
{
    os << "{LeaseModifyFlag ";
    bool no_relinquish_unbind =
        flag.flag & (uint8_t) LeaseModifyFlag::kNoRelinquishUnbind;
    if (no_relinquish_unbind)
    {
        os << "no-rel-unbind, ";
    }
    bool force_unbind = flag.flag & (uint8_t) LeaseModifyFlag::kForceUnbind;
    if (force_unbind)
    {
        os << "force-unbind, ";
    }
    bool dealloc = flag.flag & (uint8_t) LeaseModifyFlag::kTypeDeallocation;
    if (dealloc)
    {
        os << "de-alloc";
    }
    bool reserve = flag.flag & (uint8_t) LeaseModifyFlag::kReserved;
    DCHECK(!reserve);

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
       << ", flag: " << req.flag << " }";
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