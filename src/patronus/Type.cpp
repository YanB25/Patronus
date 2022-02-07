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
    if (flag.flag & (uint8_t) AcquireRequestFlag::kNoGc)
    {
        os << "no-gc, ";
    }
    if (flag.flag & (uint8_t) AcquireRequestFlag::kWithConflictDetect)
    {
        os << "with-lock, ";
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
       << resp.buffer_base << "< header_base: " << resp.header_base
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