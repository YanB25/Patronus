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
        os << "R";
        break;
    }
    case RequestType::kAcquireWLease:
    {
        os << "W";
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
       << ", ddl_term: " << resp.ddl_term << ", success: " << resp.success
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
       << ", lease_id: " << req.lease_id << ", ns: " << req.ns << " }";
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