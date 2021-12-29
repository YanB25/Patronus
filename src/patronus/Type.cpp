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
    default:
    {
        os << "Unknown(" << (int) t << ")";
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

std::ostream &operator<<(std::ostream &os, const AcquireRequest &req)
{
    os << "{AcquireRequest type: " << req.type << ", cid: " << req.cid
       << ", key: " << req.key << ", size: " << req.size
       << ", require_term: " << req.require_term << ", dir_id: " << req.dir_id
       << "}";
    return os;
}

std::ostream &operator<<(std::ostream &os, const AcquireResponse &resp)
{
    os << "{AcquireResponse type: " << resp.type << ", cid: " << resp.cid
       << ", rkey_0: " << resp.rkey_0 << ", base " << resp.base
       << ", term: " << resp.term << "}";
    return os;
}

}  // namespace patronus