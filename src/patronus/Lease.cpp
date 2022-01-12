#include "patronus/Lease.h"

namespace patronus
{
std::ostream &operator<<(std::ostream &os, const Lease &lease)
{
    os << "{" << (lease.lease_type_ == LeaseType::kReadLease ? "R" : "W")
       << "Lease base_addr: " << (void *) lease.base_addr_
       << ", header_addr: " << (void *) lease.header_addr_
       << ", node: " << lease.node_id_ << ", header: " << lease.header_size_
       << ", buffer: " << lease.buffer_size_ << ", rkey_0: " << lease.rkey_0_
       << ", cur_rkey: " << lease.cur_rkey_
       << ", header_rkey: " << lease.header_rkey_
       << ", cur_ddl: " << lease.cur_ddl_term_ << "}";
    return os;
}
}  // namespace patronus