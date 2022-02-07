#include "patronus/Lease.h"

namespace patronus
{
std::ostream &operator<<(std::ostream &os, const Lease &lease)
{
    os << "{" << (lease.lease_type_ == LeaseType::kReadLease ? "R" : "W")
       << "Lease base_addr: " << (void *) lease.base_addr_
       << ", header_addr: " << (void *) lease.header_addr_
       << ", node: " << lease.node_id_ << ", dir_id: " << lease.dir_id_
       << ", header: " << lease.header_size_
       << ", buffer: " << lease.buffer_size_ << ", rkey_0: " << lease.rkey_0_
       << ", cur_rkey: " << lease.cur_rkey_
       << ", header_rkey: " << lease.header_rkey_
       << ", begin_term_: " << lease.begin_term_
       << ", ns_per_unit_: " << lease.ns_per_unit_
       << ", unit_nr_begin_to_ddl_: " << lease.unit_nr_begin_to_ddl_
       << ", cur_unit_nr_: " << lease.cur_unit_nr_
       << ", ddl_term: " << lease.ddl_term_ << "}";
    return os;
}
}  // namespace patronus