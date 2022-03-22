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
       << ", buffer: " << lease.buffer_size_
       << ", cur_rkey: " << lease.cur_rkey_
       << ", header_rkey: " << lease.header_rkey_
       << ", begin_term_: " << lease.begin_term_
       << ", ns_per_unit_: " << lease.ns_per_unit_
       << ", aba_unit_nr_to_ddl: " << lease.aba_unit_nr_to_ddl_
       << ", ddl_term: " << lease.ddl_term_ << "}";
    return os;
}
}  // namespace patronus