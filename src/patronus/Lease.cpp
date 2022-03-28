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
// Lease::Lease(Lease &&rhs)
// {
//     status_ = rhs.status_;
//     rhs.status_ = AcquireRequestStatus::kReserved;
//     ready_ = rhs.ready_;
//     rhs.ready_ = false;
//     id_ = rhs.id_;
//     rhs.id_ = false;
//     lease_type_ = rhs.lease_type_;
//     rhs.lease_type_ = LeaseType::kUnknown;
//     base_addr_ = rhs.base_addr_;
//     rhs.base_addr_ = 0;
//     header_addr_ = rhs.header_addr_;
//     rhs.header_addr_;
//     node_id_ = rhs.node_id_;
//     rhs.node_id_ = 0;
//     dir_id_ = rhs.dir_id_;
//     rhs.dir_id_ = 0;
//     buffer_size_ = rhs.buffer_size_;
//     rhs.buffer_size_ = 0;
//     header_size_ = rhs.header_size_;
//     rhs.header_size_ = 0;
//     cur_rkey_ = rhs.cur_rkey_;
//     rhs.cur_rkey_ = 0;
//     header_rkey_ = rhs.header_addr_;
//     rhs.header_rkey_ = 0;
//     begin_term_ = rhs.begin_term_;
//     rhs.begin_term_ = 0;
//     rhs.ns_per_unit_ = rhs.ns_per_unit_;
//     rhs.ns_per_unit_ = 0;
//     aba_unit_nr_to_ddl_ = rhs.aba_unit_nr_to_ddl_;
//     rhs.aba_unit_nr_to_ddl_.val = 0;
//     ddl_term_ = rhs.ddl_term_;
//     rhs.ddl_term_ = 0;
//     no_gc_ = rhs.no_gc_;
//     rhs.no_gc_ = 0;
//     cur_unit_nr_ = rhs.cur_unit_nr_;
//     rhs.cur_unit_nr_ = 0;
//     cache_ = std::move(rhs.cache_);
// }
}  // namespace patronus