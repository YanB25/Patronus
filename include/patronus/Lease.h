#ifndef PATRONUS_LEASE_H_
#define PATRONUS_LEASE_H_

#include "patronus/Type.h"
#include <iostream>
namespace patronus
{
enum class LeaseType
{
    kReadLease,
    kWriteLease,
    kUnknown,
};

// TODO(patronus): Change to use Result.
class Lease
{
public:
    Lease() = default;
    uint64_t base_addr() const
    {
        return base_addr_;
    }
    bool success() const
    {
        return success_;
    }

private:
    friend class Patronus;
    friend std::ostream &operator<<(std::ostream&, const Lease&);
    bool success_{false};
    // basical information
    LeaseType lease_type_{LeaseType::kUnknown};
    uint64_t base_addr_{0};
    uint16_t node_id_{0};
    // total_size = header_size_ + buffer_size_
    size_t header_size_{0};
    size_t buffer_size_{0};
    rkey_t rkey_0_{0};
    rkey_t cur_rkey_{0};
    rkey_t ex_rkey_{0};
    term_t cur_ddl_term_{0};
};

std::ostream &operator<<(std::ostream &os, const Lease &lease);
}  // namespace patronus
#endif