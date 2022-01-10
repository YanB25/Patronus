#pragma once
#ifndef PATRONUS_LEASE_H_
#define PATRONUS_LEASE_H_

#include <iostream>

#include "patronus/Type.h"
namespace patronus
{
enum class LeaseType
{
    kReadLease,
    kWriteLease,
    kUnknown,
};

// wont fix: do not use result.
// it has problems of lifecycle management.
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
    bool ready() const
    {
        return ready_;
    }
    bool error() const
    {
        return !success_;
    }
    bool is_read_lease() const
    {
        return lease_type_ == LeaseType::kReadLease;
    }
    bool is_write_lease() const
    {
        return lease_type_ == LeaseType::kWriteLease;
    }
    id_t id() const
    {
        return id_;
    }
    size_t dir_id() const
    {
        return dir_id_;
    }

    void copy_from(const Lease& rhs)
    {
        lease_type_ = rhs.lease_type_;
        base_addr_ = rhs.base_addr_;
        node_id_ = rhs.node_id_;
        dir_id_ = rhs.dir_id_;
        header_size_ = rhs.header_size_;
        buffer_size_ = rhs.buffer_size_;
        rkey_0_ = rhs.rkey_0_;
        cur_rkey_ = rhs.cur_rkey_;
        ex_rkey_ = rhs.ex_rkey_;
        cur_ddl_term_ = rhs.cur_ddl_term_;

        success_ = false;
        ready_ = false;
        id_ = 0;
    }


private:
    void set_finish()
    {
        ready_ = true;
        success_ = true;
    }
    void set_error()
    {
        ready_ = true;
        success_ = false;
    }
    void set_invalid()
    {
        DCHECK(ready_) << "Why setting a not-ready lease to invalid?";
        ready_ = false;
        success_ = false;
        id_ = 0;
        cur_rkey_ = 0;
        rkey_0_ = 0;
        cur_ddl_term_ = 0;
    }
    friend class Patronus;
    friend std::ostream &operator<<(std::ostream &, const Lease &);
    bool success_{false};
    bool ready_{false};
    // basical information
    id_t id_{0};
    LeaseType lease_type_{LeaseType::kUnknown};
    uint64_t base_addr_{0};
    uint16_t node_id_{0};
    size_t dir_id_{0};
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