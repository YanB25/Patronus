#pragma once
#ifndef PATRONUS_LEASE_H_
#define PATRONUS_LEASE_H_

#include <iostream>

#include "patronus/Time.h"
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
    bool is_readable() const
    {
        return lease_type_ == LeaseType::kReadLease ||
               lease_type_ == LeaseType::kWriteLease;
    }
    bool is_writable() const
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
    time::PatronusTime ddl_term() const
    {
        return ddl_term_;
    }
    auto cur_rkey() const
    {
        return cur_rkey_;
    }

    void copy_from(const Lease &rhs)
    {
        lease_type_ = rhs.lease_type_;
        base_addr_ = rhs.base_addr_;
        header_addr_ = rhs.header_addr_;
        node_id_ = rhs.node_id_;
        dir_id_ = rhs.dir_id_;
        header_size_ = rhs.header_size_;
        buffer_size_ = rhs.buffer_size_;
        rkey_0_ = rhs.rkey_0_;
        cur_rkey_ = rhs.cur_rkey_;
        header_rkey_ = rhs.header_rkey_;
        begin_term_ = rhs.begin_term_;
        ns_per_unit_ = rhs.ns_per_unit_;
        unit_nr_begin_to_ddl_ = rhs.unit_nr_begin_to_ddl_;
        cur_unit_nr_ = rhs.cur_unit_nr_;
        ddl_term_ = rhs.ddl_term_;

        success_ = false;
        ready_ = false;
        id_ = 0;
    }
    auto begin_term() const
    {
        return begin_term_;
    }
    auto cur_unit_nr() const
    {
        return cur_unit_nr_.u32_2;
    }
    auto aba_id() const
    {
        return cur_unit_nr_.u32_1;
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
        ddl_term_ = 0;
    }

    void update_ddl_term()
    {
        DCHECK_NE(ns_per_unit_, 0)
            << "** ns_per_unit_ is zero. lease: " << *this;
        DCHECK_NE(unit_nr_begin_to_ddl_, 0) << "** invalid. Lease: " << *this;
        auto ns = ns_per_unit_ * unit_nr_begin_to_ddl_;
        ddl_term_ = begin_term_ + ns;
    }

    friend class Patronus;
    friend std::ostream &operator<<(std::ostream &, const Lease &);
    bool success_{false};
    bool ready_{false};
    // basical information
    id_t id_{0};
    LeaseType lease_type_{LeaseType::kUnknown};
    uint64_t base_addr_{0};
    uint64_t header_addr_{0};
    uint16_t node_id_{0};
    size_t dir_id_{0};
    size_t buffer_size_{0};
    size_t header_size_{0};
    rkey_t rkey_0_{0};
    rkey_t cur_rkey_{0};
    rkey_t header_rkey_{0};

    // for the management of Lease life cycle
    time::term_t begin_term_{0};
    time::ns_t ns_per_unit_{0};
    size_t unit_nr_begin_to_ddl_{0};
    compound_uint64_t cur_unit_nr_{0};
    time::PatronusTime ddl_term_{0};
    bool no_gc_{false};
};

std::ostream &operator<<(std::ostream &os, const Lease &lease);
}  // namespace patronus
#endif