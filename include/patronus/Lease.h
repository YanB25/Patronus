#pragma once
#ifndef PATRONUS_LEASE_H_
#define PATRONUS_LEASE_H_

#include <iostream>

#include "patronus/LeaseCache.h"
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
    Lease(Lease &&rhs) = default;
    Lease &operator=(Lease &&rhs) = default;

    // It is the dsm offset
    // @see DSM
    uint64_t base_addr() const
    {
        return base_addr_;
    }
    bool success() const
    {
        return status_ == AcquireRequestStatus::kSuccess;
    }
    bool ready() const
    {
        return ready_;
    }
    bool error() const
    {
        return !success();
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
    AcquireRequestStatus ec() const
    {
        return status_;
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
        cur_rkey_ = rhs.cur_rkey_;
        header_rkey_ = rhs.header_rkey_;
        begin_term_ = rhs.begin_term_;
        ns_per_unit_ = rhs.ns_per_unit_;
        aba_unit_nr_to_ddl_ = rhs.aba_unit_nr_to_ddl_;
        ddl_term_ = rhs.ddl_term_;
        no_gc_ = rhs.no_gc_;
        cur_unit_nr_ = rhs.cur_unit_nr_;
        post_qp_idx_ = rhs.post_qp_idx_;

        status_ = AcquireRequestStatus::kReserved;
        ready_ = false;
        id_ = 0;
    }
    auto begin_term() const
    {
        return begin_term_;
    }
    auto aba() const
    {
        return aba_unit_nr_to_ddl_.u32_1;
    }
    // currently use doubling. could use other policy.
    size_t next_extend_unit_nr()
    {
        DCHECK_GT(cur_unit_nr_, 0);
        auto ret = cur_unit_nr_;
        cur_unit_nr_ *= 2;
        return ret;
    }

    // about lease cache
    bool cache_query(uint64_t addr, size_t len, char *obuf)
    {
        return cache_.query(addr, len, obuf);
    }
    void cache_insert(uint64_t addr, size_t len, const char *ibuf)
    {
        cache_.insert(addr, len, ibuf);
    }

private:
    void set_finish()
    {
        ready_ = true;
        status_ = AcquireRequestStatus::kSuccess;
    }
    void set_error(AcquireRequestStatus status)
    {
        ready_ = true;
        status_ = status;
    }
    void set_invalid()
    {
        DCHECK(ready_) << "Why setting a not-ready lease to invalid?";
        ready_ = false;
        status_ = AcquireRequestStatus::kReserved;
        id_ = 0;
        cur_rkey_ = 0;
        ddl_term_ = 0;
    }

    void update_ddl_term()
    {
        if (no_gc_)
        {
            ddl_term_ = time::PatronusTime::max();
        }
        else
        {
            DCHECK_NE(ns_per_unit_, 0)
                << "** ns_per_unit_ is zero. lease: " << *this;
            DCHECK_NE(aba_unit_nr_to_ddl_, 0) << "** invalid. Lease: " << *this;
            auto ns = ns_per_unit_ * aba_unit_nr_to_ddl_.u32_2;
            ddl_term_ = begin_term_ + ns;
        }
    }

    friend class Patronus;
    friend std::ostream &operator<<(std::ostream &, const Lease &);
    AcquireRequestStatus status_{AcquireRequestStatus::kReserved};
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
    rkey_t cur_rkey_{0};
    rkey_t header_rkey_{0};

    // for the management of Lease life cycle
    time::term_t begin_term_{0};
    time::ns_t ns_per_unit_{0};
    // @aba_unit_to_ddl_ .u32_1 is aba, .u32_2 is unit_nr_to_ddl
    compound_uint64_t aba_unit_nr_to_ddl_{0};
    time::PatronusTime ddl_term_{0};
    bool no_gc_{false};

    // for extend policy
    size_t cur_unit_nr_{1};

    LeaseCache<::config::kLeaseCacheItemLimitNr> cache_;
    uint32_t post_qp_idx_{0};
};

std::ostream &operator<<(std::ostream &os, const Lease &lease);
}  // namespace patronus
#endif