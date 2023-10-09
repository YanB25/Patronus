#pragma once
#ifndef PROTECTION_REGION_H_
#define PROTECTION_REGION_H_

#include <cinttypes>
#include <cstddef>

#include "patronus/Type.h"

namespace patronus
{
constexpr static size_t kSpeculativeLeaseNr = 4;
using small_size_t = uint8_t;
using small_bit_t = uint8_t;

struct LeaseDescriptor
{
    uint32_t rkey;
    time::term_t term;
} __attribute__((packed));

struct ProtectionRegionMeta
{
    std::atomic<small_bit_t> relinquished;
    std::atomic<small_bit_t> wait;
} __attribute__((packed));
static_assert(sizeof(ProtectionRegionMeta::relinquished) * 8 >=
              kSpeculativeLeaseNr);
inline std::ostream &operator<<(std::ostream &os,
                                const ProtectionRegionMeta &meta)
{
    os << "{PRMeta relinquished: " << (int) meta.relinquished
       << ", wait: " << (int) meta.wait << "}";

    return os;
}

// NOTE: could not be __attribute__((packed))
// because the client will try to CAS @aba_unit_to_ddl
// This field must be 8-byte aligned.
struct ProtectionRegion
{
    // the lifecycle of lease
    bool valid{false};
    time::term_t begin_term{0};
    time::ns_t ns_per_unit{0};
    // @aba_unit_to_ddl client and server cas this field for lease extension.
    // The high bits will add one each time to avoid the ABA problem.
    std::atomic<compound_uint64_t> aba_unit_nr_to_ddl{compound_uint64_t(0)};
    // will be modified by clients or servers
    ProtectionRegionMeta meta;
};

inline std::ostream &operator<<(std::ostream &os, const ProtectionRegion &pr)
{
    os << "{ProtectionRegion valid: " << pr.valid
       << ", begin_term: " << pr.begin_term
       << ", ns_per_unit: " << pr.ns_per_unit << ", aba_unit_to_ddl "
       << pr.aba_unit_nr_to_ddl << ", meta: " << pr.meta << "}";
    return os;
}

}  // namespace patronus

#endif