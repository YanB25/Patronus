#pragma once
#ifndef PROTECTION_REGION_H_
#define PROTECTION_REGION_H_

namespace patronus
{
constexpr static size_t kSpeculativeLeaseNr = 4;
using small_size_t = uint8_t;
using small_bit_t = uint8_t;

struct LeaseDescriptor
{
    uint32_t rkey;
    term_t term;
}__attribute__((packed));

struct ProtectionRegionMeta
{
    small_bit_t granted;
    small_bit_t relinquished;
    small_bit_t wait;
}__attribute__((packed));
static_assert(sizeof(ProtectionRegionMeta::granted) * 8 <= kSpeculativeLeaseNr);
static_assert(sizeof(ProtectionRegionMeta::relinquished) * 8 <= kSpeculativeLeaseNr);

struct ProtectionRegion
{
    small_size_t lease_nr;
    // this covers the Meta part
    LeaseDescriptor ex_lease;
    // these cover the buffer part
    LeaseDescriptor spec_leases[kSpeculativeLeaseNr];
    ProtectionRegionMeta meta;
    // need external data to determine the buffer length.
    char buf[0];
}__attribute__((packed));

// TODO: this design is too complicated.
// just ignore it for now.
struct SplitProtectionRegion
{
    small_size_t lease_nr;
    LeaseDescriptor ex_lease;
    LeaseDescriptor spec_leases[kSpeculativeLeaseNr];
    LeaseDescriptor spec_ex_leases[kSpeculativeLeaseNr];
    ProtectionRegionMeta meta;
    // pointer to the actual buffer
    char* buf;
}

}

#endif