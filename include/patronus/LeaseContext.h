#pragma once
#ifndef PATRONUS_LEASE_CONTEXT_H_
#define PATRONUS_LEASE_CONTEXT_H_

#include <infiniband/verbs.h>

#include <cstddef>
#include <cstdint>

#include "patronus/Type.h"

namespace patronus
{
struct LeaseContext
{
    // correctness related fields:
    // will refuse relinquish requests from unrelated clients.
    ClientID client_cid;
    bool valid;
    // end
    ibv_mw *buffer_mw;
    ibv_mw *header_mw;
    ibv_mr *buffer_mr;
    ibv_mr *header_mr;
    size_t dir_id{size_t(-1)};
    uint64_t addr_to_bind{0};
    size_t buffer_size{0};
    size_t protection_region_id;
    // about with_conflict_detect
    bool with_conflict_detect{false};
    bool with_pr{true};
    bool with_buf{true};
    uint64_t key_bucket_id{0};
    uint64_t key_slot_id{0};
    uint64_t hint{0};
};
}  // namespace patronus

#endif