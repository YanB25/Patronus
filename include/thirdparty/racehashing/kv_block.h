#pragma once
#ifndef PERTRONUS_RACEHASHING_KV_BLOCK_H_
#define PERTRONUS_RACEHASHING_KV_BLOCK_H_

#include <cstdint>
#include <cstring>

#include "./utils.h"

namespace patronus::hash
{
struct KVBlock
{
    // TODO: add checksum to KVBlock
    // Paper sec 3.3, there will be a corner case where one client is reading
    // @key and @value from the KVBlock. Meanwhile, the KVBlock is freed,
    // re-allocated, and be filled with the same @key but different @value.
    // TO detect this inconsistency, add checksum to the KVBlock.
    uint32_t key_len;
    uint32_t value_len;
    uint64_t hash;
    char buf[0];
    static KVBlock *new_instance(const Key &key,
                                 const Value &value,
                                 uint64_t hash)
    {
        auto expect_size = sizeof(KVBlock) + key.size() + value.size();
        auto &ret = *(KVBlock *) malloc(expect_size);
        ret.key_len = key.size();
        ret.value_len = value.size();
        ret.hash = hash;
        memcpy(ret.buf, key.data(), key.size());
        memcpy(ret.buf + key.size(), value.data(), value.size());
        return &ret;
    }
} __attribute__((packed));

}  // namespace patronus::hash
#endif