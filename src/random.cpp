//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "random.h"

#include <stdint.h>
#include <string.h>

#include <thread>
#include <utility>

Random *Random::GetTLSInstance()
{
    thread_local Random *tls_instance;
    thread_local std::aligned_storage<sizeof(Random)>::type tls_instance_bytes;

    auto rv = tls_instance;
    if (rv == nullptr)
    {
        size_t seed = std::hash<std::thread::id>()(std::this_thread::get_id());
        rv = new (&tls_instance_bytes) Random((uint32_t) seed);
        tls_instance = rv;
    }
    return rv;
}
