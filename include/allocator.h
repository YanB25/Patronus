//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Abstract interface for allocating memory in blocks. This memory is freed
// when the allocator object is destroyed. See the Arena class for more info.

#pragma once
#include <cerrno>
#include <cstddef>
class Allocator
{
public:
    char *Allocate(size_t bytes)
    {
        return AllocateAligned(bytes);
    }
    char *AllocateAligned(size_t bytes, size_t huge_page_size = 0)
    {
        return (char *)aligned_alloc(8, bytes);
    }
};
