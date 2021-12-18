#pragma once
#ifndef POOL_H_
#define POOL_H_

#include <atomic>

template <size_t kObjSize>
class Pool
{
public:
    Pool(void *base, size_t length) : base_(base), length_(length)
    {
        CHECK_GT(length, kObjSize)
            << "length should at least be greater than ONE object size";
        allocated_.resize(obj_nr());
    }

    void *get()
    {
        size_t tried = 0;
        while (true)
        {
            size_t my_idx =
                (idx_.fetch_add(1, std::memory_order_relaxed) % obj_nr());
            if (allocated_[my_idx])
            {
                if (tried++ > obj_nr())
                {
                    return nullptr;
                }
                continue;
            }
            allocated_[my_idx] = true;
            return base_ + kObjSize * my_idx;
        }
    }
    void put(const void *buf)
    {
        char *buf_addr = (char *) buf;
        DCHECK_GT(buf_addr, base_);
        DCHECK_EQ((buf_addr - base_) % kObjSize, 0);
        size_t idx = (buf_addr - base_) / kObjSize;
        DCHECK(allocated_[idx]);
        allocated_[idx] = false;
    }

    size_t obj_nr() const
    {
        return length_ / kObjSize;
    }

private:
    void *base_;
    size_t length_;
    std::atomic<size_t> idx_{0};
    std::vector<bool> allocated_;
};

#endif