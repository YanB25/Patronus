#pragma once
#ifndef VALIDITY_LOCK_H
#define VALIDITY_LOCK_H

#include <glog/logging.h>

#include <atomic>

template <bool kEnable>
class ValidityMutex
{
public:
    ValidityMutex &operator=(const ValidityMutex &) = delete;
    ValidityMutex(const ValidityMutex &) = delete;

    void lock();
    bool try_lock();
    void unlock();

private:
};

template <>
class ValidityMutex<true>
{
public:
    ValidityMutex &operator=(const ValidityMutex &) = delete;
    ValidityMutex(const ValidityMutex &) = delete;
    ValidityMutex() = default;

    void lock()
    {
        bool expect = false;
        CHECK(lock_.compare_exchange_strong(
            expect, true, std::memory_order_acquire))
            << "** Conflict detected. Expect lock to be unhold";
    }
    bool try_lock()
    {
        lock();
        return true;
    }
    void unlock()
    {
        bool expect = true;
        CHECK(lock_.compare_exchange_strong(
            expect, false, std::memory_order_release))
            << "** Conflict detected. Expect lock to be hold";
    }

private:
    std::atomic<bool> lock_{false};
};

template <>
class ValidityMutex<false>
{
public:
    ValidityMutex &operator=(const ValidityMutex &) = delete;
    ValidityMutex(const ValidityMutex &) = delete;
    ValidityMutex() = default;

    void lock()
    {
    }
    bool try_lock()
    {
        return true;
    }
    void unlock()
    {
    }

private:
};

#endif