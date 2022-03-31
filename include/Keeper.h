#pragma once
#ifndef __KEEPER__H__
#define __KEEPER__H__

#include <assert.h>
#include <infiniband/verbs.h>
#include <libmemcached/memcached.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include <chrono>
#include <functional>
#include <string>
#include <thread>

#include "Config.h"
#include "Rdma.h"

using namespace std::chrono_literals;

class PerThreadMemcachedSt
{
public:
    PerThreadMemcachedSt() = default;
    PerThreadMemcachedSt(const PerThreadMemcachedSt &) = delete;
    void operator=(const PerThreadMemcachedSt &) = delete;

    memcached_st *get_memc(memcached_st *global_memc)
    {
        if (unlikely(memc_ == nullptr))
        {
            init(global_memc);
        }
        return memc_;
    }
    ~PerThreadMemcachedSt()
    {
        if (memc_)
        {
            memcached_free(memc_);
        }
    }

private:
    void init(memcached_st *global_memc)
    {
        memc_ = memcached_clone(
            nullptr /* alloc for me */,
            CHECK_NOTNULL(global_memc) /* copy server list from here */);
    }
    memcached_st *memc_{nullptr};
};

class Keeper
{
private:
    static const char *SERVER_NUM_KEY;

    uint32_t maxServer;
    uint16_t myNodeID;
    std::string myIP;
    uint16_t myPort;

    memcached_st *global_memc;
    // RAII to management the life time of per-thread memc.
    static thread_local PerThreadMemcachedSt thread_memc;

protected:
    bool connectMemcached();
    bool disconnectMemcached();
    void serverConnect();
    /**
     * @brief Get server node id
     */
    void serverEnter();
    virtual bool connectNode(uint16_t remoteID) = 0;

public:
    Keeper(uint32_t maxServer = 12);
    virtual ~Keeper();

    uint16_t getMyNodeID() const
    {
        return this->myNodeID;
    }
    uint16_t getServerNR() const
    {
        return this->maxServer;
    }
    uint16_t getMyPort() const
    {
        return this->myPort;
    }

    std::string getMyIP() const
    {
        return this->myIP;
    }

    template <typename T>
    void memSet(const char *key,
                uint32_t klen,
                const char *val,
                uint32_t vlen,
                const T &sleep_time)
    {
        memcached_return rc;
        auto *t_memc = thread_memc.get_memc(global_memc);
        while (true)
        {
            rc = memcached_set(
                t_memc, key, klen, val, vlen, (time_t) 0, (uint32_t) 0);
            if (rc == MEMCACHED_SUCCESS)
            {
                break;
            }
            std::this_thread::sleep_for(sleep_time);
        }
    }

    template <typename T>
    char *memTryGet(const char *key,
                    uint32_t klen,
                    size_t *v_size,
                    const T &sleep_time)
    {
        size_t l;
        char *res;
        uint32_t flags;
        memcached_return rc;
        auto *t_memc = thread_memc.get_memc(global_memc);

        while (true)
        {
            res = memcached_get(t_memc, key, klen, &l, &flags, &rc);
            if (rc == MEMCACHED_SUCCESS || rc == MEMCACHED_NOTFOUND)
            {
                break;
            }
            std::this_thread::sleep_for(sleep_time);
        }

        if (rc == MEMCACHED_NOTFOUND)
        {
            if (v_size)
            {
                *v_size = 0;
            }
            return nullptr;
        }
        else
        {
            if (v_size)
            {
                *v_size = l;
            }

            return res;
        }
    }

    template <typename T>
    char *memGet(const char *key,
                 uint32_t klen,
                 size_t *v_size,
                 const T &sleep_time)
    {
        size_t l;
        char *res;
        uint32_t flags;
        memcached_return rc;

        auto *t_memc = thread_memc.get_memc(global_memc);
        while (true)
        {
            res = memcached_get(t_memc, key, klen, &l, &flags, &rc);
            if (rc == MEMCACHED_SUCCESS)
            {
                break;
            }
            std::this_thread::sleep_for(sleep_time);
        }

        if (v_size != nullptr)
        {
            *v_size = l;
        }

        return res;
    }

    template <typename T>
    uint64_t memFetchAndAdd(const char *key, uint32_t klen, const T &sleep_time)
    {
        uint64_t res;
        while (true)
        {
            auto *t_memc = thread_memc.get_memc(global_memc);
            memcached_return rc =
                memcached_increment(t_memc, key, klen, 1, &res);
            if (rc == MEMCACHED_SUCCESS)
            {
                return res;
            }

            std::this_thread::sleep_for(sleep_time);
        }
    }
};

#endif
