#include "Keeper.h"

#include <fstream>
#include <iostream>
#include <random>
#include <glog/logging.h>

char *getIP();

std::string trim(const std::string &s)
{
    std::string res = s;
    if (!res.empty())
    {
        res.erase(0, res.find_first_not_of(" "));
        res.erase(res.find_last_not_of(" ") + 1);
    }
    return res;
}

const char *Keeper::SERVER_NUM_KEY = "serverNum";

Keeper::Keeper(uint32_t maxServer) : maxServer(maxServer), memc(NULL)
{
}

Keeper::~Keeper()
{
    //   listener.detach();

    disconnectMemcached();
}

/**
 * @brief connect this machine into memcached.
 */
bool Keeper::connectMemcached()
{
    memcached_server_st *servers = NULL;
    memcached_return rc;

    std::ifstream conf("../memcached.conf");

    if (!conf)
    {
        LOG(ERROR) << "can't open memchaced.conf at ../memcached.conf";
        return false;
    }

    std::string addr, port;
    std::getline(conf, addr);
    std::getline(conf, port);

    memc = memcached_create(NULL);
    servers = memcached_server_list_append(
        servers, trim(addr).c_str(), std::stoi(trim(port)), &rc);
    rc = memcached_server_push(memc, servers);

    free(servers);

    if (rc != MEMCACHED_SUCCESS)
    {
        LOG(ERROR) << "Can't add server:" << memcached_strerror(memc, rc);
        return false;
    }

    memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL, 1);
    return true;
}

bool Keeper::disconnectMemcached()
{
    if (memc)
    {
        memcached_quit(memc);
        memcached_free(memc);
        memc = NULL;
    }
    return true;
}

void Keeper::serverEnter()
{
    memcached_return rc;
    uint64_t serverNum;

    while (true)
    {
        rc = memcached_increment(
            memc, SERVER_NUM_KEY, strlen(SERVER_NUM_KEY), 1, &serverNum);
        if (rc == MEMCACHED_SUCCESS)
        {
            myNodeID = serverNum - 1;

            DLOG(INFO) << "This server get NodeId " << myNodeID << "["
                       << getIP() << "]";
            return;
        }
        LOG(ERROR) << "Server " << myNodeID
                   << " Counld't incr value and get ID: "
                   << memcached_strerror(memc, rc) << ". retry. ";
        sleep(1);
    }
}

void Keeper::serverConnect()
{
    uint32_t curServer = 0;
    size_t l;
    uint32_t flags;
    memcached_return rc;

    while (curServer < maxServer)
    {
        char *serverNumStr = memcached_get(
            memc, SERVER_NUM_KEY, strlen(SERVER_NUM_KEY), &l, &flags, &rc);
        if (rc != MEMCACHED_SUCCESS)
        {
            LOG(ERROR) << "Server " << myNodeID << " Counld't get serverNum:"
                       << memcached_strerror(memc, rc) << ". retry. ";
            sleep(1);
            continue;
        }
        uint32_t serverNum = atoi(serverNumStr);
        free(serverNumStr);

        // /connect server K
        for (size_t k = curServer; k < serverNum; ++k)
        {
            if (k != myNodeID)
            {
                connectNode(k);
                DLOG(INFO) << "Connected to server " << k;
            }
        }
        curServer = serverNum;
    }
}

void Keeper::memSet(const char *key,
                    uint32_t klen,
                    const char *val,
                    uint32_t vlen)
{
    memcached_return rc;
    while (true)
    {
        rc =
            memcached_set(memc, key, klen, val, vlen, (time_t) 0, (uint32_t) 0);
        if (rc == MEMCACHED_SUCCESS)
        {
            break;
        }
        sleep(400);
        std::this_thread::yield();
    }
}

char *Keeper::memGet(const char *key, uint32_t klen, size_t *v_size)
{
    size_t l;
    char *res;
    uint32_t flags;
    memcached_return rc;

    while (true)
    {
        res = memcached_get(memc, key, klen, &l, &flags, &rc);
        if (rc == MEMCACHED_SUCCESS)
        {
            break;
        }
        usleep(400 * myNodeID);
    }

    if (v_size != nullptr)
    {
        *v_size = l;
    }

    return res;
}

uint64_t Keeper::memFetchAndAdd(const char *key, uint32_t klen)
{
    uint64_t res;
    while (true)
    {
        memcached_return rc = memcached_increment(memc, key, klen, 1, &res);
        if (rc == MEMCACHED_SUCCESS)
        {
            return res;
        }
        usleep(10000);
        std::this_thread::yield();
    }
}
