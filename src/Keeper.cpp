#include "Keeper.h"

#include <fstream>
#include <iostream>
#include <random>

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
        error("can't open memchaced.conf at ../memcached.conf");
        return false;
    }

    std::string addr, port;
    std::getline(conf, addr);
    std::getline(conf, port);

    memc = memcached_create(NULL);
    servers = memcached_server_list_append(
        servers, trim(addr).c_str(), std::stoi(trim(port)), &rc);
    rc = memcached_server_push(memc, servers);

    if (rc != MEMCACHED_SUCCESS)
    {
        error("Can't add server:%s", memcached_strerror(memc, rc));
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

            dinfo("This server get NodeId %d [%s]\n", myNodeID, getIP());
            return;
        }
        error("Server %d Counld't incr value and get ID: %s, retry...",
              myNodeID,
              memcached_strerror(memc, rc));
        usleep(10000);
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
            error("Server %d Counld't get serverNum: %s, retry\n",
                  myNodeID,
                  memcached_strerror(memc, rc));
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
                dinfo("Connected to server %d", k);
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
        std::this_thread::yield();
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
        std::this_thread::yield();
    }
}
