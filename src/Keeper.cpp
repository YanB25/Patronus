#include "Keeper.h"

#include <glog/logging.h>

#include <chrono>
#include <fstream>
#include <iostream>
#include <random>

using namespace std::chrono_literals;

char *getIP();

thread_local PerThreadMemcachedSt Keeper::thread_memc;

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

Keeper::Keeper(uint32_t maxServer) : maxServer(maxServer), global_memc(NULL)
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

    global_memc = memcached_create(NULL);
    servers = memcached_server_list_append(
        servers, trim(addr).c_str(), std::stoi(trim(port)), &rc);
    rc = memcached_server_push(global_memc, servers);

    free(servers);

    if (rc != MEMCACHED_SUCCESS)
    {
        LOG(ERROR) << "Can't add server:"
                   << memcached_strerror(global_memc, rc);
        return false;
    }

    memcached_behavior_set(global_memc, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL, 1);
    return true;
}

bool Keeper::disconnectMemcached()
{
    if (global_memc)
    {
        memcached_quit(global_memc);
        memcached_free(global_memc);
        global_memc = NULL;
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
            global_memc, SERVER_NUM_KEY, strlen(SERVER_NUM_KEY), 1, &serverNum);
        if (rc == MEMCACHED_SUCCESS)
        {
            myNodeID = serverNum - 1;

            DLOG(INFO) << "This server get NodeId " << myNodeID << "["
                       << getIP() << "]";
            return;
        }
        LOG(ERROR) << "Server " << myNodeID
                   << " Counld't incr value and get ID: "
                   << memcached_strerror(global_memc, rc) << ". retry. ";
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
        char *serverNumStr = memcached_get(global_memc,
                                           SERVER_NUM_KEY,
                                           strlen(SERVER_NUM_KEY),
                                           &l,
                                           &flags,
                                           &rc);
        if (rc != MEMCACHED_SUCCESS)
        {
            LOG(ERROR) << "Server " << myNodeID << " Counld't get serverNum:"
                       << memcached_strerror(global_memc, rc) << ". retry. ";
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
