#include "Common.h"

#include <arpa/inet.h>
#include <glog/logging.h>
#include <net/if.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

void bindCore(uint16_t core)
{
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core, &cpuset);
    int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0)
    {
        LOG(ERROR) << "can't bind core " << core;
    }
}

char *getIP()
{
    struct ifreq ifr;
    int fd = socket(AF_INET, SOCK_DGRAM, 0);

    ifr.ifr_addr.sa_family = AF_INET;
    strncpy(ifr.ifr_name, "ib0", IFNAMSIZ - 1);

    ioctl(fd, SIOCGIFADDR, &ifr);
    close(fd);

    return inet_ntoa(((struct sockaddr_in *) &ifr.ifr_addr)->sin_addr);
}

char *getMac()
{
    static struct ifreq ifr;
    int fd = socket(AF_INET, SOCK_DGRAM, 0);

    ifr.ifr_addr.sa_family = AF_INET;
    strncpy(ifr.ifr_name, "ens2", IFNAMSIZ - 1);

    ioctl(fd, SIOCGIFHWADDR, &ifr);
    close(fd);

    return (char *) ifr.ifr_hwaddr.sa_data;
}
