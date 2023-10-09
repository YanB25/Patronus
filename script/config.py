#!/usr/python3
import subprocess
import os
import util

NODES = [
    # 'IP', numa_id
    ('10.0.2.132', 0),

    ('10.0.2.133', 0),

    ('10.0.2.134', 0),

    ('10.0.2.135', 0),
]
MACHINE_NR = len(NODES)

# memcached
MEMC_CONF = "../memcached.conf"


def get_memcached_conf():
    with open(MEMC_CONF) as f:
        lines = f.read().strip().split()
        ip = lines[0]
        port = lines[1]
        if not util.is_valid_ip(ip) or not util.is_valid_port(port):
            util.error(
                f"Failed to load correct memcached address. Got {ip}:{port} from {MEMC_CONF}")
            return "", ""
        return ip, port


# The directory. Don't change.
BASE_DIR = "/home/yanbin/workspace"
CUR_DIR = "./"

BIN_DIR = os.path.join(BASE_DIR, "bin")
RESULT_DIR = os.path.join(BASE_DIR, "result")
ARTIFACTS_DIR = os.path.join(BASE_DIR, "artifacts")

FETCH_DIR = os.path.join(CUR_DIR, "fetched")
BUILD_DIR = os.path.join(CUR_DIR, "../build")

# used to disable annoying messages and warnings
SSH_FLAGS = ['-tt', '-o LOGLEVEL=QUIET']

BUILD_FAILED_MSG = "recipe for target 'all' failed"
COREDUMP_MSG = "Notice: 2 systemd-coredump@.service units are running, output may be incomplete"

RNIC_VERSION = 5


def select_rnic(numa_id: 'str|int'):
    return f"mlx{RNIC_VERSION}_{numa_id}"


CC = "clang-10"
CXX = "clang++-10"

DEBUG_SCRIPT = False
