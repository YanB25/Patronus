#!/usr/bin/python3
import sys
import config
from cluster import Cluster
import util


def fetch_coredumps(c: 'Cluster'):
    cmd = ["coredumpctl", "-1"]
    processes = c.cluster_execute_short_cmd_async(cmd, True)
    results = Cluster.get_process_result(processes)
    return results


def coredump_nr(results: 'dict[str, list[str]]'):
    ret = {}
    for node, lines in results.items():
        ret[node] = 0
        for line in lines:
            if config.COREDUMP_MSG in line:
                ret[node] += 1
    return ret


def fetch_coredump_nodes(c: 'Cluster'):
    core_dump = fetch_coredumps(c)
    cdnr = coredump_nr(core_dump)
    coredump_nodes = []
    for node, nr in cdnr.items():
        if nr > 0:
            coredump_nodes.append(node)
    return coredump_nodes


if __name__ == '__main__':
    if len(sys.argv) <= 1:
        print("{} bench_file [flags]".format(sys.argv[0]))
        exit(-1)
    binary = sys.argv[1]
    params = []
    if len(sys.argv) >= 2:
        params = sys.argv[2:]

    c = Cluster(config.NODES)
    c.benchmark(binary, params)

    coredump_nodes = fetch_coredump_nodes(c)
    if len(coredump_nodes) > 0:
        util.banner(f"** nodes {' '.join(coredump_nodes)} COREDUMP")
