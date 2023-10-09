#!/usr/bin/python3
import config
from cluster import Cluster
import pprint
import os
import time
import util


def running_nr(results: 'dict[str, list[str]]') -> 'dict[str, int]':
    ret = {}
    for (node, lines) in results.items():
        ret[node] = 0
        for line in lines:
            if config.BIN_DIR in line:
                ret[node] += 1
    return ret


def all_exited(results: 'dict[str, list[str]]') -> 'bool':
    number_of_running = running_nr(results)
    for (_, n) in number_of_running.items():
        if n > 0:
            return False
    return True


def fetch_process_states(c: 'Cluster') -> 'dict[str, list[str]]':
    cmd = ["ps", "-aux", "|", "grep", "workspace"]
    processes = c.cluster_execute_short_cmd_async(cmd, True)
    results = Cluster.get_process_result(processes)
    return results


def fetch_log_tails(c: 'Cluster', node, tail_nr=10) -> 'dict[str, list[str]]':
    log_files = os.path.join(config.BASE_DIR, "*.LOG")
    cmd = ["tail", "-n", f"{tail_nr}", log_files]
    processes = c.node_execute_short_cmd_async(node, cmd, True)
    results = Cluster.get_process_result([processes])
    return results


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
    c = Cluster(config.NODES)
    nodes = list(c.unique_nodes())
    node_idx = 0
    while True:
        node = nodes[node_idx % len(nodes)]
        node_idx += 1

        logs = fetch_log_tails(c, node)
        Cluster.show_process_result(logs)

        coredump_nodes = fetch_coredump_nodes(c)
        if len(coredump_nodes) > 0:
            util.banner(f"** nodes {' '.join(coredump_nodes)} COREDUMP")
            time.sleep(1)
            coredump_nodes = fetch_coredump_nodes(c)
            util.state("** Dumping logs of crashed processes...")
            for node in coredump_nodes:
                logs = fetch_log_tails(c, node, 100)
                Cluster.show_process_result(logs)
            util.banner("waiting for coredump finish")
            while len(fetch_coredump_nodes(c)) > 0:
                time.sleep(1)
            util.state("coredump finished.")
            exit(0)

        ps = fetch_process_states(c)
        if all_exited(ps):
            util.state("All processes exited.")
            exit(0)

        time.sleep(0.2)
