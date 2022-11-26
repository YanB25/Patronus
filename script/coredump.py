#!/usr/bin/python3
import config
from cluster import Cluster
import pprint


def fetch_coredumps(c: 'Cluster'):
    cmds = ["coredumpctl", "-1"]
    processes = c.cluster_execute_short_cmd_async(cmds, False)
    results = Cluster.get_process_result(processes)
    return results


if __name__ == '__main__':
    c = Cluster(config.NODES)
    Cluster.show_process_result(fetch_coredumps(c))
