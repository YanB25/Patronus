#!/usr/bin/python3
import config
import cluster
import sys
if len(sys.argv) <= 1:
    print(f"{sys.argv[0]} commands")
    exit(-1)
c = cluster.Cluster(config.NODES)
c.cluster_execute_short_cmd(sys.argv[1:], False)
