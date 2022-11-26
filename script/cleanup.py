#!/usr/bin/python3
import config
import cluster

c = cluster.Cluster(config.NODES)
c.kill_all_processes(silent=not config.DEBUG_SCRIPT)
