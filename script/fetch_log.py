#!/usr/bin/python3
import config
import cluster
c = cluster.Cluster(config.NODES)
c.fetch_logs(silent=not config.DEBUG_SCRIPT)
