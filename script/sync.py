#!/usr/bin/python3
import config
import cluster
c = cluster.Cluster(config.NODES)
c.deploy_sync(silent=not config.DEBUG_SCRIPT)
