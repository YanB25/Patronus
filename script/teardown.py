#!/usr/bin/python3
import config
import cluster
c = cluster.Cluster(config.NODES)
c.cluster_tear_down()
