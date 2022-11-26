#!/usr/bin/python3
import config
import cluster
import util

util.build_debug(False)

c = cluster.Cluster(config.NODES)
c.correct_test_all(not config.DEBUG_SCRIPT)
