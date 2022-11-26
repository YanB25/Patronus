#!/usr/bin/python3
import config
import cluster
import sys

if len(sys.argv) <= 1:
    print("{} bench_file [flags]".format(sys.argv[0]))
    exit(-1)
binary = sys.argv[1]
params = []

c = cluster.Cluster(config.NODES)
c.local_test(binary, params, not config.DEBUG_SCRIPT)
