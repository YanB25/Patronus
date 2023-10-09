#!/usr/bin/python3
import config
import cluster
import sys

if len(sys.argv) <= 1:
    print("{} bench_file [flags]".format(sys.argv[0]))
    exit(-1)
binary = sys.argv[1]
params = []
if len(sys.argv) >= 2:
    params = sys.argv[2:]

c = cluster.Cluster(config.NODES)
c.local_run(binary, params, hooks=[])
