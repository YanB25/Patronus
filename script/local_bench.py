#!/usr/bin/python3
import config
import cluster
import sys

silent = not config.DEBUG_SCRIPT

if len(sys.argv) <= 1:
    print("{} bench_file [flags]".format(sys.argv[0]))
    exit(-1)
binary = sys.argv[1]
params = []
if len(sys.argv) >= 2:
    params = sys.argv[2:]

# HOOKS = ['valgrind', '--leak-check=full']
# HOOKS = ['valgrind']
HOOKS = []

c = cluster.Cluster(config.NODES)
c.local_benchmark(binary, params, hooks=HOOKS, silent=silent)
