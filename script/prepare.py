#!/usr/bin/python3
import config
from cluster import Cluster
if __name__ == '__main__':
    c = Cluster(config.NODES)
    c.prepare_benchmark(silent=not config.DEBUG_SCRIPT)
