#!/bin/bash
# ./gen_flame_graph test_name
PATH=$PATH:/home/yanbin/FlameGraph
sudo perf script > "$1.perf"
stackcollapse-perf.pl "$1.perf" > "$1.folded"
flamegraph.pl "$1.folded" > "$1.svg"