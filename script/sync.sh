#!/bin/bash
set -e
. env.sh
ssh $1 "mkdir -p ${BIN_DIR}"
# rsync -av -e ssh ../build/bench_mw $1:${BIN_DIR}
# rsync -av -e ssh ../build/correct_mw $1:${BIN_DIR}
# rsync -av -e ssh ../build/crash $1:${BIN_DIR}
rsync -av -e ssh ../build/* $1:${BIN_DIR}
rsync -av -e ssh ../memcached.conf $1:${WORK_DIR}
rsync -av -e ssh ./report.sh $1:${BIN_DIR}
rsync -av -e ssh ./local_run.sh $1:${BIN_DIR}
rsync -av -e ssh ../artifacts/* $1:${ARTIFACTS_DIR}

# for profiling and flame graph
rsync -av -e ssh ./perf_local_run.sh $1:${BIN_DIR}
rsync -av -e ssh ./gen_flame_graph.sh $1:${BIN_DIR}

# sending the inet.conf to rank the node
rsync -av -e ssh ./inet.conf $1:${BIN_DIR}