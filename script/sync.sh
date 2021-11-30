#!/bin/bash
set -e
. env.sh
ssh $1 "mkdir -p ${BIN_DIR}"
rsync -av -e ssh ../build/bench_mw $1:${BIN_DIR}
rsync -av -e ssh ../build/correct_mw $1:${BIN_DIR}
rsync -av -e ssh ../build/crash $1:${BIN_DIR}
rsync -av -e ssh ../memcached.conf $1:${WORK_DIR}