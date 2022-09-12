#!/bin/bash
# ./run.sh <script>
. env.sh

cmd=$1
shift

declare -a vms=()
declare -a inets=()
for vm in `cat vm.conf`; do
    vms+=($vm)
done
for inet in `cat inet.conf`; do
    inets+=($inet)
done

# enable color and only log to terminal
# NOTE: glog needs TERM env to enable color.
GLOG_FLAGS="GLOG_logtostderr=1 GLOG_colorlogtostderr=1 "
ASAN_FLAGS="ASAN_OPTIONS=halt_on_error=0"

# hint to `date` and current git commit hash
# so that the binary knows who he is.
commit_hash=$(git rev-parse HEAD)
commit_hash=${commit_hash:0:8}
date=$(date +'%Y-%m-%d.%H:%M:%S')
exec_meta="${date}.${commit_hash}"
EXEC_META_FLAGS="--exec_meta=${exec_meta}"

# NOTE: this is necessary
# the indirection will avoid breaking "--v=3 --n=1" into "--v=3" "--v=n1"
param=$@

for (( i=1; i<${#vms[@]}; i++ )); do
    echo [start service on ${vms[$i]} \(inet ${inets[$i]}\) ...]
    # ./ssh.sh ${inets[$i]} "cd ${BIN_DIR}; nohup ${BIN_DIR}/$1 1>${WORK_DIR}/LOG 2>&1 &"
    nohup ssh root@${inets[$i]} "cd ${BIN_DIR}; source /etc/profile; export TERM='linux'; ${GLOG_FLAGS} ${ASAN_FLAGS} unbuffer ${BIN_DIR}/$cmd $param ${EXEC_META_FLAGS} 1>${WORK_DIR}/LOG 2>&1" &
    # nohup ssh root@${inets[$i]} "cd ${BIN_DIR}; source /etc/profile; export TERM='linux'; ldd ${BIN_DIR}/$cmd $@ ${EXEC_META_FLAGS} 1>${WORK_DIR}/LOG 2>&1" &
done

# ./ssh.sh ${inets[0]} "cd ${BIN_DIR}; source /etc/profile; export TERM='linux'; ${GLOG_FLAGS} ${ASAN_FLAGS} unbuffer ${BIN_DIR}/$cmd ${EXEC_META_FLAGS} $param 2>&1 | tee ../LOG"
# ./ssh.sh ${inets[0]} "cd ${BIN_DIR}; source /etc/profile; export TERM='linux'; ldd ${BIN_DIR}/$cmd ${EXEC_META_FLAGS} $@ 2>&1 | tee ../LOG"
echo [Waiting peers to finish]
wait