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
GLOG_FLAGS="--logtostderr=true --colorlogtostderr=true"

# hint to `date` and current git commit hash
# so that the binary knows who he is.
commit_hash=$(git rev-parse HEAD)
echo $commit_hash
commit_hash=${commit_hash:0:8}
echo $commit_hash
date=$(date +'%Y-%m-%d.%H:%M:%S')
exec_meta="${date}.${commit_hash}"
EXEC_META_FLAGS="--exec_meta=${exec_meta}"

for (( i=1; i<${#vms[@]}; i++ )); do
    echo [start service on ${vms[$i]} \(inet ${inets[$i]}\) ...]
    # ./ssh.sh ${inets[$i]} "cd ${BIN_DIR}; nohup ${BIN_DIR}/$1 1>${WORK_DIR}/LOG 2>&1 &"
    nohup ssh root@${inets[$i]} "cd ${BIN_DIR}; source /etc/profile; export TERM='linux'; unbuffer ${BIN_DIR}/$cmd $@ ${GLOG_FLAGS} ${EXEC_META_FLAGS} 1>${WORK_DIR}/LOG 2>&1" &
done

./ssh.sh ${inets[0]} "cd ${BIN_DIR}; source /etc/profile; export TERM='linux'; unbuffer ${BIN_DIR}/$cmd ${GLOG_FLAGS} ${EXEC_META_FLAGS} $@ 2>&1 | tee ../LOG"
# echo [Waiting peers to finish]
# wait