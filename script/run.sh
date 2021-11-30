#!/bin/bash
# ./run.sh <script>
. env.sh

declare -a vms=()
declare -a inets=()
for vm in `cat vm.conf`; do
    vms+=($vm)
done
for inet in `cat inet.conf`; do
    inets+=($inet)
done

for (( i=1; i<${#vms[@]}; i++ )); do
    echo [start service on ${vms[$i]} \(inet ${inets[$i]}\) ...]
    # ./ssh.sh ${inets[$i]} "cd ${BIN_DIR}; nohup ${BIN_DIR}/$1 1>${WORK_DIR}/LOG 2>&1 &"
    nohup ssh root@${inets[$i]} "cd ${BIN_DIR}; ${BIN_DIR}/$1 1>${WORK_DIR}/LOG 2>&1" &
done

./ssh.sh ${inets[0]} "cd ${BIN_DIR}; ${BIN_DIR}/$1 2>&1 | tee ../LOG"
echo [Waiting peers to finish]
wait