#!/bin/bash
#
# ./fetch_log.sh
#
# Fetch the runtime logs from the cluster,
# so that you can analyze the metrics (like throughput/latency)
# or understand the behavious of the program.
# 
# Logs are placed in `./fetched/<ip>.log`.
. env.sh
LOG_PATH=${REMOTE_DIR}/LOG

mkdir -p fetched

for vm in `cat inet.conf`; do
    echo [fetching log from $vm ..]
    nohup ./fetch.sh $vm $LOG_PATH fetched/$vm.log 1>log/$vm.log 2>&1 &
done

for vm in `cat inet.conf`; do
    echo [coredump from $vm]
    ./ssh.sh $vm "coredumpctl -1"
done
