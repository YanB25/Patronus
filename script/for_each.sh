#!/bin/bash
set -e
for vm in `cat inet.conf`; do
    echo [start on vm $vm ...]
    nohup $1 $vm 1>log/$vm.log 2>&1 </dev/null &
done
wait
echo [finished]