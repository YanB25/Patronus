#!/bin/bash
set -e
for vm in `cat inet.conf`; do
    echo [start on vm $vm ...]
    nohup ssh root@$vm "$1" 1>log/$vm.log 2>&1 </dev/null &
done
wait
echo [finished]