#!/bin/bash
#
# ./fetch_result.sh
#
# Fetch the result CSVs from the cluster,
# 
# Logs are placed in `./result/$binary.$date.$time.$hash.csv`

. env.sh

mkdir -p result

for vm in `cat inet.conf`; do
    echo [fetching CSVs from $vm ..]
    nohup rsync -r $vm:${RESULT_DIR} ./fetched/${vm}/ 1>log/$vm.log 2>&1 &
done