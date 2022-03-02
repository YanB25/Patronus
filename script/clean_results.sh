#!/bin/bash
#
# ./clean_result.sh
#
# Clean all the generated results from the cluster,

. env.sh

rm -rf ./result

for vm in `cat inet.conf`; do
    ./ssh.sh $vm "rm -rf ${RESULT_DIR}/*"
done