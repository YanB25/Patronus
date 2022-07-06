#!/bin/bash

# setpgrp(0, 12345) || die "$!"
# hash=$(git rev-parse HEAD)
# hash=${hash:0:8}
# date=$(date +'%Y-%m-%d.%H:%M:%S')
# meta="${date}.${hash}"
# echo $meta


set -o xtrace
. env.sh
cmd=$1
shift

inet="10.0.2.132"
param=$@

./ssh.sh ${inet} "unbuffer ${BIN_DIR}/$cmd $param"