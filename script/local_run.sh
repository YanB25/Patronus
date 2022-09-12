#!/bin/bash 
# ./local_run.sh <script>
set -e

cmd=$1
shift
param=$@

export GLOG_logtostderr=1 
export GLOG_colorlogtostderr=1 
export ASAN_OPTIONS=halt_on_error=0 
./$cmd $param
