#!/bin/bash
#
# ./fetch.sh $vm $remote_path $local_path
#
# Fetch file(s) from a remote machine($vm and $remote_path) 
# to your local machine($local_path).
#
# e.g. ./fetch.sh 104.215.95.233 ~/raft/LOG fetched_log.txt
#
# typical use:
# Used by fetch_log.sh as a helper scripts.
# Used to fetch one file from the remote.

if [ "$#" -ne 3 ]; then
    echo "Illegal number of parameters"
    echo "./fetch.sh \$vm \$remote_path \$local_path"
    exit -1
fi;

rsync $1:$2 $3