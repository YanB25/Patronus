#!/bin/bash
set -e
set -o xtrace

./build_debug.sh

# hash table
hput () {
  eval hash"$1"='$2'
}

hget () {
  eval echo '${hash'"$1"'#hash}'
}

cd ../build/
arr=(correct*)
cd ../script/
for file in "${arr[@]}" 
do
    start=`date +%s`
    ./bench.sh "${file}"
    end=`date +%s`
    runtime=$((end-start))
    hput ${file} $runtime
done

set +x

for file in "${arr[@]}"
do
    echo ${file}  takes `hget ${file}` s
done