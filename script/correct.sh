#!/bin/bash
set -e
set -o xtrace

./build_debug.sh

cd ../build/
arr=(correct*)
cd ../script/
for file in "${arr[@]}" 
do
    ./bench.sh "${file}"
done