#!/bin/bash
set -e 
set -o xtrace

cd ../build
cmake -DCMAKE_BUILD_TYPE=Debug ..
make clean
make -j