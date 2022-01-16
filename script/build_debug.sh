#!/bin/bash
set -e 
set -o xtrace

cd ../build
rm -rf ./*
cmake -DCMAKE_BUILD_TYPE=Debug ..
make clean
make -j