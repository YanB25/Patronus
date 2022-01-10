#!/bin/bash
set -e
set -o xtrace

cd ../build
make clean
make -j