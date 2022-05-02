#!/bin/bash
sudo apt install -y clang libboost-all-dev clang-format
echo 16384 > /proc/sys/vm/nr_hugepages
