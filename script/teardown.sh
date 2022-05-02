#!/bin/bash
./ssh.sh $1 "echo 0 > /proc/sys/vm/nr_hugepages"