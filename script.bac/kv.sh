#!/bin/bash

#Usage: ./benchmark nodeNR threadNR readNR locality sharing

mpi="/usr/local/openmpi/bin/mpiexec --allow-run-as-root -hostfile host_mpi -np "
th_nr="4"
time=$(date "+%d-%H-%M-%S")

vary_nodes () {
  nodes_arr=(2 3 4 5 6 7 8)
  for nodes in ${nodes_arr[@]}; do
    ./restartMemc.sh
    ssh 10.0.2.125 "bash /home/workspace/ccDSM/p4src/auto_run.sh >> /dev/null"
    ${mpi} ${nodes}  ./dht ${nodes} ${th_nr} ${time}
  done
}

vary_nodes