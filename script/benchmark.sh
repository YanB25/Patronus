#!/bin/bash

#Usage: ./benchmark nodeNR threadNR readNR locality sharing

mpi="/usr/local/openmpi/bin/mpiexec --allow-run-as-root -hostfile host_mpi -np "
th_nr="4"
time=$(date "+%d-%H-%M-%S")

vary_shared_ratios () {
  node_nr=8
  # shared_arr=(0 10 20 30 40 50 60 70 80 90 100)
  shared_arr=(100)
  for shared_ratio in ${shared_arr[@]}; do
    ./restartMemc.sh
    ssh 10.0.2.125 "bash /home/workspace/ccDSM/p4src/auto_run.sh >> /dev/null"
    ${mpi} ${node_nr}  ./benchmark ${node_nr} ${th_nr} 50 0 ${shared_ratio} ${time}
  done
}

vary_locality_ratios () {
  node_nr=8
  locality_arr=(0 10 20 30 40 50 60 70 80 90 100)
  for locality_ratio in ${locality_arr[@]}; do
    ./restartMemc.sh
    ssh 10.0.2.125 "bash /home/workspace/ccDSM/p4src/auto_run.sh >> /dev/null"
    ${mpi} ${node_nr}  ./benchmark ${node_nr} ${th_nr} 50 ${locality_ratio} 20 ${time}
  done

  for locality_ratio in ${locality_arr[@]}; do
    ./restartMemc.sh
    ssh 10.0.2.125 "bash /home/workspace/ccDSM/p4src/auto_run.sh >> /dev/null"
    ${mpi} ${node_nr}  ./benchmark ${node_nr} ${th_nr} 50 ${locality_ratio} 60 ${time}
  done
}

vary_read_ratios () {
  node_nr=8
  read_arr=(0 10 20 30 40 50 60 70 80 90 100)
  for read_ratio in ${read_arr[@]}; do
    ./restartMemc.sh
    ssh 10.0.2.125 "bash /home/workspace/ccDSM/p4src/auto_run.sh >> /dev/null"
    ${mpi} ${node_nr}  ./benchmark ${node_nr} ${th_nr} ${read_ratio} 0 20 ${time}
  done

  for read_ratio in ${read_arr[@]}; do
    ./restartMemc.sh
    ssh 10.0.2.125 "bash /home/workspace/ccDSM/p4src/auto_run.sh >> /dev/null"
    ${mpi} ${node_nr}  ./benchmark ${node_nr} ${th_nr} ${read_ratio} 0 60 ${time}
  done
}

vary_nodes () {
  nodes_arr=(2 3 4 5 6 7 8)
  for nodes in ${nodes_arr[@]}; do
    ./restartMemc.sh
    ssh 10.0.2.125 "bash /home/workspace/ccDSM/p4src/auto_run.sh >> /dev/null"
    ${mpi} ${nodes}  ./benchmark ${nodes} ${th_nr} 50 0 60 ${time}
  done
}

vary_shared_ratios
# vary_locality_ratios
# vary_read_ratios
# vary_nodes