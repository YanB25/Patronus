#!/bin/bash
# install expect for `unbuffer` cmd
. env.sh
./ssh.sh $1 "apt install -y systemd-coredump expect llvm valgrind libgoogle-glog-dev clang clang-format"
# for serverless
./ssh.sh $1 "apt install -y libmagick++-dev"
# for linux profile
./ssh.sh $1 "apt install -y linux-tools-generic"
./ssh.sh $1 "echo 32768 > /proc/sys/vm/nr_hugepages"
./ssh.sh $1 "swapoff -a"
./ssh.sh $1 "mkdir -p ${RESULT_DIR}"
# sshpass -p "${password}" ssh-copy-id -oStrictHostKeyChecking=no -p "${port}" "${ip}";