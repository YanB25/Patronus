#!/bin/bash
# install expect for `unbuffer` cmd
. env.sh
./ssh.sh $1 "apt install -y systemd-coredump expect llvm valgrind libgoogle-glog-dev clang clang-format"
./ssh.sh $1 "echo 16384 > /proc/sys/vm/nr_hugepages"
./ssh.sh $1 "swapoff -a"
./ssh.sh $1 "mkdir -p ${RESULT_DIR}"
# sshpass -p "${password}" ssh-copy-id -oStrictHostKeyChecking=no -p "${port}" "${ip}";