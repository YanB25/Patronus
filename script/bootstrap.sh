#!/bin/bash
# install expect for `unbuffer` cmd
./ssh.sh $1 "apt install -y systemd-coredump expect llvm valgrind"
./ssh.sh $1 "echo 16384 > /proc/sys/vm/nr_hugepages"
# sshpass -p "${password}" ssh-copy-id -oStrictHostKeyChecking=no -p "${port}" "${ip}"; 