#!/bin/bash
./ssh.sh $1 "apt install systemd-coredump"
./ssh.sh $1 "echo 16384 > /proc/sys/vm/nr_hugepages"
# sshpass -p "${password}" ssh-copy-id -oStrictHostKeyChecking=no -p "${port}" "${ip}"; 