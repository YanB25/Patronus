#!/bin/bash
set -e
set -o xtrace

tail -n200 ../LOG
ps -aux | grep workspace
sudo coredumpctl -1
