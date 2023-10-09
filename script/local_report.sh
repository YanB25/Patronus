#!/bin/bash
set -e
set -o xtrace

tail -n50 ../*.LOG
ps -aux | grep workspace