#!/bin/bash
set -e
set -o xtrace

cd ../build; make -j; cd ../script
./for_each.sh ./cleanup.sh
./restartMemc.sh
./for_each.sh ./sync.sh
./run.sh $1
./fetch_log.sh
./for_each.sh ./cleanup.sh
