#!/bin/bash
set -e
set -o xtrace

cd ../build; make -j; cd ../script
./for_each.sh ./cleanup.sh
./restartMemc.sh
./for_each.sh ./sync.sh
wait
./run.sh $@
wait
./fetch_log.sh
./fetch_result.sh
# ./for_each.sh ./cleanup.sh
