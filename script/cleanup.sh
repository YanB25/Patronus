#!/bin/bash
. env.sh
ssh root@$1 "killall bench_mw"
ssh root@$1 "killall correct_mw"
ssh root@$1 "killall bench_mw_rw"
ssh root@$1 "killall correct_nomem"
ssh root@$1 "killall test_sync_clock"