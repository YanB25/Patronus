#!/bin/bash
. env.sh
ssh root@$1 "killall bench_mw"
ssh root@$1 "killall correct_mw"