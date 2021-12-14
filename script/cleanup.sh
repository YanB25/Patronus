#!/bin/bash
. env.sh

cd ../build
# collect all the files as an array
arr=(*)
# equivalent to ' '.join(arr)
str=$( IFS=$' '; echo "${arr[*]}" )
# kill them all.
ssh root@$1 "killall $str"
