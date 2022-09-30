#!/bin/bash
. env.sh

cd ../build
# collect all the files as an array
arr=(*)

FLAGS="--signal SIGKILL"

# equivalent to ' '.join(arr)
str=$( IFS=$' '; echo "${arr[@]:0:32}" )
# kill them all.
echo "$str"
ssh root@$1 "killall ${FLAGS} $str"

str=$( IFS=$' '; echo "${arr[@]:32:64}" )
# kill them all.
echo $str
ssh root@$1 "killall ${FLAGS} $str"

str=$( IFS=$' '; echo "${arr[@]:64:96}" )
# kill them all.
echo $str
ssh root@$1 "killall ${FLAGS} $str"

ssh root@$1 "killall ${FLAGS} pcm-pcie"