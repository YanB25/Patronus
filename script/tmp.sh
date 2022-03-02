#!/bin/bash
# setpgrp(0, 12345) || die "$!"
hash=$(git rev-parse HEAD)
hash=${hash:0:8}
date=$(date +'%Y-%m-%d.%H:%M:%S')
meta="${date}.${hash}"
echo $meta