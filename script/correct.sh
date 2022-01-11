#!/bin/bash

cd ../build
arr=(correct*)
for file in "${arr[@]}" 
do
    echo "${file}"
done