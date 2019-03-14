#!/bin/bash 

if [[ ! -f sorted/$1 ]]; then
    echo $1
    gunzip -c transactions/$1 | sort -nk2 | gzip -c > temp.dat.gz
    mv temp.dat.gz sorted/$1
fi