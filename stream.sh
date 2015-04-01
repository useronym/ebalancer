#!/bin/bash

## author: A. Krupicka (2015)
##
## This script reads from stdin line by line and distributes the contents
## to a set of hosts in the 'hosts' array thorugh TCP. The distribution
## process preserves the order, meaning it sends lines in the order they are
## read from stdin.
##
## Needs at least bash 4.2 to work

hosts=(virtual-228 virtual-229 virtual-230)
port=5600

i=0
for host in ${hosts[@]}
do
	exec {fds[$i]}> >(nc "${hosts[i]}" $port)
	i=$((i+1))
done

i=0
len=${#hosts[@]}
while read line
do
	printf >&"${fds[i]}" '%s\n' "$line"
	i=$(( (i+1) % $len ))
done
