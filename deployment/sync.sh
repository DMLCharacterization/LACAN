#!/bin/sh

mkdir -p results

while true
do
	echo start
	rsync -a root@$1:/tmp/results results/$1
	sleep 60
done
