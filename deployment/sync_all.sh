#!/bin/sh

mkdir -p results

while read leader; do
	nohup ./sync.sh $leader > results/out-$leader 2>&1 &
done < leaders.txt

