#! /bin/bash
rm -rf /home/xp/proc

mkdir /home/xp/proc

while true
do

head -n 3 /proc/meminfo >> /home/xp/proc/meminfo
echo -en "$\n" >> /home/xp/proc/meminfo

head -n 1 /proc/stat >> /home/xp/proc/stat

cat  /proc/diskstats >> /home/xp/proc/diskstats

tail -n 1 /proc/net/netstat >> /home/xp/proc/netstat

sleep 1s

done
