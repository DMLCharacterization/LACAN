#! /bin/bash

# 1 --> name of metrics-collection script
# 2 --> file of the workers
# 3 --> filepath of result

kill -9 $(pidof -xs "$1")
ips=$(cat $2)
for ip in $ips; do
  ssh -n "$ip" "pkill -9 $1"
  ssh -n "$ip" "pid=\$(ps -aux | grep '/bin/bash /home/xp/collect.proc.sh' | awk {'print \$2'}); echo \$pid |xargs kill"
  ssh -n "$ip" "pid=\$(ps -aux | grep 'ssh $ip /home/xp/collect.proc.sh' | awk {'print \$2'}); echo \$pid |xargs kill"
  kill -9 $(ps -aux | grep "ssh $ip /home/xp/collect.proc.sh" | awk {'print $2'})
done < "$2"

kill -9 $(ps -aux | grep '/bin/bash /home/xp/collect.proc.sh' | awk {'print $2'})

mkdir "$3"/system-metrics

mv /home/xp/proc "$3"/system-metrics/master
iden=0
while read ip; do
  scp -r "$ip":/home/xp/proc "$3"/system-metrics/"$iden"
  iden=$(($iden+1))
done < "$2"
