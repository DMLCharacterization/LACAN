#! /bin/bash

#######################################################################################
#	deploys the images to the appropriate nodes & calls the autoconf.sh script
#######################################################################################

# Deploy the images on the different nodes
#uniq $OAR_NODE_FILE > machineFile.txt
master=$(head -n 1 machineFile.txt)
tail -n +2 machineFile.txt > slaves.txt

workers=""
while read worker; do
  workers="$workers -m $worker"
done < slaves.txt
rm slaves.txt
(kadeploy3 $workers -a ../env/worker.env -k)&
(kadeploy3 -m "$master" -a ../env/master.env -k)&

wait

./autoconf.sh
