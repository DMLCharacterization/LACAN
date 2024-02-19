#! /bin/bash

############################################################################################
#	Automatically sets the different configuration files of both Spark and Hadoop
#	Sets the IP addresses and private/public keys
#	Sends the appropriate files to the nodes in the cluster
############################################################################################

# Read the nodes to machineFile.txt
#uniq $OAR_NODE_FILE > machineFile.txt

# Run Python script to create the hosts.tmp and the slaves files
python run.py machineFile.txt

# Define the master variable
master=$(head -n 1 machineFile.txt)

# Copy the hosts.txt to /etc/hosts of each node in the cluster
while read m; do
  scp hosts.tmp "root@$m":/etc/hosts
done < machineFile.txt

# Copy the slaves file in the spark's folder in the master
scp slaves "root@$master":/usr/local/spark/conf/slaves

# Copy the slaves file in Hadoop's folder in all the nodes
while read m; do
  scp slaves "root@$m":/opt/hadoop-2.7.7/etc/hadoop/slaves
done < machineFile.txt

# Delete the /data/hadoop-data from all nodes
while read m; do
  (ssh "root@$m" "rm -r /data/hadoop-data; rm -Rf /tmp/hadoop-xp-*")&
done < machineFile.txt
wait

# Create a new public private keys
ssh "root@$master" "yes y | ssh-keygen -t rsa -P '' -f /home/xp/.ssh/id_rsa"
ssh "root@$master" "cat /home/xp/.ssh/id_rsa.pub > /home/xp/.ssh/authorized_keys"

# Grab the authorized_keys file from master
scp "root@$master":/home/xp/.ssh/authorized_keys ./authorized

# Copy the authorized_keys file into the slaves (In this case, for ease of programming, we copy it to
#   all the nodes)
while read m; do
  scp authorized "root@$m":/home/xp/.ssh/authorized_keys
done < machineFile.txt

# Add the workers fingerprints to the master's known_hosts file
ssh "root@$master" "echo > /home/xp/.ssh/known_hosts"
ssh "root@$master" "ssh-keyscan -Ht rsa 127.0.0.1, localhost >> /home/xp/.ssh/known_hosts"
ssh "root@$master" "ssh-keyscan -Ht rsa 0.0.0.0 >> /home/xp/.ssh/known_hosts"
while read ip; do
  ssh -n "root@$master" "ssh-keyscan -Ht rsa $ip >> /home/xp/.ssh/known_hosts"
done < slaves
wait

# Copy the script start-cluster.sh to the master
scp start_cluster.sh "root@$master":/home/xp/
scp prepare_experiment.sh "root@$master":/home/xp/
scp merge_low_level.py "root@$master":/home/xp/

# Send the slaves file to the master node
scp slaves "root@$master":/home/xp/

# Create the workers file
#ssh "root@$master" "tail -n +2 /home/xp/slaves > /home/xp/workers"

# Send the collect.proc.sh script to all nodes
while read m; do
  scp collect.proc.sh "root@$m":/home/xp/
  scp hdfs-site.xml "root@$m":/opt/hadoop-2.7.7/etc/hadoop/hdfs-site.xml
  ssh -n "root@$m" "rm -rf /tmp/results /tmp/data /tmp/work /tmp/logs"
  ssh -n "root@$m" "mkdir /tmp/results /tmp/data /tmp/work /tmp/logs"
  ssh -n "root@$m" "rmdir /usr/local/spark/work/"
  ssh -n "root@$m" "rm -rf /opt/hadoop-2.7.7/logs"
  ssh -n "root@$m" "ln -s /tmp/work /usr/local/spark/work"
  ssh -n "root@$m" "ln -s /tmp/logs /opt/hadoop-2.7.7/logs"
  ssh -n "root@$m" "chmod -R 777 /tmp"
done < machineFile.txt


# Clear all the junk files
#rm machineFile.txt
rm slaves
rm hosts.tmp
rm authorized
rm ip_addresses


# Make the necessary directories in the master node
ssh "root@$master" "mkdir /home/xp/data"
ssh "root@$master" "mkdir /home/xp/apps"

scp -r ../LACAN-Data/datasets/*/* "root@$master":/home/xp/data
