#! /bin/bash

# Format hdfs
/opt/hadoop-2.7.7/bin/hdfs namenode -format

# Start HDFS
/opt/hadoop-2.7.7/sbin/start-dfs.sh

# Start YARN
/opt/hadoop-2.7.7/sbin/start-yarn.sh

# Start Spark
/usr/local/spark/sbin/start-all.sh

# Create the spark-defaults file
cat /usr/local/spark/conf/spark-defaults.conf.template > spark-defaults.conf

# Add the configuration
#echo "spark.master spark://master:7077" >>  /usr/local/spark/conf/spark-defaults.conf
#echo "spark.executor.memory 5g" >> /usr/local/spark/conf/spark-defaults.conf


# Create the folders
#mkdir data
#mkdir apps
mkdir results

$HADOOP_HOME/bin/hdfs dfs -mkdir /data
$HADOOP_HOME/bin/hdfs dfs -put data/* /data/
