#!/bin/sh

sudo apt-get install git

echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
sudo apt-get install apt-transport-https
sudo apt-get update
sudo apt-get install sbt

tail -n 4 ~/slaves > ~/workers

cd apps/ && 
sudo git clone https://gitlab.liris.cnrs.fr/sbouchen/LACAN &&
cd LACAN/LACAN/src/lacan/experiments/ &&
sbt assembly &&
sudo mkdir -p target/scala-2.11 &&
cd target/scala-2.11 &&
sudo mv "$(find /home/xp/.sbt -name LACAN*.jar)" .

cd /home/xp/apps/LACAN && sudo chmod 777 -R .

python -m pip install tqdm pandas

cd ~
wget https://github.com/CODAIT/spark-bench/releases/download/v99/spark-bench_2.3.0_0.4.0-RELEASE_99.tgz
tar -xvzf spark-bench_2.3.0_0.4.0-RELEASE_99.tgz

./start-cluster.sh

./spark-bench_2.3.0_0.4.0-RELEASE/bin/spark-bench.sh ./apps/LACAN/deployment/data_generation.conf
