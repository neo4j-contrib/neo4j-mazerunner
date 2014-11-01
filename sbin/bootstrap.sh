#!/usr/bin/env bash

apt-get update
apt-get install -y python-software-properties
add-apt-repository -y ppa:webupd8team/java
apt-get update
echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | sudo /usr/bin/debconf-set-selections
apt-get -y install oracle-java8-installer
export JAVA_HOME=/usr/lib/jvm/java-8-oracle
apt-get -y install wget
apt-get -y install erlang-nox

# Install curl
apt-get -y install curl

# Install Hadoop 2.4.1
wget https://archive.apache.org/dist/hadoop/core/hadoop-2.4.1/hadoop-2.4.1.tar.gz
tar -xzf hadoop-2.4.1.tar.gz

# Configure Hadoop
yes y | ssh-keygen -t dsa -P '' -f ~/.ssh/id_dsa
cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys

# Copy configurations for HDFS
cp /vagrant/conf/hadoop/* hadoop-2.4.1/etc/hadoop/

# Format namenode
hadoop-2.4.1/bin/hadoop namenode -format

# Start dfs
yes Yes | hadoop-2.4.1/sbin/start-dfs.sh

# Install and start RabbitMQ
echo "deb http://www.rabbitmq.com/debian/ testing main" >/etc/apt/sources.list.d/rabbitmq.list

curl -L -o ~/rabbitmq-signing-key-public.asc http://www.rabbitmq.com/rabbitmq-signing-key-public.asc
apt-key add ~/rabbitmq-signing-key-public.asc

apt-get update
apt-get -y --allow-unauthenticated --force-yes install rabbitmq-server

apt-get upgrade
apt-get -y install aptitude

# Install Neo4j
wget -O - http://debian.neo4j.org/neotechnology.gpg.key| apt-key add - # Import our signing key
echo 'deb http://debian.neo4j.org/repo stable/' > /etc/apt/sources.list.d/neo4j.list # Create an Apt sources.list file
aptitude update -y # Find out about the files in our repository
aptitude install neo4j -y # Install Neo4j, community edition

# Replace default Neo4j configuration
cp /vagrant/conf/neo4j/* /etc/neo4j/

# Install git
apt-get -y install git

# Install Maven
apt-get -y install maven

# Install Scala
apt-get -y remove scala-library scala
wget http://www.scala-lang.org/files/archive/scala-2.11.1.deb
dpkg -i scala-2.11.1.deb
apt-get update
apt-get -y install scala

# sbt installation
# remove sbt:> sudo apt-get purge sbt.

wget http://dl.bintray.com/sbt/debian/sbt-0.13.5.deb
dpkg -i sbt-0.13.5.deb
apt-get update
apt-get -y install sbt

# Copy mazerunner source
git clone https://github.com/kbastani/neo4j-mazerunner.git

# Maven package mazerunner/extension with assemblies
cd neo4j-mazerunner/extension
mvn assembly:assembly -DdescriptorId=jar-with-dependencies

# Copy the mazerunner/extension jar file from mazerunner/extension/target to neo4j/plugins
cp target/extension-1.0-jar-with-dependencies.jar /usr/share/neo4j/plugins/extension-1.0-jar-with-dependencies.jar

# Finished vagrant provisioning
# To start Mazerunner on the VM, do the following

# vagrant ssh
# sudo neo4j-mazerunner/spark/sbt run
# yes Yes | sudo hadoop-2.4.1/sbin/start-dfs.sh

# # Before running the next step, import a dataset into Neo4j

# curl http://localhost:7474/service/mazerunner/warmup
# curl http://localhost:7474/service/mazerunner/pagerank
