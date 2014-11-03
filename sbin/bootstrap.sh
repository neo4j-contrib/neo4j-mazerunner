#!/usr/bin/env bash

# TODO: Create Chef + Puppet recipe from this template

# Install Java 8
echo "Installing Java 8..."
apt-get -qq update
apt-get -qq install -y python-software-properties
add-apt-repository -y ppa:webupd8team/java
apt-get -qq update
echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | sudo /usr/bin/debconf-set-selections
apt-get -y -qq install oracle-java8-installer
export JAVA_HOME=/usr/lib/jvm/java-8-oracle

# Install wget
echo "Installing wget..."
apt-get -y -qq install wget

# Install erlang
echo "Installing erlang..."
apt-get -y -qq install erlang-nox

# Install curl
echo "Installing curl..."
apt-get -y -qq install curl

# Install and start RabbitMQ
echo "Installing RabbitMQ..."
echo "deb http://www.rabbitmq.com/debian/ testing main" >/etc/apt/sources.list.d/rabbitmq.list

curl -quiet -L -o ~/rabbitmq-signing-key-public.asc http://www.rabbitmq.com/rabbitmq-signing-key-public.asc
apt-key add ~/rabbitmq-signing-key-public.asc

apt-get -qq update
apt-get -y -qq --allow-unauthenticated --force-yes install rabbitmq-server

#apt-get -qq upgrade

# Install Neo4j
echo "Installing Neo4j..."
wget -O - http://debian.neo4j.org/neotechnology.gpg.key| apt-key add - # Import our signing key
echo 'deb http://debian.neo4j.org/repo stable/' > /etc/apt/sources.list.d/neo4j.list # Create an Apt sources.list file
apt-get -qq update # Find out about the files in our repository
apt-get -qq -y install neo4j # Install Neo4j, community edition

# Replace default Neo4j configuration
cp /vagrant/conf/neo4j/* /etc/neo4j/

# Install git
echo "Installing Git..."
apt-get -y -qq install git

# Install Maven
echo "Installing Maven..."
apt-get -y -qq install maven

# Install Scala
echo "Installing Scala..."
apt-get -y -qq remove scala-library scala
wget http://www.scala-lang.org/files/archive/scala-2.11.1.deb
dpkg -i scala-2.11.1.deb
apt-get -qq update
apt-get -y -qq install scala

# Install sbt
echo "Installing SBT..."
wget http://dl.bintray.com/sbt/debian/sbt-0.13.5.deb
dpkg -i sbt-0.13.5.deb
apt-get -qq update
apt-get -y -q install sbt

# Configure Hadoop security settings
#echo "Configuring Hadoop HDFS..."
#su vagrant -c -l "sudo yes y | ssh-keygen -t dsa -P '' -f ~/.ssh/id_dsa"
#su vagrant -c -l "sudo cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys"

# Install Hadoop 2.4.1
echo "Installing Hadoop 2.4.1..."
wget https://archive.apache.org/dist/hadoop/core/hadoop-2.4.1/hadoop-2.4.1.tar.gz
tar -xzf hadoop-2.4.1.tar.gz

# Copy configurations for HDFS
echo "Copying Hadoop configurations..."
cp /vagrant/conf/hadoop/* hadoop-2.4.1/etc/hadoop/

sudo chmod 777 -R /home/vagrant/hadoop-2.4.1

# Format namenode
# echo "Formatting Hadoop namenode..."
# sudo su root -c -l "hadoop-2.4.1/bin/hadoop namenode -format"

# Maven package mazerunner/extension with assemblies
echo "Compiling neo4j-mazerunner extension..."
cd /vagrant/extension
mvn -q assembly:assembly -DdescriptorId=jar-with-dependencies -DskipTests

# Copy the mazerunner/extension jar file from mazerunner/extension/target to neo4j/plugins
cp target/extension-1.0-jar-with-dependencies.jar /usr/share/neo4j/plugins/extension-1.0-jar-with-dependencies.jar

ln -s /vagrant /home/vagrant/neo4j-mazerunner

# Finished provisioning -- use "vagrant ssh" and start Mazerunner with "sh neo4j-mazerunner/sbin/mazerunner.sh"
