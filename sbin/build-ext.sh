#!/usr/bin/env bash

echo "Compiling neo4j-mazerunner extension..."
cd /vagrant/extension
mvn -q assembly:assembly -DdescriptorId=jar-with-dependencies -DskipTests

# Copy the mazerunner/extension jar file from mazerunner/extension/target to neo4j/plugins
cp target/extension-1.0-jar-with-dependencies.jar /usr/share/neo4j/plugins/extension-1.0-jar-with-dependencies.jar

sudo su root -c -l "/var/lib/neo4j/bin/neo4j restart"

echo "Compiling neo4j-mazerunner spark processor..."
cd /vagrant/spark
sudo su root -c -l "mvn -q clean compile exec:java"
