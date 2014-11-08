#!/usr/bin/env bash

# Import Neo4j sample dataset
neo4j-shell -file /vagrant/sbin/movie-dataset.cql
neo4j-shell -file /vagrant/sbin/actors-knows.cql

sudo su root -c -l "hadoop-2.4.1/bin/hdfs namenode -format"
sudo su root -c -l "yes y | sudo ssh-keygen -t dsa -P '' -f ~/.ssh/id_dsa"
sudo su root -c -l "cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys"
sudo su root -c -l "yes Yes | /home/vagrant/hadoop-2.4.1/sbin/start-dfs.sh"
sudo su root -c -l "/var/lib/neo4j/bin/neo4j restart"

# Warmup the service and start listening for messages
curl http://localhost:7474/service/mazerunner/warmup

cd neo4j-mazerunner/spark
echo ""
echo ""
echo "    __  ______ _____   __________  __  ___   ___   ____________  "
echo "   /  |/  /   /__  /  / ____/ __ \/ / / / | / / | / / ____/ __ \ "
echo "  / /|_/ / /| | / /  / __/ / /_/ / / / /  |/ /  |/ / __/ / /_/ / "
echo " / /  / / ___ |/ /__/ /___/ _, _/ /_/ / /|  / /|  / /___/ _, _/  "
echo "/_/  /_/_/  |_/____/_____/_/ |_|\____/_/ |_/_/ |_/_____/_/ |_|   "
echo "                                                                 "
echo "========================="
echo "Mazerunner is running..."
echo "========================="
echo "To start a PageRank job, access the Mazerunner PageRank endpoint"
echo "Example: curl http://localhost:7474/service/mazerunner/pagerank"

sudo su root -c -l "mvn -q compile exec:java"
