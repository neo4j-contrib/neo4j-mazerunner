#!/bin/bash

yes yes | sudo hadoop-2.4.1/sbin/start-dfs.sh
cd neo4j-mazerunner/spark
sudo sbt run

# Before running the next step, import a dataset into Neo4j
curl http://localhost:7474/service/mazerunner/warmup
curl http://localhost:7474/service/mazerunner/pagerank
