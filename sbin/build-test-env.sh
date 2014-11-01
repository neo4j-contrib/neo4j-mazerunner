#!/bin/bash

# Setup Linux
#vagrant up --provision

# SSH into the VM
vagrant ssh

# Install Hadoop 2.4.1
wget https://archive.apache.org/dist/hadoop/core/hadoop-2.4.1/hadoop-2.4.1.tar.gz
tar -xzf hadoop-2.4.1.tar.gz

# Install RabbitMQ

# Install Neo4j

# Install curl

# Configure Hadoop

# Format namenode

# Start DFS

# Install Maven

# Copy mazerunner source

# Configure mazerunner/extension/src/main/resources/mazerunner.properties

# Maven package mazerunner/extension with assemblies

# Copy the mazerunner/extension jar file from mazerunner/extension/target to neo4j/plugins

# Modify neo4j-server.properties to declare mazerunner as an extension

# Start mazerunner/spark in background

# Start RabbitMQ server

# Start Neo4j server

# Import sample dataset to Neo4j

# curl to http://localhost:7474/service/mazerunner/warmup

# curl to http://localhost:7474/service/mazerunner/pagerank
