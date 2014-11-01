#!/bin/bash

yes yes | sudo hadoop-2.4.1/sbin/start-dfs.sh

sudo /var/lib/neo4j/bin/neo4j restart

# Before running the next step, import a dataset into Neo4j
curl http://localhost:7474/service/mazerunner/warmup

cd neo4j-mazerunner/spark
sudo mvn install


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
echo "To start a PageRank job, access the mazerunner pagerank endpoint"
echo "Example: curl http://localhost:7474/service/mazerunner/pagerank"

sudo mvn exec:java
