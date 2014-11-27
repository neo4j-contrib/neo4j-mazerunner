#!/usr/bin/env bash

# Start rabbitmq
sudo su root -c -l "service rabbitmq-server start"

cd /lib/neo4j-mazerunner/spark
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
echo "Example: curl http://localhost:7474/service/mazerunner/analysis/pagerank/KNOWS"

sudo su root -c -l "mvn -q compile exec:java"
