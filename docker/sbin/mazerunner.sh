#!/usr/bin/env bash

# Start rabbitmq
service rabbitmq-server start

cd /etc/mazerunner
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

java -cp $CLASSPATH org.mazerunner.core.messaging.Worker --spark.master $SPARK_HOST --hadoop.hdfs $HDFS_HOST --spark.driver.host $DRIVER_HOST --spark.executor.memory $SPARK_EXECUTOR_MEMORY --rabbitmq.host $RABBITMQ_HOST
