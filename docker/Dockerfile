# Dockerizing Neo4j Mazerunner: Dockerfile for building graph analytics
# applications.

FROM       java:openjdk-8-jdk
MAINTAINER K.B. Name <kb@socialmoon.com>

USER root

# Set the default HDFS and Spark hosts
ENV SPARK_HOST local
ENV HDFS_HOST hdfs://hdfs:9000
ENV DRIVER_HOST mazerunner
ENV RABBITMQ_HOST localhost
ENV SPARK_EXECUTOR_MEMORY 6g
ENV HADOOP_HOME /etc/hadoop
ENV MAZERUNNER_HOME /etc/mazerunner
ENV CLASSPATH /etc/hadoop/conf:/etc/hadoop/*:/etc/mazerunner/*:/etc/mazerunner/lib/*
ENV SPARK_CLASSPATH /etc/hadoop/conf:/etc/hadoop/*:/etc/mazerunner/*:/etc/mazerunner/lib/*

RUN mkdir /etc/mazerunner

# Update apt-get
RUN apt-get update && \
    apt-get -y -qq install erlang-nox && \
    mkdir /etc/rabbitmq && \
    echo "[{rabbit, [{loopback_users, []}]}]." > /etc/rabbitmq/rabbitmq.config && \
    echo "deb http://www.rabbitmq.com/debian/ testing main" >/etc/apt/sources.list.d/rabbitmq.list && \
    curl -quiet -L -o ~/rabbitmq-signing-key-public.asc http://www.rabbitmq.com/rabbitmq-signing-key-public.asc && \
    apt-key add ~/rabbitmq-signing-key-public.asc && \
    apt-get -qq update && \
    apt-get -y -qq --allow-unauthenticated --force-yes install rabbitmq-server && \
    apt-get clean

# Copy bootstrapper
COPY sbin/mazerunner.sh /etc/mazerunner/bootstrap.sh
RUN chown root:root /etc/mazerunner/bootstrap.sh
RUN chmod 700 /etc/mazerunner/bootstrap.sh

# Copy Spark's HDFS configurations
RUN mkdir /etc/hadoop
COPY conf/hadoop /etc/hadoop

# Copy Mazerunner service binary
WORKDIR /etc/mazerunner
RUN wget https://s3-us-west-1.amazonaws.com/mazerunner-artifacts/spark-1.1.2-RELEASE-driver.jar

ENV BOOTSTRAP /etc/mazerunner/bootstrap.sh

CMD ["/etc/mazerunner/bootstrap.sh", "-d"]
