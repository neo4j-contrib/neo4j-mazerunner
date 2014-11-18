# Dockerizing Neo4j Mazerunner: Dockerfile for building graph analytics
# applications.

FROM       dockerfile/ubuntu
MAINTAINER K.B. Name <kb@socialmoon.com>

#
# Oracle Java 8 Dockerfile
#
# https://github.com/dockerfile/java
# https://github.com/dockerfile/java/tree/master/oracle-java8
#

# Install Java.
RUN \
  echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
  add-apt-repository -y ppa:webupd8team/java && \
  apt-get update && \
  apt-get install -y oracle-java8-installer && \
  rm -rf /var/lib/apt/lists/* && \
  rm -rf /var/cache/oracle-jdk8-installer

# Define commonly used JAVA_HOME variable
ENV JAVA_HOME /usr/lib/jvm/java-8-oracle

RUN apt-get update
RUN apt-get upgrade -y

# Install wget
RUN echo "Installing wget..."
RUN apt-get -y -qq install wget

# Install erlang
RUN echo "Installing erlang..."
RUN apt-get -y -qq install erlang-nox

# Install curl
RUN echo "Installing curl..."
RUN apt-get -y -qq install curl

# Install and start RabbitMQ
RUN echo "Installing RabbitMQ..."
RUN echo "deb http://www.rabbitmq.com/debian/ testing main" >/etc/apt/sources.list.d/rabbitmq.list

RUN curl -quiet -L -o ~/rabbitmq-signing-key-public.asc http://www.rabbitmq.com/rabbitmq-signing-key-public.asc
RUN apt-key add ~/rabbitmq-signing-key-public.asc

RUN apt-get -qq update
RUN apt-get -y -qq --allow-unauthenticated --force-yes install rabbitmq-server

# Install Neo4j
RUN echo "Installing Neo4j..."
RUN wget -O - http://debian.neo4j.org/neotechnology.gpg.key| apt-key add - # Import our signing key
RUN echo 'deb http://debian.neo4j.org/repo stable/' > /etc/apt/sources.list.d/neo4j.list # Create an Apt sources.list file
RUN apt-get -qq update # Find out about the files in our repository
RUN apt-get -qq -y install neo4j # Install Neo4j, community edition

# Replace default Neo4j configuration
# RUN cp /vagrant/conf/neo4j/* /etc/neo4j/

# Install git
RUN echo "Installing Git..."
RUN apt-get -y -qq install git

# Clone Mazerunner
RUN mkdir -p /lib
WORKDIR /lib
RUN git clone https://github.com/kbastani/neo4j-mazerunner.git
WORKDIR /lib/neo4j-mazerunner
COPY conf/neo4j /etc/neo4j

WORKDIR /lib

# Install Maven
RUN echo "Installing Maven..."
RUN apt-get -y -qq install maven

# Install Scala
RUN echo "Installing Scala..."
RUN apt-get -y -qq remove scala-library scala
RUN wget http://www.scala-lang.org/files/archive/scala-2.11.1.deb
RUN dpkg -i scala-2.11.1.deb
RUN apt-get -qq update
RUN apt-get -y -qq install scala

# Install sbt
RUN echo "Installing SBT..."
RUN wget http://dl.bintray.com/sbt/debian/sbt-0.13.5.deb
RUN dpkg -i sbt-0.13.5.deb
RUN apt-get -qq update
RUN apt-get -y -q install sbt

WORKDIR /lib

# Install Hadoop 2.4.1
RUN echo "Installing Hadoop 2.4.1..."
RUN wget https://archive.apache.org/dist/hadoop/core/hadoop-2.4.1/hadoop-2.4.1.tar.gz
RUN tar -xzf hadoop-2.4.1.tar.gz

WORKDIR /lib/neo4j-mazerunner

# Copy configurations for HDFS
RUN echo "Copying Hadoop configurations..."
COPY conf/hadoop /lib/hadoop-2.4.1/etc/hadoop

WORKDIR /lib

RUN sudo chmod 777 -R /lib/hadoop-2.4.1

# Maven package mazerunner/extension with assemblies
RUN echo "Compiling neo4j-mazerunner extension..."
WORKDIR /lib/neo4j-mazerunner/extension
RUN mvn -q assembly:assembly -DdescriptorId=jar-with-dependencies -DskipTests

# Copy the mazerunner/extension jar file from mazerunner/extension/target to neo4j/plugins
RUN cp /lib/neo4j-mazerunner/extension/target/extension-1.0-jar-with-dependencies.jar /usr/share/neo4j/plugins/extension-1.0-jar-with-dependencies.jar

EXPOSE 7474
ENTRYPOINT /lib
