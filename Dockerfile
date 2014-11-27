# Dockerizing Neo4j Mazerunner: Dockerfile for building graph analytics
# applications.

FROM       dockerfile/ubuntu
MAINTAINER K.B. Name <kb@socialmoon.com>

# Install Java 8.
RUN \
  echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
  add-apt-repository -y ppa:webupd8team/java && \
  apt-get update && \
  apt-get install -y oracle-java8-installer && \
  rm -rf /var/lib/apt/lists/* && \
  rm -rf /var/cache/oracle-jdk8-installer

# Define commonly used JAVA_HOME variable
ENV JAVA_HOME /usr/lib/jvm/java-8-oracle

# Update apt-get
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

# Create rabbitmq config
RUN mkdir /etc/rabbitmq
RUN echo "[{rabbit, [{loopback_users, []}]}]." > /etc/rabbitmq/rabbitmq.config

# Install and start RabbitMQ
RUN echo "Installing RabbitMQ..."
RUN echo "deb http://www.rabbitmq.com/debian/ testing main" >/etc/apt/sources.list.d/rabbitmq.list

RUN curl -quiet -L -o ~/rabbitmq-signing-key-public.asc http://www.rabbitmq.com/rabbitmq-signing-key-public.asc
RUN apt-key add ~/rabbitmq-signing-key-public.asc

RUN apt-get -qq update
RUN apt-get -y -qq --allow-unauthenticated --force-yes install rabbitmq-server

# Clone Mazerunner
RUN mkdir -p /lib
WORKDIR /lib
COPY . /lib/neo4j-mazerunner
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

COPY sbin/mazerunner.sh /etc/bootstrap.sh
RUN chown root:root /etc/bootstrap.sh
RUN chmod 700 /etc/bootstrap.sh

ENV BOOTSTRAP /etc/bootstrap.sh

CMD ["/etc/bootstrap.sh", "-d"]
