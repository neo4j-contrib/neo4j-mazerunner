# Dockerizing Neo4j Mazerunner: Dockerfile for building graph analytics
# applications.

FROM       dockerfile/java:oracle-java8
MAINTAINER K.B. Name <kb@socialmoon.com>

USER root
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
COPY bin /etc/mazerunner

WORKDIR /etc/mazerunner

ENV BOOTSTRAP /etc/mazerunner/bootstrap.sh

CMD ["/etc/mazerunner/bootstrap.sh", "-d"]
