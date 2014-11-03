Mazerunner for Neo4j
================

Mazerunner extends a ![Neo4j graph database](http://www.neo4j.com) to run scheduled big data graph compute algorithms at scale with HDFS and Apache Spark.

![Neo4j Mazerunner](https://raw.githubusercontent.com/kbastani/neo4j-mazerunner/master/docs/img/mazerunner-logo.png)

What is Mazerunner?
================

Mazerunner is a ![Neo4j unmanaged extension](http://neo4j.com/docs/stable/server-unmanaged-extensions.html) and distributed graph processing platform that extends Neo4j to do scheduled batch and stream processing jobs while persisting the results back to Neo4j.

How does it work?
================

Mazerunner uses a message broker to distribute graph processing jobs to ![Apache Spark's GraphX](https://spark.apache.org/graphx/) module. When an agent job is dispatched, a subgraph is exported from Neo4j and written to ![Apache Hadoop HDFS](https://hadoop.apache.org/docs/r2.4.1/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html).

Mazerunner runs a controller service that listens for any agent jobs dispatched from Neo4j. After Neo4j exports a subgraph to HDFS, the Mazerunner service is notified to begin processing that data. The Mazerunner service will then start a distributed graph processing algorithm using Scala and GraphX. The GraphX algorithm is serialized and dispatched to Apache Spark for processing. Once the Apache Spark job completes, the results are written back to HDFS as a Key-Value list of property updates to be applied back to Neo4j.

Neo4j is then notified that a property update list is available from Apache Spark on HDFS. Neo4j batch imports the results and applies the updates back to the original graph.

Alpha version
================

Mazerunner is currently in its alpha stages of development. For this initial release PageRank is the only available graph processing algorithm.

Other considerations:

* Apache Spark is running embedded as a single instance cluster. For the 1.0 release, this will be configured to scale to many instances.
* Contributions will be accepted in the form of Scala algorithms. Contributions guidelines (how to contribute) will soon be announced.
* All pull requests will be reviewed and implemented if they improve the core architecture and pass all tests performed in the bundled test environment.

Mazerunner sandbox
================

Mazerunner alpha ships with a bundled Unbuntu development environment that automatically sets up an environment with all required dependencies preconfigured.

One of the challenges with setting up this architecture is the dependency management across Neo4j, Hadoop, and Spark. In order to make installation and development easier, a test environment was designed to get users and contributors up and running in minutes.

Sandbox installation
================

The Mazerunner development environment sandbox requires ![Vagrant](https://docs.vagrantup.com/v2/getting-started/index.html). To get started, visit the provided Vagrant link and follow the directions to install the platform.

After Vagrant is installed on your machine, close this repository and run the following command from the Mazerunner repository root:

    $ vagrant up --provision

The development environment will take a few minutes to provision, once complete, run the following command to remote into the development environment:

    $ vagrant ssh

You will then be logged into the machine as the user `vagrant`. All `sudo` operations will not require a username and password, instead it uses a private key.

Start Mazerunner
================

To start Mazerunner on the provisioned development environment, run the following command after remoting into the machine via `vagrant ssh`:

    $ sh neo4j-mazerunner/sbin/mazerunner.sh
