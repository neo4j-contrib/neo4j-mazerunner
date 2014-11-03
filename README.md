Mazerunner for Neo4j
================

Mazerunner extends a [Neo4j graph database](http://www.neo4j.com) to run scheduled big data graph compute algorithms at scale with HDFS and Apache Spark.

What is Mazerunner?
================

Mazerunner is a [Neo4j unmanaged extension](http://neo4j.com/docs/stable/server-unmanaged-extensions.html) and distributed graph processing platform that extends Neo4j to do scheduled batch and stream processing jobs while persisting the results back to Neo4j.

How does it work?
================

Mazerunner uses a message broker to distribute graph processing jobs to [Apache Spark's GraphX](https://spark.apache.org/graphx/) module. When an agent job is dispatched, a subgraph is exported from Neo4j and written to [Apache Hadoop HDFS](https://hadoop.apache.org/docs/r2.4.1/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html).

After Neo4j exports a subgraph to HDFS, a separate Mazerunner service for Spark is notified to begin processing that data. The Mazerunner service will then start a distributed graph processing algorithm using Scala and Spark's GraphX module. The GraphX algorithm is serialized and dispatched to Apache Spark for processing.

Once the Apache Spark job completes, the results are written back to HDFS as a Key-Value list of property updates to be applied back to Neo4j.

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

The Mazerunner development environment sandbox requires [Vagrant](https://docs.vagrantup.com/v2/getting-started/index.html). To get started, visit the provided Vagrant link and follow the directions to install the platform.

You want to get the [latest version](https://www.vagrantup.com/downloads.html) which at the time of writing is `1.6.5`

After Vagrant is installed you'll need to [download the Ubuntu image](https://docs.vagrantup.com/v2/getting-started/index.html) which we'll base our VM on:

    $ vagrant init hashicorp/precise32

    ==> box: Loading metadata for box 'hashicorp/precise32'
    box: URL: https://vagrantcloud.com/hashicorp/precise32
    ==> box: Adding box 'hashicorp/precise32' (v1.0.0) for provider: virtualbox
    box: Downloading: https://vagrantcloud.com/hashicorp/boxes/precise32/versions/1/providers/virtualbox.box
    ==> box: Successfully added box 'hashicorp/precise32' (v1.0.0) for 'virtualbox'!

Next, clone this repository and run the following command from the Mazerunner repository root:

    $ vagrant up --provision

The development environment will take a few minutes to provision, once complete, run the following command to remote into the development environment:

    $ vagrant ssh

You will then be logged into the machine as the user `vagrant`. All `sudo` operations will not require a username and password, instead it uses a private key.

Start Mazerunner
================

To start Mazerunner on the provisioned development environment, run the following command after remoting into the machine via `vagrant ssh`:

    $ sh neo4j-mazerunner/sbin/mazerunner.sh

    [*] Waiting for messages. To exit press CTRL+C

This script will also import a sample movie data set into a Neo4j instance running on port `7474`.

On another SSH session we can now send a job for mazerunner to execute like this:

    $ curl -v http://localhost:7474/service/mazerunner/pagerank

This will run the Page Rank algorithm over it and write the results back into Neo4j.

If we connect to Neo4j via the Neo4j shell tool we can see that `Person` nodes now have a `weight` property:

    $ echo "MATCH n WHERE HAS(n.weight) RETURN n ORDER BY n.weight DESC LIMIT 10;" | /var/lib/neo4j/bin/neo4j-shell
    Welcome to the Neo4j Shell! Enter 'help' for a list of commands
    NOTE: Remote Neo4j graph database service 'shell' at port 1337

    neo4j-sh (?)$ MATCH n WHERE HAS(n.weight) RETURN n ORDER BY n.weight DESC LIMIT 10;
    +------------------------------------------------------------------------+
    | n                                                                      |
    +------------------------------------------------------------------------+
    | Node[71]{name:"Tom Hanks",born:1956,weight:4.642800717539658}          |
    | Node[242]{name:"Tom Hanks",born:1956,weight:4.642800717539658}         |
    | Node[1]{name:"Keanu Reeves",born:1964,weight:2.605304495549113}        |
    | Node[172]{name:"Keanu Reeves",born:1964,weight:2.605304495549113}      |
    | Node[22]{name:"Cuba Gooding Jr.",born:1968,weight:2.5655048212974223}  |
    | Node[193]{name:"Cuba Gooding Jr.",born:1968,weight:2.5655048212974223} |
    | Node[34]{name:"Meg Ryan",born:1961,weight:2.52628473708215}            |
    | Node[205]{name:"Meg Ryan",born:1961,weight:2.52628473708215}           |
    | Node[16]{name:"Tom Cruise",born:1962,weight:2.430592498009265}         |
    | Node[187]{name:"Tom Cruise",born:1962,weight:2.430592498009265}        |
    +------------------------------------------------------------------------+
    10 rows

1.0.0 Roadmap
================

The roadmap for 1.0.0-M01 milestone can be found here: [Neo4j Mazerunner 1.0.0-M01 Milestone Features](https://github.com/kbastani/neo4j-mazerunner/issues/1)

To add feature requests to this list, please open an issue against the `Mazerunner Roadmap` label.

Licensing
================

Neo4j is an open source product. Neo4j supports a Community edition under the GPLv3 license. The Enterprise edition is available under the AGPLv3 license for open source projects otherwise under a commercial license from Neo Technology. The Neo4j Mazerunner project ships with a community edition of Neo4j under GPLv3 license.

Mazerunner is an open source Neo4j community project. As an extension to Neo4j, all contributed code is permissively licensed under an [Apache License](https://github.com/kbastani/neo4j-mazerunner/blob/master/LICENSE).

All other dependencies included in the Neo4j Mazerunner distribution are Apache License or another form of permissive license. These dependencies include:

* Apache Hadoop 2.4.1 (Apache License)
* Apache Spark 1.1.0 (Apache License)
* RabbitMQ (Mozilla Public License)
* See `pom.xml` for other bundled dependencies

![Neo4j Mazerunner](http://i.imgur.com/wCZKXNO.png)
