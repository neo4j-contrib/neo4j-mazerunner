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

Mazerunner is currently in its alpha stages of development.

The following analysis algorithms are available:

 * PageRank
 * Triangle Count
 * Connected Components
 * Strongly Connected Components

Sandbox
================

Mazerunner alpha ships with a bundled Unbuntu development environment that automatically sets up an environment with all required dependencies preconfigured.

One of the challenges with setting up this architecture is the dependency management across Neo4j, Hadoop, and Spark. In order to make installation and development easier, a test environment was designed to get users and contributors up and running in minutes.

Installation
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

Only run this script once, as it imports the Neo4j movies sample dataset. It also runs the following query:

#### Create a KNOWS relationship between all actors who acted in the same movie

    MATCH (a1:Person)-[:ACTED_IN]->(m)<-[:ACTED_IN]-(coActors)
    CREATE (a1)-[:KNOWS]->(coActors);

By doing this, we create a direct link between actors that can be used to create a PageRank of the most valuable actors to work with.

To start the PageRank job, on the host OS, navigate to the following URL:

    http://localhost:65074/service/mazerunner/analysis/pagerank/KNOWS

This will run the PageRank algorithm on actors who know each other and then write the results back into Neo4j.

If we connect to Neo4j via the Neo4j shell tool from the sandbox (`vagrant ssh`), we can see that `Person` nodes now have a `pagerank` property:

    $ echo "MATCH n WHERE HAS(n.pagerank) RETURN n ORDER BY n.pagerank DESC LIMIT 10;" | /var/lib/neo4j/bin/neo4j-shell
    Welcome to the Neo4j Shell! Enter 'help' for a list of commands
    NOTE: Remote Neo4j graph database service 'shell' at port 1337

    neo4j-sh (?)$ MATCH n WHERE HAS(n.pagerank) RETURN n ORDER BY n.pagerank DESC LIMIT 10;
    +-----------------------------------------------------------------------+
    | n                                                                     |
    +-----------------------------------------------------------------------+
    | Node[71]{name:"Tom Hanks",born:1956,pagerank:4.642800717539658}         |
    | Node[1]{name:"Keanu Reeves",born:1964,pagerank:2.605304495549113}       |
    | Node[22]{name:"Cuba Gooding Jr.",born:1968,pagerank:2.5655048212974223} |
    | Node[34]{name:"Meg Ryan",born:1961,pagerank:2.52628473708215}           |
    | Node[16]{name:"Tom Cruise",born:1962,pagerank:2.430592498009265}        |
    | Node[19]{name:"Kevin Bacon",born:1958,pagerank:2.0886893112867035}      |
    | Node[17]{name:"Jack Nicholson",born:1937,pagerank:1.9641313625284538}   |
    | Node[120]{name:"Ben Miles",born:1967,pagerank:1.8680986516285438}       |
    | Node[4]{name:"Hugo Weaving",born:1960,pagerank:1.8515582875810466}      |
    | Node[20]{name:"Kiefer Sutherland",born:1966,pagerank:1.784065038526406} |
    +-----------------------------------------------------------------------+
    10 rows

You can also access the Neo4j Browser from the host OS at the URL: `http://localhost:65074` and run the same query in the browser.

To run other graph analysis algorithms, use the following endpoints:

 * `http://localhost:65074/service/mazerunner/analysis/pagerank/KNOWS`
 * `http://localhost:65074/service/mazerunner/analysis/triangle_count/KNOWS`
 * `http://localhost:65074/service/mazerunner/analysis/connected_components/KNOWS`
 * `http://localhost:65074/service/mazerunner/analysis/strongly_connected_components/KNOWS`

Roadmap
================

The roadmap for 1.0.0-M01 milestone can be found here: [Neo4j Mazerunner 1.0.0-M01 Milestone Features](https://github.com/kbastani/neo4j-mazerunner/issues/1)

To add feature requests to this list, please open an issue against the `Mazerunner Roadmap` label.

License
================

This library is licensed under the Apache License, Version 2.0.

![Neo4j Mazerunner](http://i.imgur.com/wCZKXNO.png)

Feedback
================

If you run into any issues head on over to the issues page and submit an issue. If you have any general feedback, add a comment to [Neo4j Mazerunner 1.0.0-M01 Milestone Features](https://github.com/kbastani/neo4j-mazerunner/issues/1).
