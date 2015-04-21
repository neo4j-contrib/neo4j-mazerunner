# Graph Analytics for Neo4j

This docker image adds high-performance graph analytics to a [Neo4j graph database](http://www.neo4j.com). This image deploys a container with [Apache Spark](https://spark.apache.org/) and uses [GraphX](https://spark.apache.org/graphx/) to perform ETL graph analysis on subgraphs exported from Neo4j. The results of the analysis are applied back to the data in the Neo4j database.

## Supported Algorithms

*PageRank*

*Closeness Centrality*

*Betweenness Centrality*

*Triangle Counting*

*Connected Components*

*Strongly Connected Components*

### Neo4j Mazerunner Service

The Neo4j Mazerunner service in this image is a [unmanaged extension](http://neo4j.com/docs/stable/server-unmanaged-extensions.html) that adds a REST API endpoint to Neo4j for submitting graph analysis jobs to Apache Spark GraphX. The results of the analysis are applied back to the nodes in Neo4j as property values, making the results queryable using Cypher.

## Installation/Deployment

Installation requires 3 docker image deployments, each containing a separate linked component.

* *Hadoop HDFS* (sequenceiq/hadoop-docker:2.4.1)
* *Neo4j Graph Database* (kbastani/docker-neo4j:2.2.1)
* *Apache Spark Service* (kbastani/neo4j-graph-analytics:1.1.0)

Pull the following docker images:

    docker pull sequenceiq/hadoop-docker:2.4.1
    docker pull kbastani/docker-neo4j:2.2.1
    docker pull kbastani/neo4j-graph-analytics:1.1.0

After each image has been downloaded to your Docker server, run the following commands in order to create the linked containers.

    # Create HDFS
    docker run -i -t --name hdfs sequenceiq/hadoop-docker:2.4.1 /etc/bootstrap.sh -bash

    # Create Mazerunner Apache Spark Service
    docker run -i -t --name mazerunner --link hdfs:hdfs kbastani/neo4j-graph-analytics:1.1.0

    # Create Neo4j database with links to HDFS and Mazerunner
    # Replace <user> and <neo4j-path>
    # with the location to your existing Neo4j database store directory
    docker run -d -P -v /Users/<user>/<neo4j-path>/data:/opt/data --name graphdb --link mazerunner:mazerunner --link hdfs:hdfs kbastani/docker-neo4j:2.2.1

### Use Existing Neo4j Database

To use an existing Neo4j database, make sure that the database store directory, typically `data/graph.db`, is available on your host OS. Read the [setup guide](https://github.com/kbastani/docker-neo4j#start-neo4j-container) for *kbastani/docker-neo4j* for additional details.

> Note: The kbastani/docker-neo4j:2.2.1 image is running Neo4j 2.2.1. If you point it to an older database store, that database may become unable to be attached to a previous version of Neo4j. Make sure you back up your store files before proceeding.

### Use New Neo4j Database

To create a new Neo4j database, use any path to a valid directory.

### Accessing the Neo4j Browser

The Neo4j browser is exposed on the `graphdb` container on port 7474. If you're using boot2docker on MacOSX, follow the directions [here](https://github.com/kbastani/docker-neo4j#boot2docker) to access the Neo4j browser.

## Usage Directions

Graph analysis jobs are started by accessing the following endpoint:

    http://localhost:7474/service/mazerunner/analysis/{analysis}/{relationship_type}

Replace `{analysis}` in the endpoint with one of the following analysis algorithms:

- pagerank
- closeness_centrality
- betweenness_centrality
- triangle_count
- connected_components
- strongly_connected_components

Replace `{relationship_type}` in the endpoint with the relationship type in your Neo4j database that you would like to perform analysis on. The nodes that are connected by that relationship will form the graph that will be analyzed. For example, the equivalent Cypher query would be the following:

    MATCH (a)-[:FOLLOWS]->(b)
    RETURN id(a) as src, id(b) as dst

The result of the analysis will set the property with `{analysis}` as the key on `(a)` and `(b)`. For example, if you ran the `pagerank` analysis on the `FOLLOWS` relationship type, the following Cypher query will display the results:

    MATCH (a)-[:FOLLOWS]-()
    RETURN DISTINCT id(a) as id, a.pagerank as pagerank
    ORDER BY pagerank DESC

## Available Metrics

To begin graph analysis jobs on a particular metric, HTTP GET request on the following Neo4j server endpoints:

### PageRank

    http://172.17.0.21:7474/service/mazerunner/analysis/pagerank/FOLLOWS

* Gets all nodes connected by the `FOLLOWS` relationship and updates each node with the property key `pagerank`.

* The value of the `pagerank` property is a float data type, ex. `pagerank: 3.14159265359`.

* PageRank is used to find the relative importance of a node within a set of connected nodes.

### Closeness Centrality

    http://172.17.0.21:7474/service/mazerunner/analysis/closeness_centrality/FOLLOWS

* Gets all nodes connected by the `FOLLOWS` relationship and updates each node with the property key `closeness_centrality`.

* The value of the `closeness_centrality` property is a float data type, ex. `pagerank: 0.1337`.

* A key node centrality measure in networks is closeness centrality (Freeman, 1978; Opsahl et al., 2010; Wasserman and Faust, 1994). It is defined as the inverse of farness, which in turn, is the sum of distances to all other nodes.

### Betweenness Centrality

    http://172.17.0.21:7474/service/mazerunner/analysis/betweenness_centrality/FOLLOWS

* Gets all nodes connected by the `FOLLOWS` relationship and updates each node with the property key `betweenness_centrality`.

* The value of the `betweenness_centrality` property is a float data type, ex. `betweenness_centrality: 20.345`.

* Betweenness centrality is an indicator of a node's centrality in a network. It is equal to the number of shortest paths from all vertices to all others that pass through that node. A node with high betweenness centrality has a large influence on the transfer of items through the network, under the assumption that item transfer follows the shortest paths.

### Triangle Counting

    http://172.17.0.21:7474/service/mazerunner/analysis/triangle_count/FOLLOWS

* Gets all nodes connected by the `FOLLOWS` relationship and updates each node with the property key `triangle_count`.

* The value of the `triangle_count` property is an integer data type, ex. `triangle_count: 2`.

* The value of `triangle_count` represents the count of the triangles that a node is connected to.

* A node is part of a triangle when it has two adjacent nodes with a relationship between them. The `triangle_count` property provides a measure of clustering for each node.

### Connected Components

    http://172.17.0.21:7474/service/mazerunner/analysis/connected_components/FOLLOWS

* Gets all nodes connected by the `FOLLOWS` relationship and updates each node with the property key `connected_components`.

* The value of `connected_components` property is an integer data type, ex. `connected_components: 181`.

* The value of `connected_components` represents the *Neo4j internal node ID* that has the lowest integer value for a set of connected nodes.

* Connected components are used to find isolated clusters, that is, a group of nodes that can reach every other node in the group through a *bidirectional* traversal.

### Strongly Connected Components

    http://172.17.0.21:7474/service/mazerunner/analysis/strongly_connected_components/FOLLOWS

* Gets all nodes connected by the `FOLLOWS` relationship and updates each node with the property key `strongly_connected_components`.

* The value of `strongly_connected_components` property is an integer data type, ex. `strongly_connected_components: 26`.

* The value of `strongly_connected_components` represents the *Neo4j internal node ID* that has the lowest integer value for a set of strongly connected nodes.

* Strongly connected components are used to find clusters, that is, a group of nodes that can reach every other node in the group through a *directed* traversal.

Architecture
================

Mazerunner uses a message broker to distribute graph processing jobs to [Apache Spark's GraphX](https://spark.apache.org/graphx/) module. When an agent job is dispatched, a subgraph is exported from Neo4j and written to [Apache Hadoop HDFS](https://hadoop.apache.org/docs/r2.4.1/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html).

After Neo4j exports a subgraph to HDFS, a separate Mazerunner service for Spark is notified to begin processing that data. The Mazerunner service will then start a distributed graph processing algorithm using Scala and Spark's GraphX module. The GraphX algorithm is serialized and dispatched to Apache Spark for processing.

Once the Apache Spark job completes, the results are written back to HDFS as a Key-Value list of property updates to be applied back to Neo4j.

Neo4j is then notified that a property update list is available from Apache Spark on HDFS. Neo4j batch imports the results and applies the updates back to the original graph.

License
================

This library is licensed under the Apache License, Version 2.0.
