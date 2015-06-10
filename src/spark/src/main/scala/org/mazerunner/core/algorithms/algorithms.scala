package org.mazerunner.core

import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.lib.ShortestPaths.SPMap
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.mazerunner.core.programs._

import scala.collection.{JavaConversions, mutable}
import scala.util.Random

/**
 * Copyright (C) 2014 Kenny Bastani
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
object algorithms {

  /**
   * Perform a Connected Components calculation using Spark's GraphX algorithms
   * @param sc is the Spark Context
   * @param path is the file path to the edge list for the calculation
   * @return a key-value list of calculations for each vertex
   */
  def connectedComponents(sc: SparkContext, path: String): java.lang.Iterable[String] = {
    val graph = GraphLoader.edgeListFile(sc, path);

    val v = graph.connectedComponents().vertices

    val results = v.map { row =>
      row._1 + " " + row._2 + "\n"
    }.toLocalIterator.toIterable

    JavaConversions.asJavaIterable(results)
  }

  /**
   * Perform a PageRank calculation using Spark's GraphX algorithms
   * @param sc is the Spark Context
   * @param path is the file path to the edge list for the calculation
   * @return a key-value list of calculations for each vertex
   */
  def pageRank(sc: SparkContext, path: String): java.lang.Iterable[String] = {
    val graph = GraphLoader.edgeListFile(sc, path);

    val v = graph.pageRank(.0001).vertices

    val results = v.map { row =>
      row._1 + " " + row._2 + "\n"
    }.toLocalIterator.toIterable

    JavaConversions.asJavaIterable(results)
  }

  /**
   * Perform a Strongly Connected Components calculation using Spark's GraphX algorithms
   * @param sc is the Spark Context
   * @param path is the file path to the edge list for the calculation
   * @return a key-value list of calculations for each vertex
   */
  def stronglyConnectedComponents(sc: SparkContext, path: String): java.lang.Iterable[String] = {
    val graph = GraphLoader.edgeListFile(sc, path);

    val v = graph.stronglyConnectedComponents(2).vertices

    val results = v.map { row =>
      row._1 + " " + row._2 + "\n"
    }.toLocalIterator.toIterable

    JavaConversions.asJavaIterable(results)
  }

  /**
   * Perform a Triangle Count calculation using Spark's GraphX algorithms
   * @param sc is the Spark Context
   * @param path is the file path to the edge list for the calculation
   * @return a key-value list of calculations for each vertex
   */
  def triangleCount(sc: SparkContext, path: String): java.lang.Iterable[String] = {
    val graph = GraphLoader.edgeListFile(sc, path, canonicalOrientation = true)
      .partitionBy(PartitionStrategy.RandomVertexCut)

    val v = graph.triangleCount().vertices

    val results = v.map { row =>
      row._1 + " " + row._2 + "\n"
    }.toLocalIterator.toIterable

    JavaConversions.asJavaIterable(results)
  }

  implicit def iterebleWithAvg[T: Numeric](data: Iterable[T]) = new {
    def avg = average(data)
  }

  /**
   * Perform a Closeness Centrality calculation
   * @param sc is the Spark Context
   * @param path is the file path to the edge list for the calculation
   * @return a key-value list of calculations for each vertex
   */
  def closenessCentrality(sc: SparkContext, path: String): java.lang.Iterable[String] = {
    // The farness of a node s is defined as the sum of its distances to all other nodes,
    // and its closeness is defined as the reciprocal of the farness.
    val graph = shortestPaths(sc, path)

    // Map each vertex id to the sum of the distance of its shortest paths to all other vertices
    val results = graph.vertices.map {
      vx => (vx._1, {
        val dx = 1.0 / vx._2.map {
          sx => sx._2
        }.seq.avg
        val d = if (dx.isNaN | dx.isNegInfinity | dx.isPosInfinity) 0.0 else dx
        d
      })
    }.sortBy({ vx => vx._1}, ascending = true).map {
      row => row._1 + " " + row._2 + "\n"
    }.toLocalIterator.toIterable

    JavaConversions.asJavaIterable(results)
  }

  /**
   * Perform a Single Source Shortest Path (SSSP) calculation using Spark's GraphX algorithms
   * @param sc is the Spark Context
   * @param path is the file path to the edge list for the calculation
   * @return a key-value list of calculations for each vertex
   */
  def shortestPaths(sc: SparkContext, path: String): Graph[SPMap, Int] = {
    val graph = GraphLoader.edgeListFile(sc, path)

    ShortestPaths.run(graph, graph.vertices.map { vx => vx._1}.collect())
  }

  def average[T](ts: Iterable[T])(implicit num: Numeric[T]) = {
    num.toDouble(ts.sum) / ts.size
  }

  /**
   * Perform a Betweenness Centrality calculation on a supplied edge list.
   * @param sc is the Spark Context
   * @param path is the file path to the edge list
   * @return a key-value list of calculations for each vertex
   */
  def betweennessCentrality(sc: SparkContext, path: String): java.lang.Iterable[String] = {
    // Load the edge list
    val graph = GraphLoader.edgeListFile(sc, path)

    val result: RDD[(VertexId, Double)] = betweennessCentralityMap(sc, graph)
    
    val results = result.sortBy({ vx => vx._1}, ascending = true).map {
      row => row._1 + " " + row._2 + "\n"
    }.collect().toIterable

    JavaConversions.asJavaIterable(results)
  }

  def betweennessCentralityMap(sc: SparkContext, graph: Graph[Int, Int]): RDD[(VertexId, Double)] = {
    // Create a tree
    val tree = new DecisionTree[VertexId](0L, mutable.HashMap[VertexId, DecisionTree[VertexId]]())

    // Create an in-memory graph data structure
    graph.edges.collect().foreach(ed => tree.addLeaf(ed.srcId).addLeaf(ed.dstId))

    // Get the set of vertices, cache, and collect
    val vertexIds = graph.vertices.map(v => v._1).cache().collect()

    // Get a map of all shortest path distances using Pregel API
    val sssp = ShortestPaths.run(graph, graph.vertices.map { vx => vx._1 }.collect()).vertices.collect()

    var count = 0;
    // Collect all shortest paths
    val graphResults = sc.parallelize(vertexIds).map(row => {
      count = count + 1;
      println("** " + count)
      (row, vertexIds.map(vt => {
        (vt, tree.getNode(row).allShortestPathsTo(vt, sssp))
      }))
    }).collectAsync().get().toArray

    // Run betweenness centrality using the aggregated shortest path data for each vertex
    val result = algorithms.betweennessCentrality(sc, graphResults)
    result
  }

  /**
   * Perform a Betweenness Centrality calculation on a supplied shortest path vertex map
   * @param sc is the Spark Context
   * @param graphResults is a map of vertices to their shortest paths
   * @return a key-value list of calculations for each vertex
   */
  def betweennessCentrality(sc: SparkContext, graphResults: Array[(VertexId, Array[(VertexId, Seq[Seq[VertexId]])])]): RDD[(VertexId, Double)] = {

    // Calculate the number of shortest paths that go through each vertex
    val results = sc.parallelize(graphResults.map(dstVertex => {
      val results = dstVertex._2.map(v => {
        if (v._2 != null) (v._1, v._2.map(vt => {
          vt.drop(1).dropRight(1)
        }).filter(vt => vt.size > 0))
        else {
          (v._1, Seq[Seq[VertexId]]())
        }
      }).map(srcVertex => {
        val paths = srcVertex._2.length
        val vertexMaps = srcVertex._2.flatMap(v => v)
          .map(v => (v, 1.0 / paths.toDouble))
        if (vertexMaps.length == 0) {
          Seq[(VertexId, Double)]((srcVertex._1, 0.0))
        } else {
          vertexMaps
        }
      })
      results
    }).flatten[Seq[(VertexId, Double)]].map(a => a).flatten[(VertexId, Double)]).partitionBy(new spark.HashPartitioner(2))
      .reduceByKey(_ + _)


    results
  }

  def reduceByKey[K,V](collection: Traversable[(K, V)])(implicit num: Numeric[V]) = {
    import num._
    collection
      .groupBy(_._1)
      .map { case (group: K, traversable) => traversable.reduce{(a,b) => (a._1, a._2 + b._2)} }
  }

  class edgeMapper(val key : String, val paths : Int, val countMap : Seq[(VertexId, Double)]) extends Serializable {
    def reducer(): Map[VertexId, Double] = {
      reduceByKey[VertexId, Double](countMap)
    }

    def getCentrality: Map[VertexId, Double] = {
      reducer().map(a => (a._1, a._2.toDouble / math.max(paths.toDouble, 1.0)))
    }
  }

  /**
   * The single source shortest path (SSSP) computes the minimum distance for all vertices
   * in the graph to a single source vertex.
   * @param sc is the Spark Context, providing access to the initialized Spark engine.
   * @param path is the file system path to an edge list to be loaded as a graph
   * @return a graph where all vertices have the minimum distance value to the srcVertex
   */
  def singleSourceShortestPath(sc: SparkContext, path: String): RDD[(VertexId, Seq[(VertexId, Seq[Seq[VertexId]])])] = {
    // Load source graph
    val graph = GraphLoader.edgeListFile(sc, path)

    singleSourceShortestPath(sc, graph)
  }

  /**
   * The single source shortest path (SSSP) computes the minimum distance for all vertices
   * in the graph to a single source vertex.
   * @param sc is the Spark Context, providing access to the initialized Spark engine.
   * @param graph the graph to process
   * @return a graph where all vertices have the minimum distance value to the srcVertex
   */
  def singleSourceShortestPath(sc: SparkContext, graph: Graph[Int, Int]): RDD[(VertexId, Seq[(VertexId, Seq[Seq[VertexId]])])] = {

    val srcVertex = graph.vertices.map(a => a._1).collect().toSeq

    // Initialize vertex attributes to max int value
    val newVertices = graph.vertices.map {
      case (id, attr) =>
        (id, new ShortestPathState(id, srcVertex))
    }

    val newEdges = graph.edges.map {
      case (f) =>
        new Edge[Double](f.srcId, f.dstId, 1.0)
    }

    // Create new graph from new vertex attributes
    val newGraph = Graph(newVertices, EdgeRDD.fromEdges(newEdges))

    // Run the shortest path program
    val graphResult = new BetweennessCentralityProgram(newGraph, srcVertex).run()

    val results = graphResult.vertices.map {
      row => {
        (row._1, row._2.getShortestPaths)
      }
    }

    results
  }

  /**
   * The maximum value example is mentioned in a paper, "Pregel: A System for Large-Scale Graph Processing",
   * as the first example of an algorithm solved by Pregel. The goal of this algorithm is to replicate the
   * largest value in the graph to all nodes, and halt on that condition.
   * @param sc is the Spark Context, providing access to the initialized Spark engine.
   * @param path is the file system path to a vertex list to be loaded as a { @see Graph[Int, Int]}
   * @return a graph where all vertices have the largest vertex attribute in the initial graph
   */
  def maximumValueExample(sc: SparkContext, path: String): java.lang.Iterable[String] = {

    val graph = GraphLoader.edgeListFile(sc, path)
    val randomVertex = graph.pickRandomVertex()

    // Update vertices with a random value 1-100 and a final value 101 on a random vertex
    val newVertices = graph.vertices.map {
      case (id, attr) =>
        if (id == randomVertex) (id, 101)
        else (id, new Random().nextInt(100))
    }

    // Create new graph from new vertex attributes
    val newGraph = Graph(newVertices, graph.edges)

    // Run the maximum value program for this graph
    val graphResult = new MaximumValueProgram(newGraph).run(0)

    val results = graphResult.vertices.map {
      row => row._1 + " " + row._2 + "\n"
    }.toLocalIterator.toIterable

    JavaConversions.asJavaIterable(results)
  }

  def edgeBetweenness(sc: SparkContext, path: String): java.lang.Iterable[String] = {

    // Load the edge list
    var graph = GraphLoader.edgeListFile(sc, path)

    var graphResults: (VertexId, VertexId) = edgeBetweenness(sc, graph)

    var communitySeq : Seq[(VertexId, VertexId)] = Seq[(VertexId, VertexId)]()

    while(graphResults != null) {
      communitySeq = communitySeq ++ Seq[(VertexId, VertexId)](graphResults)
      println(graph.edges.count())
      val newEdges = graph.edges.filter(a => (a.srcId, a.dstId) != (graphResults._1, graphResults._2))
      val vertices = graph.vertices
      println(newEdges.count())
      graph = Graph[Int, Int](vertices, newEdges)
      if(newEdges.count() > 0) {
        graphResults = edgeBetweenness(sc, graph)
      } else {
        graphResults = null;
      }
    }

    val endResults =  graph.stronglyConnectedComponents(1).vertices
      .map {
      row => row._1 + " " + row._2 + "\n"
    }.toLocalIterator.toIterable

    JavaConversions.asJavaIterable(endResults)
  }

  def edgeBetweenness(sc: SparkContext, graph: Graph[PartitionID, PartitionID]): (VertexId, VertexId) = {
    val newVertices = graph.vertices.map {
      case (id, attr) =>
        (id, Seq[VertexId]())
    }

    val newEdges = graph.edges.map {
      (e) =>
        Edge[Seq[VertexId]](e.srcId, e.dstId, Seq[VertexId](e.dstId))
    }

    // Create new graph from new vertex attributes
    val newGraph = Graph(newVertices, newEdges)

    val results = new EdgeBetweennessProgram(newGraph).run(Seq[VertexId]())
      .mapTriplets(a => (a.srcAttr ++ a.dstAttr).distinct)

    val indexMap = results.edges.map(a => ((a.srcId, a.dstId), a.attr))
      .zipWithIndex()
      .map(a => (a._1._1, (a._2, a._1._2)))
      .collectAsMap()

    val reverseIndexMap = results.edges.map(a => ((a.srcId, a.dstId), a.attr))
      .zipWithIndex()
      .map(a => (a._2, a._1._1))
      .collectAsMap()

    val triplets = results.triplets.map(a => {
      a.dstAttr.map(b => {
        Edge(indexMap.get((a.srcId, a.dstId)).get._1, indexMap.get((a.dstId, b)).get._1, 1)
      })
    }).flatMap(a => a)

    val edges = EdgeRDD.fromEdges[PartitionID, PartitionID](triplets)
    val vertices = VertexRDD.fromEdges(edges, 1, 1)
    val graphRemake = Graph(vertices, edges)

    val graphResults = betweennessCentralityMap(sc, graphRemake).sortBy({ vx => vx._2 }, ascending = false)


    if(graphResults.count() > 0) {
      reverseIndexMap.get(graphResults.first()._1).get
    } else {
      null
    }
  }

  /**
   * Performs collaborative filtering and creates user recommendations to products
   * @param sc is the SparkContext
   * @param path is the HDFS path to the CSV for this analysis
   * @return a CSV file that will be used to batch import the results
   */
  def collaborativeFiltering(sc: SparkContext, path: String): java.lang.Iterable[String] = {

    // Set constants
    val rank = 10
    val numIterations = 20
    val alpha = 0.01
    val lambda = 8

    // Ingest data for this job
    val data = sc.textFile(path)

    // Parse the CSV file
    val ratings = data.map(_.split(',') match { case Array(user, item, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })

    val items = data.map(_.split(',') match { case Array(user, item, rate) =>
      (item.toInt, user)
    }).distinct().collect().toMap

    val model = ALS.trainImplicit(ratings, rank, numIterations, lambda, alpha)

    // Evaluate the model
    val usersProducts = ratings.map { case Rating(user, product, rate) =>
      (user, product)
    }

    // Make predictions on the evaluated model
    val predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }

    // Generate rates and predictions
    val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)

    // Generate the MSE
    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = r1 - r2
      err * err
    }.mean()

    println("Mean Squared Error = " + MSE)

    // Create top recommendations for all users
    val users = ratings.map(rating => rating.user)

    // Generate a dump of the user recommendations
    val results = sc.parallelize(users.distinct().collect.map(user => {
      val myUserItems = ratings.filter(a => a.user.equals(user)).map(_.product).collect().toSeq
      val candidates = sc.parallelize(items.filter(a => !myUserItems.contains(a._1)).keys.toSeq)

      var i = 0
      val recommendations = model
        .predict(candidates.map(a => (user, a)))
        .collect()
        .sortBy(-_.rating)
        .take(10)

      val lines = recommendations.map { r =>
        i += 1
        String.format("%s,%s,%s\n", r.user.toString, r.product.toString, i.toString)
      }

      lines
    })).flatMap(line => line) : RDD[String]

    JavaConversions.asJavaIterable(results.collect())
  }
}


