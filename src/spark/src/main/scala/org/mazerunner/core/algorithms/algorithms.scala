package org.mazerunner.core

import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.lib.ShortestPaths.SPMap
import org.apache.spark.rdd.RDD
import org.mazerunner.core.programs.{MaximumValueProgram, ShortestPathProgram, ShortestPathState}

import scala.collection.JavaConversions
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

  def connectedComponents(sc: SparkContext, path: String): java.lang.Iterable[String] = {
    val graph = GraphLoader.edgeListFile(sc, path);

    val v = graph.connectedComponents().vertices

    val results = v.map { row =>
      row._1 + " " + row._2 + "\n"
    }.toLocalIterator.toIterable

    JavaConversions.asJavaIterable(results)
  }

  def pageRank(sc: SparkContext, path: String): java.lang.Iterable[String] = {
    val graph = GraphLoader.edgeListFile(sc, path);

    val v = graph.pageRank(.0001).vertices

    val results = v.map { row =>
      row._1 + " " + row._2 + "\n"
    }.toLocalIterator.toIterable

    JavaConversions.asJavaIterable(results)
  }

  def stronglyConnectedComponents(sc: SparkContext, path: String): java.lang.Iterable[String] = {
    val graph = GraphLoader.edgeListFile(sc, path);

    val v = graph.stronglyConnectedComponents(2).vertices

    val results = v.map { row =>
      row._1 + " " + row._2 + "\n"
    }.toLocalIterator.toIterable

    JavaConversions.asJavaIterable(results)
  }

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

  def shortestPaths(sc: SparkContext, path: String): Graph[SPMap, Int] = {
    val graph = GraphLoader.edgeListFile(sc, path);

    ShortestPaths.run(graph, graph.vertices.map { vx => vx._1}.collect())
  }

  def average[T](ts: Iterable[T])(implicit num: Numeric[T]) = {
    num.toDouble(ts.sum) / ts.size
  }

  def betweennessCentrality(sc: SparkContext, path: String): java.lang.Iterable[String] = {
    // Each vertex in the graph needs to count the number of times it belongs to
    // the shortest path between all pairs of nodes in the graph

    // Note: This is a really expensive algorithm for Pregel because all vertices need to
    // compute a single source shortest path

    val graph = GraphLoader.edgeListFile(sc, path)

    val graphResults = graph.vertices.map { vx => vx._1}.collect()
      .map(a => (a, singleSourceShortestPath(sc, a, graph)))

    // For each pair of vertices get the number of shortest paths between them
    val numberShortestPathMap = sc.parallelize(sc.parallelize(graphResults.map(a => {
      a._2.map(b => (math.max(a._1, b._1) + "_" + Math.min(a._1, b._1), {
        for (j <- (b._2 match {
          case x: Seq[Seq[VertexId]] => x
          case null => Seq[Seq[VertexId]]()
        }).toSeq) yield j
      }.flatMap(d => d))).collect().toSet
    })).flatMap(a => a).partitionBy(new spark.HashPartitioner(2)).groupByKey().map(a => (a._1, a._2.toSeq))
      .map(a => new edgeMapper(a._1, math.max(a._2.size - 1, 0), a._2.flatMap(d => d)
      .map(v => (v, 1.0 / math.max(a._2.size.toDouble - 1.0, 0.0))).toSeq))
      .map(v => v.getCentrality).flatMap(v => v).collect())

    val partitioned = graph.vertices.map(t => (t._1, 0.0)).leftOuterJoin(numberShortestPathMap.partitionBy(new spark.HashPartitioner(2))
      .reduceByKey(_ + _)).map(t => (t._1, t._2._2.getOrElse(0.0)))

      .sortBy(a => a._1, ascending = true)
      .map {
      row => row._1 + " " + row._2 + "\n"
    }.toLocalIterator.toIterable


    val theResults = partitioned.toIterable


    JavaConversions.asJavaIterable(theResults.toIterable)
  }

  def reduceByKey[K,V](collection: Traversable[Tuple2[K, V]])(implicit num: Numeric[V]) = {
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
   * @param srcVertex is the id of the source vertex to compute minimum distance to
   * @param path is the file system path to an edge list to be loaded as a graph
   * @return a graph where all vertices have the minimum distance value to the srcVertex
   */
  def singleSourceShortestPath(sc: SparkContext, srcVertex: VertexId, path: String): RDD[(VertexId, Seq[Seq[VertexId]])] = {
    // Load source graph
    val graph = GraphLoader.edgeListFile(sc, path)

    singleSourceShortestPath(sc, srcVertex, graph)
  }

  /**
   * The single source shortest path (SSSP) computes the minimum distance for all vertices
   * in the graph to a single source vertex.
   * @param sc is the Spark Context, providing access to the initialized Spark engine.
   * @param srcVertex is the id of the source vertex to compute minimum distance to
   * @param graph the graph to process
   * @return a graph where all vertices have the minimum distance value to the srcVertex
   */
  def singleSourceShortestPath(sc: SparkContext, srcVertex: VertexId, graph: Graph[Int, Int]): RDD[(VertexId, Seq[Seq[VertexId]])] = {

    // Initialize vertex attributes to max int value
    val newVertices = graph.vertices.map {
      case (id, attr) =>
        if (id == srcVertex) (id, new ShortestPathState(id, srcVertex))
        else (id, new ShortestPathState(id, srcVertex))
    }

    val newEdges = graph.edges.map {
      case (f) =>
        new Edge[Double](f.srcId, f.dstId, 1.0)
    }

    // Create new graph from new vertex attributes
    val newGraph = Graph(newVertices, EdgeRDD.fromEdges(newEdges))

    // Run the shortest path program
    val graphResult = new ShortestPathProgram(newGraph, srcVertex).run()

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
}


