package org.mazerunner.core

import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.lib.ShortestPaths.SPMap
import org.apache.spark.rdd.RDD
import org.mazerunner.core.programs.{BetweennessCentralityProgram, DecisionTree, MaximumValueProgram, ShortestPathState}

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

  def inMemoryBetweennessCentrality(sc: SparkContext, path: String): java.lang.Iterable[String] = {

    // Create a tree
    val tree  = new DecisionTree[VertexId](0L, mutable.HashMap[VertexId, DecisionTree[VertexId]]())

    val graph = GraphLoader.edgeListFile(sc, path)

    graph.edges.collect().foreach(ed => tree.addLeaf(ed.srcId).addLeaf(ed.dstId))

    val vertexIds = graph.vertices.map(v => v._1).cache().collect()

    val sssp = ShortestPaths.run(graph, graph.vertices.map { vx => vx._1}.collect()).vertices.collect()

    val graphResults = sc.parallelize(vertexIds).map(row => {
      println("*** " + row)
      (row, vertexIds.map(vt => {
        (vt, tree.getNode(row).allShortestPathsTo(vt, sssp))
      }))
    }).collectAsync().get().toArray

    val result = algorithms.betweennessCentrality(sc, graphResults)


    result
  }

  def betweennessCentrality(sc: SparkContext, graphResults: Array[(VertexId, Array[(VertexId, Seq[Seq[VertexId]])])]): java.lang.Iterable[String] = {
    val results =sc.parallelize(graphResults.map(dstVertex => {
      val results = dstVertex._2.map(v => {
        if (v._2 != null) {
          (v._1, v._2.map(vt => {
            vt.drop(1).dropRight(1)
          }).filter(vt => vt.size > 0))
        } else {
          (v._1, Seq[Seq[VertexId]]())
        }
      })
        .map(srcVertex => {
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
      .sortBy({ vx => vx._1}, ascending = true).map {
      row => row._1 + " " + row._2 + "\n"
    }.collect().toIterable

    JavaConversions.asJavaIterable(results)
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
}


