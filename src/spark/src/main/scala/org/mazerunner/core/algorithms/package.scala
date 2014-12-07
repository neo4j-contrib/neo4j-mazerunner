package org.mazerunner.core

import org.apache.spark.SparkContext
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.lib.ShortestPaths.SPMap
import org.apache.spark.graphx.{Graph, GraphLoader, PartitionStrategy}

import scala.collection.JavaConversions

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

  def connectedComponents(sc: SparkContext, path: String) : java.lang.Iterable[String] = {
    val graph = GraphLoader.edgeListFile(sc, path);

    val v = graph.connectedComponents().vertices

    val results = v.map { row =>
      row._1 + " " + row._2 + "\n"
    }.toLocalIterator.toIterable

    JavaConversions.asJavaIterable(results)
  }

  def pageRank(sc: SparkContext, path: String) : java.lang.Iterable[String] = {
    val graph = GraphLoader.edgeListFile(sc, path);

    val v = graph.staticPageRank(2, 0.001).vertices

    val results = v.map { row =>
      row._1 + " " + row._2 + "\n"
    }.toLocalIterator.toIterable

    JavaConversions.asJavaIterable(results)
  }

  def stronglyConnectedComponents(sc: SparkContext, path: String) : java.lang.Iterable[String] = {
    val graph = GraphLoader.edgeListFile(sc, path);

    val v = graph.stronglyConnectedComponents(2).vertices

    val results = v.map { row =>
      row._1 + " " + row._2 + "\n"
    }.toLocalIterator.toIterable

    JavaConversions.asJavaIterable(results)
  }

  def triangleCount(sc: SparkContext, path: String) : java.lang.Iterable[String] = {
    val graph = GraphLoader.edgeListFile(sc, path, canonicalOrientation = true)
      .partitionBy(PartitionStrategy.RandomVertexCut)

    val v = graph.triangleCount().vertices

    val results = v.map { row =>
      row._1 + " " + row._2 + "\n"
    }.toLocalIterator.toIterable

    JavaConversions.asJavaIterable(results)
  }

  implicit def iterebleWithAvg[T:Numeric](data:Iterable[T]) = new {
    def avg = average(data)
  }

  def closenessCentrality(sc: SparkContext, path: String) :  java.lang.Iterable[String] = {
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
    }.sortBy({ vx => vx._1 }, ascending = true).map {
      row => row._1 + " " + row._2 + "\n"
    }.toLocalIterator.toIterable

    JavaConversions.asJavaIterable(results)
  }

  def shortestPaths(sc: SparkContext, path: String) : Graph[SPMap, Int] = {
    val graph = GraphLoader.edgeListFile(sc, path);

    ShortestPaths.run(graph, graph.vertices.map { vx => vx._1 }.collect())
  }

  def average[T]( ts: Iterable[T] )( implicit num: Numeric[T] ) = {
    num.toDouble( ts.sum ) / ts.size
  }



}
