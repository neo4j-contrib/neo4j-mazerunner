package org.mazerunner.core.programs

import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.rdd.RDD
import org.mazerunner.core.algorithms
import org.mazerunner.core.config.ConfigurationLoader
import org.mazerunner.core.processor.GraphProcessor
import org.scalatest.FlatSpec

import scala.collection.mutable

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
class ShortestPathTests  extends FlatSpec {
  /**
   * To collect the shortest path results for all nodes to a single destination node,
   * the following steps must be taken:
   *
   */

  ConfigurationLoader.testPropertyAccess = true

  // Create Spark context
  val sc = GraphProcessor.initializeSparkContext.sc

  val vertexIds = sc.parallelize(Seq(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L)).collect().toSeq

  def fixture =
    new {
      
      // Create an RDD for the vertices
      val vertices: RDD[(VertexId, ShortestPathState)] = sc.parallelize(Array(
        (0L, new ShortestPathState(0L, vertexIds)),
        (1L, new ShortestPathState(1L, vertexIds)),
        (2L, new ShortestPathState(2L, vertexIds)),
        (3L, new ShortestPathState(3L, vertexIds)),
        (4L, new ShortestPathState(4L, vertexIds)),
        (5L, new ShortestPathState(5L, vertexIds)),
        (6L, new ShortestPathState(6L, vertexIds)),
        (7L, new ShortestPathState(7L, vertexIds)),
        (8L, new ShortestPathState(8L, vertexIds)),
        (9L, new ShortestPathState(9L, vertexIds)),
        (10L, new ShortestPathState(10L, vertexIds)),
        (11L, new ShortestPathState(11L, vertexIds)),
        (12L, new ShortestPathState(12L, vertexIds))))

      // Create an RDD for edges
      val edges: RDD[Edge[Int]] = sc.parallelize(Array(
        Edge(0L, 1L, 0),
        Edge(1L, 4L, 0),
        Edge(1L, 2L, 0),
        Edge(2L, 3L, 0),
        Edge(5L, 6L, 0),
        Edge(6L, 7L, 0),
        Edge(7L, 8L, 0),
        Edge(8L, 9L, 0),
        Edge(9L, 10L, 0),
        Edge(10L, 11L, 0),
        Edge(11L, 12L, 0),
        Edge(12L, 3L, 0),
        Edge(7L, 3L, 0),
        Edge(4L, 3L, 0)))

      // Build the initial Graph
      val graph = Graph(vertices, edges, new ShortestPathState(-1L, null))
    }

  "A node's state" should "have a decision tree" in {
    val graph = fixture.graph

    val tree  = new DecisionTree[VertexId](0L, mutable.HashMap[VertexId, DecisionTree[VertexId]]())

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

    val resultStream = result

    for (x <- resultStream) {
      println(x)
    }

  }

}
