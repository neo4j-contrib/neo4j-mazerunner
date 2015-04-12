package org.mazerunner.core.programs

import org.apache.spark.graphx._
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.mazerunner.core.config.ConfigurationLoader
import org.mazerunner.core.processor.GraphProcessor
import org.scalatest.FlatSpec

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
class ShortestPathProgramTests extends FlatSpec {
  /**
   * To collect the shortest path results for all nodes to a single destination node,
   * the following steps must be taken:
   *
   */

  ConfigurationLoader.testPropertyAccess = true

  // Create Spark context
  val sc = GraphProcessor.initializeSparkContext.sc



  def fixture =
    new {

      val vertexIds = sc.parallelize(Seq(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L)).collect().toSeq

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
      val graph = Graph(vertices, edges, new ShortestPathState(-1L, vertexIds))
    }

  /**
   * 0. The state of each node is represented as a decision tree
   * a. Each node will need to manage a serializable graph data structure
   * b. Each node can have a super state of active or inactive
   * c. Active nodes have at least one non-dead branch without the destination node in it
   */

  "A node's state" should "have a decision tree" in {
    val graph = fixture.graph

    val vertexIds = graph.vertices.map(a => a._1)

    // Get a vertex neighbor map
    val vertexNeighborMap = new PairRDDFunctions[VertexId, Array[(VertexId, ShortestPathState)]](graph.ops.collectNeighbors(EdgeDirection.Out)
      .map(a => (a._1, a._2) : (VertexId, Array[(VertexId, ShortestPathState)])))
    .collectAsMap()

    for (vertexId <- vertexIds) {


      // Get all paths to destination node as a decision tree
      val results = graph.triplets.map(triplet => {
        triplet.srcAttr.decisionTree.addLeaf(triplet.dstId)
        def addLeaf(active: VertexId, subActive: VertexId): Unit = new {
          if (subActive != vertexId) {
            vertexNeighborMap(subActive).foreach(f => {
              if (triplet.srcAttr.addToPath(f._1, subActive))
                addLeaf(active, f._1)
            })
          }
        }
        addLeaf(triplet.srcId, triplet.dstId)
        triplet.srcAttr
      }).map(a => (a.srcVertex, a))

      // Use collectAsMap to bring everything on the same thread
      new PairRDDFunctions[VertexId, ShortestPathState](results).collectAsMap()
        .foreach(p => println(p._2.decisionTree.toString))

    }
  }
}
